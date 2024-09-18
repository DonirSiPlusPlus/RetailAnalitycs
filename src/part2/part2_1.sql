DROP VIEW IF EXISTS customers;

CREATE OR REPLACE VIEW customers AS (
  WITH avg_check AS (
    SELECT 
      customer_id, 
      ROUND(CAST(AVG(transaction_summ) AS NUMERIC),2) AS customer_average_check
        FROM transactions
        JOIN cards
          ON transactions.customer_card_id = cards.customer_card_id
    GROUP BY customer_id
    ORDER BY customer_id
  ), customer_frequency AS (
    SELECT 
      customer_id,
      CAST(ROUND((EXTRACT(DAY FROM(max(to_timestamp(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))
        -min(to_timestamp(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))))
        +(EXTRACT(HOUR FROM(max(to_timestamp(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))
        -min(to_timestamp(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))))/24))
        /count(customer_id),2)AS FLOAT) AS customer_frequency
        FROM transactions
        JOIN cards
          ON transactions.customer_card_id = cards.customer_card_id
    GROUP BY customer_id
    ORDER BY customer_id
  ), customer_inactive_period AS (
    SELECT customer_id, 
                                    CAST(ROUND(EXTRACT(DAY FROM(max(to_timestamp(analysis_formation,'DD.MM.YYYY HH24:MI:SS'))-max(to_timestamp(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))))
                                      +EXTRACT(HOUR FROM(max(to_timestamp(analysis_formation,'DD.MM.YYYY HH24:MI:SS'))-max(to_timestamp(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))))/24 ,2)as FLOAT) AS customer_inactive_period
                               FROM transactions
                               JOIN cards
                                 ON transactions.customer_card_id = cards.customer_card_id, date_of_analysis
                           GROUP BY customer_id
                           ORDER BY customer_id
  ), churn_table AS (SELECT customer_inactive_period.customer_id,
                       CAST(round(CAST((customer_inactive_period/customer_frequency) AS NUMERIC),2) AS float) AS customer_churn_rate
                  FROM customer_inactive_period
                  JOIN customer_frequency
                    ON customer_inactive_period.customer_id = customer_frequency.customer_id
  ), customer_average_check_segment AS (SELECT customer_id,
                                          CASE 
                                            WHEN PERCENT_RANK() OVER(ORDER BY customer_average_check DESC) <= 0.1 THEN 'High'
                                            WHEN PERCENT_RANK() OVER(ORDER BY customer_average_check DESC) > 0.1 AND
                                              PERCENT_RANK() OVER(ORDER BY customer_average_check DESC) <= 0.35 THEN 'Medium'
                                            ELSE 'Low'
                                          END as customer_average_check_segment
                                     FROM avg_check
  ), customer_frequency_segment AS (SELECT customer_id,
                                      CASE 
                                        WHEN PERCENT_RANK() OVER(ORDER BY customer_frequency) <= 0.1 THEN 'Often'
                                        WHEN PERCENT_RANK() OVER(ORDER BY customer_frequency) > 0.1 AND
                                          PERCENT_RANK() OVER(ORDER BY customer_frequency) <= 0.35 THEN 'Occasionally'
                                        ELSE 'Rarely'
                                      END AS customer_frequency_segment 
                                 FROM customer_frequency
  ), customer_churn_segment AS (SELECT customer_id,
                                  CASE 
                                    WHEN customer_churn_rate >= 0 AND
                                      customer_churn_rate <= 2 THEN 'Low'
                                    WHEN customer_churn_rate > 2 AND
                                      customer_churn_rate <= 5 THEN 'Medium'
                                    ELSE 'High'
                                  END AS customer_churn_segment
                             FROM churn_table
  ), customer_segment AS (SELECT ccs.customer_id,
                            CASE 
                              WHEN customer_average_check_segment = 'Low' THEN
                                CASE 
                                  WHEN customer_frequency_segment = 'Rarely' THEN
                                    CASE 
                                      WHEN customer_churn_segment = 'Low' THEN '1'
                                      WHEN customer_churn_segment = 'Medium' THEN '2'
                                      ELSE '3'
                                    END
                                  WHEN customer_frequency_segment = 'Occasionally' THEN
                                    CASE 
                                      WHEN customer_churn_segment = 'Low' THEN '4'
                                      WHEN customer_churn_segment = 'Medium' THEN '5'
                                      ELSE '6'
                                    END
                                  ELSE 
                                    CASE 
                                      WHEN customer_churn_segment = 'Low' THEN '7'
                                      WHEN customer_churn_segment = 'Medium' THEN '8'
                                      ELSE '9'
                                    END
                                END
                              WHEN customer_average_check_segment = 'Medium' THEN
                                CASE 
                                  WHEN customer_frequency_segment = 'Rarely' THEN
                                    CASE 
                                      WHEN customer_churn_segment = 'Low' THEN '10'
                                      WHEN customer_churn_segment = 'Medium' THEN '11'
                                      ELSE '12'
                                    END
                                  WHEN customer_frequency_segment = 'Occasionally' THEN
                                    CASE 
                                      WHEN customer_churn_segment = 'Low' THEN '13'
                                      WHEN customer_churn_segment = 'Medium' THEN '14'
                                      ELSE '15'
                                    END
                                  ELSE 
                                    CASE 
                                      WHEN customer_churn_segment = 'Low' THEN '16'
                                      WHEN customer_churn_segment = 'Medium' THEN '17'
                                      ELSE '18'
                                    END
                                END
                              ELSE
                                CASE 
                                  WHEN customer_frequency_segment = 'Rarely' THEN
                                    CASE 
                                      WHEN customer_churn_segment = 'Low' THEN '19'
                                      WHEN customer_churn_segment = 'Medium' THEN '20'
                                      ELSE '21'
                                    END
                                  WHEN customer_frequency_segment = 'Occasionally' THEN
                                    CASE 
                                      WHEN customer_churn_segment = 'Low' THEN '22'
                                      WHEN customer_churn_segment = 'Medium' THEN '23'
                                      ELSE '24'
                                    END
                                  ELSE 
                                    CASE 
                                      WHEN customer_churn_segment = 'Low' THEN '25'
                                      WHEN customer_churn_segment = 'Medium' THEN '26'
                                      ELSE '27'
                                    END
                                END
                            END AS customer_segment 
                       FROM customer_average_check_segment AS cacs
                       JOIN customer_frequency_segment AS cfs
                         ON cfs.customer_id = cacs.customer_id
                       JOIN customer_churn_segment AS ccs
                         ON ccs.customer_id = cacs.customer_id
  ), customer_primary_store AS (
    WITH main AS (
      SELECT
        cs.customer_id pers,
        transaction_store_id store,
        COUNT(transaction_store_id) AS cnt
          FROM transactions tr
          JOIN cards cs
            ON cs.customer_card_id = tr.customer_card_id
      GROUP BY 1, 2
      ORDER BY 1
    ), maxs AS (
      SELECT pers, max(cnt) AS mx
        FROM main
      GROUP BY 1
    ), visits AS (
      SELECT
        maxs.pers,
        main.store,
        ROW_NUMBER() OVER(PARTITION BY maxs.pers) AS numbs
      FROM maxs
      JOIN main
        ON maxs.pers = main.pers
      WHERE mx = cnt
      ORDER BY 1,2,3
    ), last_stores AS (
      SELECT 
        cs.customer_id pers,
        transaction_store_id store,
        to_timestamp(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') AS date_
      FROM transactions tr
      JOIN cards cs 
        ON cs.customer_card_id = tr.customer_card_id
      ORDER BY 1, 3 DESC
    ), temp_ AS (
      SELECT *, ROW_NUMBER() OVER(PARTITION BY pers) AS numbs
      FROM last_stores
    ), last_3 AS (
      SELECT pers, store, numbs
      FROM temp_
      WHERE numbs < 4
      ORDER BY 1
    ), check_ AS (
      SELECT *,
        CASE 
          WHEN numbs = 2 THEN
            CASE
              WHEN (lead(store) OVER()) = store THEN
                CASE 
                  WHEN (lag(store) OVER()) = store THEN 'true'
                  ELSE 'false'
                END
              ELSE 'false'
            END
          ELSE 'no'
        END as ident
      FROM last_3
      ORDER BY 1
    ), end_ AS (
      SELECT pers, store, ident FROM check_
      WHERE ident != 'no'
    )
    SELECT end_.pers,
      CASE 
        WHEN end_.ident = 'true' THEN end_.store
        ELSE visits.store
      END AS customer_primary_store
    FROM visits
    JOIN end_
      ON end_.pers = visits.pers
    GROUP BY 1, 2, visits.numbs
    HAVING visits.numbs = 1
  )
  SELECT avg_check.customer_id,
         customer_average_check,
         customer_average_check_segment,
         customer_frequency,
         customer_frequency_segment,
         customer_inactive_period,
         customer_churn_rate,
         customer_churn_segment,
         customer_segment,
         customer_primary_store
    FROM avg_check
    JOIN customer_frequency
      ON avg_check.customer_id = customer_frequency.customer_id
    JOIN customer_inactive_period
      ON avg_check.customer_id = customer_inactive_period.customer_id
    JOIN churn_table
      ON churn_table.customer_id = avg_check.customer_id
    JOIN customer_average_check_segment AS cacs
      ON cacs.customer_id = avg_check.customer_id
    JOIN customer_frequency_segment AS cfs
      ON cfs.customer_id = avg_check.customer_id
    JOIN customer_churn_segment AS ccs
      ON ccs.customer_id = avg_check.customer_id
    JOIN customer_segment AS cs
      ON cs.customer_id = avg_check.customer_id
    JOIN customer_primary_store AS cps  
      ON cps.pers = avg_check.customer_id
);

SELECT * FROM customers;
