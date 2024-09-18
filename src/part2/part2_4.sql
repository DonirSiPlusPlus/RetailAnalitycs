DROP VIEW IF EXISTS groups_first_step CASCADE;
DROP FUNCTION IF EXISTS get_margin;
DROP VIEW IF EXISTS groups CASCADE;

CREATE OR REPLACE FUNCTION get_margin (days INTEGER DEFAULT 0, transaction INTEGER DEFAULT 0)
  RETURNS TABLE
                (
                  customer_id_func BIGINT,
                  group_id_func BIGINT,
                  group_margin_func FLOAT
                )
  LANGUAGE plpgsql
AS
$$
BEGIN
RETURN QUERY
WITH margin_date_time AS (SELECT customer_id,group_id,
                                 to_timestamp(transaction_datetime,'DD.MM.YYYY HH24:MI:SS') AS transaction_datetime,
                                 to_timestamp(analysis_formation,'DD.MM.YYYY HH24:MI:SS') AS analysis_formation,
                                 group_summ-group_cost AS margin
                            FROM purchases_history, date_of_analysis
                           WHERE to_timestamp(transaction_datetime,'DD.MM.YYYY HH24:MI:SS')::DATE >= to_timestamp(analysis_formation,'DD.MM.YYYY HH24:MI:SS')::DATE-days -- количество дней которое стоит вычесть
                        ORDER BY transaction_datetime DESC
                       LIMIT ALL),
sum_margin_date_time AS (SELECT customer_id,
                                group_id,
                                ROUND(SUM(margin)::NUMERIC ,2)::FLOAT
                           FROM margin_date_time 
                       GROUP BY customer_id,group_id),
group_margin_data AS (SELECT groups_first_step.customer_id,
                             groups_first_step.group_id,
                             round AS Group_Margin_data
                        FROM sum_margin_date_time
                  RIGHT JOIN groups_first_step
                          ON (groups_first_step.customer_id,groups_first_step.group_id) = (sum_margin_date_time.customer_id, sum_margin_date_time.group_id)),
margin_limit AS (SELECT customer_id, 
                        transaction_id,
                        to_timestamp(transaction_datetime,'DD.MM.YYYY HH24:MI:SS'),
                        group_id,
                        group_summ-group_cost AS margin
                   FROM purchases_history
               ORDER BY transaction_id DESC
                  LIMIT transaction), -- изменяемое значение
sum_margin_limit AS (SELECT customer_id,
                            group_id,
                            ROUND(SUM(margin)::NUMERIC ,2)::FLOAT
                       FROM margin_limit 
                   GROUP BY customer_id,group_id),
group_margin_limit AS(SELECT groups_first_step.customer_id,
                             groups_first_step.group_id,
                             round AS Group_Margin_limit
                        FROM sum_margin_limit
                  RIGHT JOIN groups_first_step
                          ON (groups_first_step.customer_id,groups_first_step.group_id) = (sum_margin_limit.customer_id, sum_margin_limit.group_id))
SELECT group_margin_limit.customer_id,
       group_margin_limit.group_id,
       CASE 
         WHEN (days=0) THEN group_margin_limit
         ELSE group_margin_data
       END
  FROM group_margin_limit
  JOIN group_margin_data 
    ON (group_margin_limit.customer_id ,group_margin_limit.group_id) = (group_margin_data.customer_id ,group_margin_data.group_id);
END;
$$;

CREATE OR REPLACE VIEW groups_first_step AS(
WITH all_transactions AS (SELECT customer_id, 
                                 count(*) AS all_tr
                            FROM transactions
                            JOIN cards
                              ON cards.customer_card_id = transactions.customer_card_id
                        GROUP BY customer_id),
group_transactions AS (SELECT customer_id, 
                              group_id, 
                              count(group_id) AS group_tr
                         FROM transactions
                         JOIN cards
                           ON cards.customer_card_id = transactions.customer_card_id
                         JOIN checks
                           ON checks.transaction_id = transactions.transaction_id
                         JOIN product_grid
                           ON checks.sku_id = product_grid.sku_id
                     GROUP BY group_id, customer_id),
affinity_index AS (SELECT group_transactions.customer_id,
                          group_id,
                          CAST(ROUND(CAST(group_tr AS NUMERIC)/CAST(all_tr AS NUMERIC),2)AS FLOAT) AS group_affinity_index
                     FROM group_transactions
                LEFT JOIN all_transactions 
                       ON group_transactions.customer_id = all_transactions.customer_id
                 ORDER BY 1,2
                LIMIT ALL),
group_churn_rate AS (SELECT customer_id,
                            group_id,
                            cast(round(CAST((extract(DAY FROM(to_timestamp(analysis_formation,'DD.MM.YYYY HH24:MI:SS')
                              - last_group_purchase_date))+extract(HOUR FROM(to_timestamp(analysis_formation,'DD.MM.YYYY HH24:MI:SS')
                              - last_group_purchase_date))/24)/ group_frequency AS NUMERIC),2)AS FLOAT) AS Group_Churn_Rate
                       FROM periods ,date_of_analysis
                   ORDER BY 1,2
                  LIMIT ALL),
group_stability AS (SELECT all_stability.customer_id,
                           all_stability.group_id,
                           COALESCE(CAST(ROUND(CAST(AVG(all_stability.c)AS NUMERIC),2) AS FLOAT),0) AS Group_Stability_Index
                      FROM (SELECT p.customer_id,
                                   p.group_id,
                                   ABS(EXTRACT(DAY FROM (to_timestamp(transaction_datetime,'DD.MM.YYYY HH24:MI:SS')
                                    - LAG(to_timestamp(transaction_datetime,'DD.MM.YYYY HH24:MI:SS'), 1)OVER (PARTITION BY ph.customer_id, ph.group_id ORDER BY transaction_datetime)))
                                    - p.group_frequency) / p.group_frequency AS c
                              FROM purchases_history ph
                              JOIN periods p
                                ON (ph.customer_id, ph.group_id) = (p.customer_id, p.group_id)) all_stability
                  GROUP BY customer_id, group_id),
discount_group_more_zero AS (SELECT customer_id,
                                    group_id,
                                    sku_discount
                               FROM checks 
                               JOIN transactions 
                                 ON transactions.transaction_id = checks.transaction_id
                               JOIN cards 
                                 ON cards.customer_card_id = transactions.customer_card_id
                               JOIN product_grid 
                                 ON product_grid.sku_id = checks.sku_id
                              WHERE sku_discount>0
                          LIMIT ALL),
discount_by_zero AS (SELECT customer_id,
                            group_id,
                            count(*)AS discount
                       FROM discount_group_more_zero
                   GROUP BY customer_id,group_id),
group_discount_share AS (SELECT group_transactions.customer_id,
                                group_transactions.group_id,
                                cast(round(cast(cast(COALESCE(discount,0) AS FLOAT)/cast(group_tr AS FLOAT)AS NUMERIC),2)AS FLOAT)AS Group_Discount_Share
                           FROM group_transactions
                      LEFT JOIN discount_by_zero 
                             ON group_transactions.customer_id = discount_by_zero.customer_id AND group_transactions.group_id = discount_by_zero.group_id),
firs_step_avg_discount AS (SELECT customer_id,
                                  group_id,
                                  CAST(ROUND(CAST(NULLIF(1-AVG(Group_Summ_Paid / Group_Summ),0)AS NUMERIC),2)AS FLOAT) AS avg_sell
                             FROM purchases_history
                            WHERE group_summ != group_summ_paid
                         GROUP BY customer_id ,group_id),
avg_disount AS (SELECT group_transactions.customer_id,
                       group_transactions.group_id,
                       COALESCE(avg_sell,0) AS Group_Average_Discount
                  FROM group_transactions
             LEFT JOIN firs_step_avg_discount 
                    ON firs_step_avg_discount.customer_id = group_transactions.customer_id AND firs_step_avg_discount.group_id = group_transactions.group_id)
  SELECT group_churn_rate.customer_id,
         group_churn_rate.group_id,
         group_affinity_index,
         Group_Churn_Rate,
         Group_Stability_Index,
         Group_Discount_Share,
         group_min_discount,
         Group_Average_Discount
    FROM group_churn_rate
    JOIN affinity_index 
      ON (affinity_index.customer_id, affinity_index.group_id) = (group_churn_rate.customer_id, group_churn_rate.group_id)
    JOIN group_discount_share 
      ON (group_discount_share.customer_id, group_discount_share.group_id) = (group_churn_rate.customer_id, group_churn_rate.group_id)
    JOIN periods 
      ON (periods.customer_id, periods.group_id) = (group_churn_rate.customer_id, group_churn_rate.group_id)
    JOIN avg_disount 
      ON (avg_disount.customer_id, avg_disount.group_id) = (group_churn_rate.customer_id, group_churn_rate.group_id)
    JOIN group_stability 
      ON (group_stability.customer_id, group_stability.group_id) = (group_churn_rate.customer_id, group_churn_rate.group_id)
ORDER BY customer_id,group_id
);
CREATE OR REPLACE VIEW groups AS(
SELECT groups_first_step.customer_id,
       groups_first_step.group_id,
       group_affinity_index,
       group_churn_rate,
       group_stability_index,
       group_discount_share,
       group_margin_func AS Group_Margin,
       group_min_discount,
       group_average_discount
  FROM groups_first_step
  JOIN get_margin(days=>100,transaction=>0) gm 
    ON (groups_first_step.customer_id , groups_first_step.group_id) = (gm.customer_id_func ,gm.group_id_func));

SELECT *
FROM groups;
