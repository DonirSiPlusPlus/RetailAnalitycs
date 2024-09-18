DROP FUNCTION IF EXISTS part5(VARCHAR, VARCHAR, INTEGER, NUMERIC, NUMERIC, NUMERIC);

CREATE OR REPLACE FUNCTION part5 (
          first_date VARCHAR,
            end_date VARCHAR,
  transactions_count INTEGER,
         churn_index NUMERIC,
      max_share_trns NUMERIC,
        share_margin NUMERIC
)
RETURNS TABLE (
                  customer_id BIGINT,
                   start_date VARCHAR,
                     end_date VARCHAR,
  required_transactions_count INTEGER,
                   group_name VARCHAR,
         offer_discount_depth NUMERIC
)
AS $$
  WITH cond2 AS (
    SELECT customer_id,
           round(extract(DAY FROM(to_timestamp(end_date, 'DD.MM.YYYY HH24:MI:SS') - to_timestamp(first_date, 'DD.MM.YYYY HH24:MI:SS')))/customer_frequency) AS cur_freq
      FROM customers
  ), cond4_1 AS (
      SELECT groups.customer_id,
             transaction_id,
             max(group_affinity_index) AS max_gai
        FROM transactions tr
        JOIN cards cs
          ON cs.customer_card_id = tr.customer_card_id
        JOIN groups
          ON groups.customer_id = cs.customer_id
       WHERE to_timestamp(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') BETWEEN
             to_timestamp(first_date, 'DD.MM.YYYY HH24:MI:SS') AND to_timestamp(end_date, 'DD.MM.YYYY HH24:MI:SS')
    group by 1, 2
  ), cond4_2_3 AS (
    SELECT 
      transaction_id,
      group_id,
      group_discount_share,
      group_churn_rate,
      CASE 
        WHEN group_churn_rate < churn_index THEN 'yes'  -- < churn_index
        ELSE 'no'
      END AS compare
    FROM transactions tr
    JOIN cards cs
      ON cs.customer_card_id = tr.customer_card_id
    JOIN groups
      ON groups.customer_id = cs.customer_id
    WHERE group_discount_share < max_share_trns -- < max_share_trns
    order by 1, 2 desc
  ), cond4_2mx AS (
    SELECT
      transaction_id,
      group_id,
      max(group_churn_rate) as group_churn_rate
    FROM cond4_2_3
    GROUP BY 1, 2, compare
    HAVING compare = 'yes'
  ), cond3 AS (
    SELECT cond4_1.customer_id,
           cond4_1.transaction_id,
           group_id,
           group_churn_rate,
           max_gai,
           cur_freq + transactions_count AS required_transactions_count  -- + transactions_count
      FROM cond4_1
      JOIN cond4_2mx
        ON cond4_2mx.transaction_id = cond4_1.transaction_id
      JOIN cond2
        ON cond2.customer_id = cond4_1.customer_id
  ), cond5 AS (
    SELECT cond3.customer_id,
           cond3.transaction_id,
           cond3.group_id,
           cond3.group_churn_rate,
           cond3.max_gai,
           required_transactions_count,
           group_min_discount,
           COALESCE(group_margin, 0) * share_margin AS Offer_Discount_Depth  -- * share_margin
      FROM cond3
      JOIN groups gr
        ON gr.customer_id = cond3.customer_id AND
           gr.group_id = cond3.group_id 
  ), cond6_main AS (
     SELECT *,
            CASE 
             WHEN (group_min_discount::NUMERIC % 0.05) = 0 THEN group_min_discount
             ELSE group_min_discount::NUMERIC + (0.05 - (group_min_discount::NUMERIC % 0.05))
            END as round_disc
       FROM cond5
    ORDER BY 1, 9
  )
  SELECT DISTINCT
         customer_id,
         first_date,
         end_date,
         required_transactions_count,
         group_name,
         Offer_Discount_Depth
    FROM cond6_main
    JOIN sku_group sg
      ON cond6_main.group_id = sg.group_id
$$ LANGUAGE SQL;

SELECT * FROM part5('18.08.2021 00:00:00', '18.08.2022 00:00:00', 1, 3, 70, 30)
