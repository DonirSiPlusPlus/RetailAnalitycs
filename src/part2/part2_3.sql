DROP VIEW IF EXISTS periods CASCADE;
CREATE OR REPLACE VIEW periods AS(
WITH min_date AS(SELECT customer_id,
                        group_id,
                        min(to_timestamp(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')::TIMESTAMP) AS First_Group_Purchase_Date
                   FROM transactions
                   JOIN cards 
                     ON cards.customer_card_id = transactions.customer_card_id
                   JOIN checks 
                     ON checks.transaction_id = transactions.transaction_id
                   JOIN product_grid 
                     ON checks.sku_id = product_grid.sku_id
               GROUP BY group_id, customer_id
               ORDER BY 1 , 2),
max_date AS(SELECT customer_id,
                   group_id,
                   max(to_timestamp(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')::TIMESTAMP) AS Last_Group_Purchase_Date
              FROM transactions
              JOIN cards 
                ON cards.customer_card_id = transactions.customer_card_id
              JOIN checks 
                ON checks.transaction_id = transactions.transaction_id
              JOIN product_grid 
                ON checks.sku_id = product_grid.sku_id
          GROUP BY group_id, customer_id
          ORDER BY 1 , 2),
purchase_sum AS(SELECT customer_id,
                       group_id,
                       count(group_id) AS Group_Purchase
                  FROM transactions
                  JOIN cards 
                    ON cards.customer_card_id = transactions.customer_card_id
                  JOIN checks 
                    ON checks.transaction_id = transactions.transaction_id
                  JOIN product_grid 
                    ON checks.sku_id = product_grid.sku_id
              GROUP BY group_id, customer_id
              ORDER BY 1 , 2),
frequency AS (SELECT max_date.customer_id,
                     max_date.group_id,
                     CAST(round((EXTRACT(DAY from(last_group_purchase_date-first_group_purchase_date))
                      + EXTRACT(HOUR from(last_group_purchase_date-first_group_purchase_date))/24+1)/group_purchase ,2)AS FLOAT)AS Group_Frequency
                FROM max_date
                JOIN min_date 
                  ON (max_date.customer_id = min_date.customer_id) AND (max_date.group_id = min_date.group_id)
                JOIN purchase_sum 
                  ON (max_date.customer_id = purchase_sum.customer_id) AND (max_date.group_id = purchase_sum.group_id)),
no_zero_sell AS (SELECT customer_id,
                        group_id,
                        sku_discount/ sku_summ AS no_zero
                   FROM checks 
                   JOIN product_grid 
                     ON checks.sku_id = product_grid.sku_id
                   JOIN transactions 
                     ON checks. transaction_id = transactions.transaction_id
                   JOIN cards 
                     ON transactions.customer_card_id = cards. customer_card_id
               GROUP BY customer_id,group_id,sku_discount,sku_summ
                 HAVING sku_discount/sku_summ > 0
              LIMIT ALL),
have_zero AS (SELECT customer_id,
                     group_id,
                     min(sku_discount/ sku_summ) AS have_zero
                FROM checks 
                JOIN product_grid 
                  ON checks.sku_id = product_grid.sku_id
                JOIN transactions 
                  ON checks. transaction_id = transactions.transaction_id
                JOIN cards 
                  ON transactions.customer_card_id = cards. customer_card_id
            GROUP BY customer_id,group_id
           LIMIT ALL),
min_sell_no_zero AS (SELECT customer_id,
                            group_id, 
                            min(no_zero)
                       FROM no_zero_sell
                   GROUP BY customer_id,group_id),
min_discount AS (SELECT have_zero.customer_id,
                        have_zero.group_id,
                        CAST(round(CAST(COALESCE(min ,0)AS NUMERIC),2)AS FLOAT) AS Group_Min_Discount
                   FROM have_zero 
              LEFT JOIN min_sell_no_zero 
                     ON have_zero.customer_id = min_sell_no_zero.customer_id AND have_zero.group_id = min_sell_no_zero.group_id)
SELECT max_date.customer_id,
       max_date.group_id,
       first_group_purchase_date,
       last_group_purchase_date,
       group_purchase,
       group_frequency,
       Group_Min_Discount
  FROM max_date
  JOIN min_date 
    ON (max_date.customer_id, max_date.group_id) = (min_date.customer_id, min_date.group_id)
  JOIN purchase_sum 
    ON (max_date.customer_id, max_date.group_id) = (purchase_sum.customer_id, purchase_sum.group_id)
  JOIN frequency 
    ON (max_date.customer_id, max_date.group_id) = (frequency.customer_id, frequency.group_id)
  JOIN min_discount
    ON (max_date.customer_id, max_date.group_id) = (min_discount.customer_id, min_discount.group_id)
);
SELECT * FROM periods;