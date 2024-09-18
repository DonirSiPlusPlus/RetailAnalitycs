DROP FUNCTION IF EXISTS cross_selling;
CREATE OR REPLACE FUNCTION cross_selling (num_groups BIGINT, max_churn_idx NUMERIC, max_stability_idx NUMERIC, max_procent_sku NUMERIC, acceptable_margin NUMERIC)
  RETURNS TABLE
                (
                  "Customer_ID" BIGINT,
                  "SKU_Name" VARCHAR,
                  "Offer_Discount_Depth" NUMERIC
                )
  LANGUAGE plpgsql
AS
$$
BEGIN
RETURN QUERY
WITH user_id AS (SELECT * 
                   FROM (SELECT row_number() OVER (PARTITION BY customer_id),customer_id,group_id,group_churn_rate,group_stability_index
                   FROM groups) t
                  WHERE group_churn_rate <= max_churn_idx AND group_stability_index < max_stability_idx AND row_number <= num_groups),
max_margin_group AS (SELECT user_id.customer_id,
                            user_id.group_id,
                            stores.sku_id,
                            max(sku_retail_price-sku_purchase_price)AS max_margin,
                            sku_retail_price
                       FROM user_id
                       JOIN customers
                         ON customers.customer_id = user_id.customer_id
                       JOIN stores
                         ON stores.transaction_store_id = customers.customer_primary_store
                       JOIN product_grid
                         ON user_id.group_id = product_grid.group_id AND product_grid.sku_id = stores.sku_id
                   GROUP BY user_id.customer_id,user_id.group_id,stores.sku_id,sku_retail_price),
group_margin AS (SELECT customer_id,
                        group_id,
                        count(*) AS group_tr
                   FROM transactions
                   JOIN cards
                     ON transactions.customer_card_id = cards. customer_card_id
                   JOIN checks
                     ON checks.transaction_id = transactions.transaction_id
                   JOIN product_grid
                     ON product_grid.sku_id = checks.sku_id
               GROUP BY customer_id,group_id),
sku_margin AS (SELECT customer_id,
                      group_id,
                      product_grid.sku_id,
                      count(*) AS sku_tr
                 FROM transactions
                 JOIN cards
                   ON transactions.customer_card_id = cards. customer_card_id
                 JOIN checks
                   ON checks.transaction_id = transactions.transaction_id
                 JOIN product_grid
                   ON product_grid.sku_id = checks.sku_id
             GROUP BY customer_id,group_id,product_grid.sku_id),
sku_procent AS (SELECT group_margin.customer_id,
                       sku_margin.group_id,
                       sku_margin.sku_id,
                       sku_tr::NUMERIC/group_tr::NUMERIC AS procent
                  FROM group_margin
                  JOIN sku_margin
                    ON (group_margin.customer_id, group_margin.group_id) = (sku_margin.customer_id, sku_margin.group_id)),
change_min_discount AS (SELECT DISTINCT max_margin_group.customer_id AS cus_id,
                                        max_margin_group.group_id,
                                        max_margin_group.sku_id AS sku_id,
                                        groups.group_min_discount,
                                        round(((max_margin*acceptable_margin)/sku_retail_price)::NUMERIC,2)::FLOAT AS ofd ,round(procent,2)::FLOAT
                          FROM max_margin_group
                          JOIN sku_procent
                            ON (sku_procent.customer_id, sku_procent.group_id) = (max_margin_group.customer_id, max_margin_group.group_id)
                          JOIN groups
                            ON (groups.customer_id, groups.group_id) = (max_margin_group.customer_id, max_margin_group.group_id)
                         WHERE procent <= max_procent_sku
                      ORDER BY 1,2,3)
SELECT cus_id,
       sku_name,
       CASE
         WHEN group_min_discount>ofd
       THEN 
         CASE
           WHEN group_min_discount::NUMERIC % 0.05 = 0
         THEN
           group_min_discount::NUMERIC
         ELSE 
           round((group_min_discount + (0.05 - group_min_discount::NUMERIC % 0.05))::NUMERIC,2)::NUMERIC
         END
       ELSE 
         CASE
           WHEN ofd::NUMERIC % 0.05 = 0
         THEN
           ofd::NUMERIC
         ELSE
           round((ofd + (0.05 - ofd::NUMERIC % 0.05))::NUMERIC,2)::NUMERIC
         END
       END
  FROM change_min_discount
  JOIN product_grid
    ON change_min_discount.sku_id = product_grid.sku_id;
END;
$$;

SELECT *
FROM cross_selling(num_groups=>3,max_churn_idx=>5,max_stability_idx=>5,max_procent_sku=>0.9,acceptable_margin=>0.3);
