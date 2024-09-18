DROP VIEW IF EXISTS purchases_history CASCADE;

CREATE OR REPLACE VIEW purchases_history AS
   SELECT customer_id,
          transactions.transaction_id,
          transaction_datetime,
          group_id,
          CAST(ROUND(CAST(SUM(sku_amount*sku_purchase_price)AS NUMERIC),2)AS FLOAT) AS Group_Cost,
          CAST(ROUND(CAST(SUM(sku_summ)AS NUMERIC),2)AS FLOAT) Group_Summ,
          CAST(ROUND(CAST(SUM(SKU_Summ_Paid)AS NUMERIC),2)AS FLOAT) AS Group_Summ_Paid
     FROM transactions 
     JOIN checks 
       ON transactions.transaction_id = checks.transaction_id
     JOIN cards 
       ON cards.customer_card_id = transactions.customer_card_id
     JOIN product_grid 
       ON product_grid.sku_id = checks.sku_id
     JOIN stores
       ON stores.transaction_store_id = transactions.transaction_store_id AND checks.sku_id = stores.sku_id
 GROUP BY 1,2,3,4
 ORDER BY customer_id ,transaction_id
LIMIT ALL;

SELECT * FROM purchases_history
LIMIT ALL;
