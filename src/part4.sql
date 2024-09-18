DROP FUNCTION IF EXISTS personal_offer_by_average_bill;
CREATE OR REPLACE FUNCTION personal_offer_by_average_bill ( method INTEGER,
                                                            period_first VARCHAR DEFAULT NULL::VARCHAR,
                                                            period_second VARCHAR DEFAULT NULL::VARCHAR,
                                                            count_of_transaction INTEGER DEFAULT NULL::INTEGER,
                                                            coefficient NUMERIC DEFAULT 1,
                                                            max_group_churn_rate NUMERIC DEFAULT NULL::NUMERIC,
                                                            max_share_of_transactioon_with_discount NUMERIC DEFAULT NULL::NUMERIC,
                                                            acceptable_share_of_margin NUMERIC DEFAULT NULL::NUMERIC)
  RETURNS TABLE
                (
                    customer_id BIGINT,
                    required_check_measure FLOAT,
                    group_name VARCHAR,
                    offer_discount_depth FLOAT
                )
LANGUAGE plpgsql
AS
$$
DECLARE
    row record;
    res CHARACTER VARYING;
    mind NUMERIC;
	mavg NUMERIC;
    cr INTEGER = 0;
BEGIN
    IF method = 1
    THEN
        res:= 'SELECT t1.customer_id
                      ,t1.group_id
                      ,AVG(transaction_summ) AS sum_of_transactions
                 FROM purchases_history t1
		   CROSS JOIN date_of_analysis t2
		   INNER JOIN transactions t3 ON t1.transaction_id = t3.transaction_id
				WHERE to_timestamp(t3.transaction_datetime,''DD.MM.YYYY HH24:MI:SS'')
			  BETWEEN to_timestamp(''' || period_first || ''', ''DD.MM.YYYY HH24:MI:SS'')
                  AND to_timestamp(''' || period_second || ''', ''DD.MM.YYYY HH24:MI:SS'')
			 GROUP BY 1, 2';
    ELSE
        res:='WITH s1 AS (SELECT
                                 customer_id
                                 ,group_id
                                 ,to_timestamp(purchases_history.transaction_datetime,''DD.MM.YYYY HH24:MI:SS'') AS date_of_transaction
                                 ,ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY to_timestamp(transaction_datetime,''DD.MM.YYYY HH24:MI:SS'') DESC) AS rn
                                 ,AVG(purchases_history.group_summ) OVER (PARTITION BY customer_id) AS sum_of_transactions
                            FROM purchases_history
                        GROUP BY customer_id
                                 ,group_id
                                 ,date_of_transaction
                                 ,purchases_history.group_summ
                        ORDER BY customer_id
                                 ,date_of_transaction DESC)
            SELECT
                   customer_id
                   ,group_id
                   ,date_of_transaction
                   ,sum_of_transactions
              FROM s1
             WHERE rn <= ' || count_of_transaction || '
          GROUP BY customer_id
                   ,date_of_transaction
                   ,group_id
                   ,sum_of_transactions
          ORDER BY customer_id
                   ,date_of_transaction DESC';
    END IF;

    res:= 'WITH s AS (' || res || ')
           SELECT t1.*,
                  t2.group_id,
                  t3.group_name,
                  group_affinity_index,
                  group_churn_rate,
                  group_discount_share,
                  group_average_discount,
                  group_min_discount,
                  group_margin,
                  group_stability_index
            FROM s t1
      INNER JOIN sku_group t3 ON t1.group_id = t3.group_id
      INNER JOIN groups t2 ON t1.customer_id = t2.customer_id AND t1.group_id = t2.group_id
           WHERE group_churn_rate <= ' || max_group_churn_rate || ' AND group_discount_share < ' || max_share_of_transactioon_with_discount / 100 ||'
        ORDER BY customer_id, group_min_discount, group_affinity_index desc';

    FOR row IN EXECUTE res
        LOOP
            IF row.customer_id <> cr THEN
                customer_id = row.customer_id;
                required_check_measure = CAST((row.sum_of_transactions * coefficient) AS NUMERIC(10,2));
                group_name = row.group_name;

                SELECT AVG(group_summ_paid - group_cost)
                  INTO mavg
                  FROM purchases_history t1
            CROSS JOIN date_of_analysis t2
                 WHERE t1.customer_id = row.customer_id
                   AND t1.group_id = row.group_id;

                offer_discount_depth := mavg * (acceptable_share_of_margin / 100);
                mind := (FLOOR((row.group_min_discount::NUMERIC(10, 2) * 100) / 5) * 0.05);
                IF (mavg > 0 AND row.group_min_discount::NUMERIC(10, 2) > 0 AND mind * mavg < offer_discount_depth)
                    THEN cr := row.customer_id;
                    IF mind = 0 THEN
                        mind = 0.05;
                    END IF;
                    offer_discount_depth := mind * 100;
                    RETURN NEXT;
                END IF;
            END IF;
        END LOOP;
END;
$$

SELECT *
FROM personal_offer_by_average_bill (2,'10.06.2020 00:00:00', '21.06.2022 23:59:59',100,1.15,3,70,30);
