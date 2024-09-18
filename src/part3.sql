REVOKE SELECT ON cards,checks,date_of_analysis,personal_information,product_grid,sku_group,stores,transactions FROM guest;
DROP USER IF EXISTS administrator;
DROP ROLE IF EXISTS guest;

CREATE ROLE administrator SUPERUSER PASSWORD 'admin';

CREATE ROLE guest LOGIN PASSWORD 'guest';
GRANT SELECT ON cards,
                checks,
                date_of_analysis,
                personal_information,
                product_grid,
                sku_group,
                stores,
                transactions TO guest;
