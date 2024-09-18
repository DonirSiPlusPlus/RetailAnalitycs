-- DROPS
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS cards;
DROP TABLE IF EXISTS personal_information;
DROP TABLE IF EXISTS stores;
DROP TABLE IF EXISTS checks;
DROP TABLE IF EXISTS product_grid;
DROP TABLE IF EXISTS SKU_group;
DROP TABLE IF EXISTS date_of_analysis;
DROP DOMAIN IF EXISTS Email_type;
DROP DOMAIN IF EXISTS Phone_type;
DROP DOMAIN IF EXISTS Name_type;
DROP DOMAIN IF EXISTS group_name;
DROP DOMAIN IF EXISTS data_time;
DROP PROCEDURE IF EXISTS import_personal_information CASCADE;
DROP PROCEDURE IF EXISTS import_cards CASCADE;
DROP PROCEDURE IF EXISTS import_SKU_group CASCADE;
DROP PROCEDURE IF EXISTS import_product_grid CASCADE;
DROP PROCEDURE IF EXISTS import_stores CASCADE;
DROP PROCEDURE IF EXISTS import_checks CASCADE;
DROP PROCEDURE IF EXISTS import_transactions CASCADE;
DROP PROCEDURE IF EXISTS import_date_of_analysis CASCADE;
DROP PROCEDURE IF EXISTS export_personal_information CASCADE;
DROP PROCEDURE IF EXISTS export_cards CASCADE;
DROP PROCEDURE IF EXISTS export_SKU_group CASCADE;
DROP PROCEDURE IF EXISTS export_product_grid CASCADE;
DROP PROCEDURE IF EXISTS export_stores CASCADE;
DROP PROCEDURE IF EXISTS export_checks CASCADE;
DROP PROCEDURE IF EXISTS export_transactions CASCADE;
DROP PROCEDURE IF EXISTS export_date_of_analysis CASCADE;

--DOMAIN

CREATE DOMAIN Email_type as VARCHAR
CHECK(
   VALUE ~ '\w{1,}[@]\w{1,}[.]\w{1,}'
);

CREATE DOMAIN Phone_type as VARCHAR
CHECK(
   VALUE ~ '\+[7][0-9]{10}'
);

CREATE DOMAIN Name_type as VARCHAR
CHECK(
   VALUE ~ '^([A-Z]|[А-Я]){1}([a-z]|[а-я]|\-|\s){1,}'
);

CREATE DOMAIN group_name as VARCHAR
CHECK(
  VALUE ~ '\S'
);

CREATE DOMAIN data_time as VARCHAR
CHECK (
  VALUE ~ '(([0-2]\d)|(3[0-1])).((0\d)|(1[0-2])).((202[0-3])|(20[0-1]\d))\s((1?\d)|(2[0-3])):([0-5]\d):([0-5]\d)'
);

-- TABLE
CREATE TABLE personal_information(
  Customer_ID SERIAL PRIMARY KEY,
  Customer_Name Name_type NOT NULL,
  Customer_Surname Name_type NOT NULL,
  Customer_Primary_Email Email_type UNIQUE NOT NULL,
  Customer_Primary_Phone Phone_type UNIQUE NOT NULL
);

CREATE TABLE cards(
  Customer_Card_ID SERIAL PRIMARY KEY,
  Customer_ID BIGINT REFERENCES personal_information (Customer_ID) NOT NULL
);

CREATE TABLE SKU_group(
  Group_ID SERIAL PRIMARY KEY,
  Group_Name group_name UNIQUE NOT NULL
);

CREATE TABLE product_grid(
  SKU_ID SERIAL PRIMARY KEY,
  SKU_Name VARCHAR NOT NULL,
  Group_ID BIGINT REFERENCES SKU_group (Group_ID) NOT NULL
);

CREATE TABLE stores(
  Transaction_Store_ID BIGINT NOT NULL,
  SKU_ID BIGINT REFERENCES product_grid(SKU_ID),
  SKU_Purchase_Price FLOAT NOT NULL,
  SKU_Retail_Price FLOAT NOT NULL
);

CREATE TABLE checks(
  Transaction_ID SERIAL PRIMARY KEY,
  SKU_ID BIGINT REFERENCES product_grid(SKU_ID) NOT NULL,
  SKU_Amount FLOAT NOT NULL,
  SKU_Summ FLOAT NOT NULL,
  SKU_Summ_Paid FLOAT NOT NULL,
  SKU_Discount FLOAT NOT NULL
);

CREATE TABLE transactions(
  Transaction_ID SERIAL PRIMARY KEY,
  Customer_Card_ID BIGINT REFERENCES cards(Customer_Card_ID) NOT NULL,
  Transaction_Summ FLOAT NOT NULL,
  Transaction_DateTime data_time NOT NULL,
  Transaction_Store_ID BIGINT NOT NULL
);

CREATE TABLE date_of_analysis(
  Analysis_Formation data_time
);
-- procedures
  -- import
CREATE OR REPLACE PROCEDURE import_personal_information(delimit VARCHAR) AS
$$
COPY personal_information FROM '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/import/personal_information.csv'
DELIMITER delimit CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE import_cards(delimit VARCHAR) AS
$$
COPY cards FROM '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/import/cards.csv'
DELIMITER delimit CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE import_product_grid(delimit VARCHAR) AS
$$
COPY product_grid FROM '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/import/product_grid.csv'
DELIMITER delimit CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE import_SKU_group(delimit VARCHAR) AS
$$
COPY SKU_group FROM '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/import/SKU_group.csv'
DELIMITER delimit CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE import_stores(delimit VARCHAR) AS
$$
COPY stores FROM '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/import/stores.csv'
DELIMITER delimit CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE import_checks(delimit VARCHAR) AS
$$
COPY checks FROM '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/import/checks.csv'
DELIMITER delimit CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE import_transactions(delimit VARCHAR) AS
$$
COPY transactions FROM '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/import/transactions.csv'
DELIMITER delimit CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE import_date_of_analysis(delimit VARCHAR) AS
$$
COPY date_of_analysis FROM '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/import/date_of_analysis.csv'
DELIMITER delimit CSV header;
$$ LANGUAGE SQL;
  -- export
CREATE OR REPLACE PROCEDURE export_personal_information() AS
$$
COPY personal_information TO '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/export/personal_information.csv'
DELIMITER ',' CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_cards() AS
$$
COPY cards TO '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/export/cards.csv'
DELIMITER ',' CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_product_grid() AS
$$
COPY product_grid TO '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/export/product_grid.csv'
DELIMITER ',' CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_SKU_group() AS
$$
COPY SKU_group TO '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/export/SKU_group.csv'
DELIMITER ',' CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_stores() AS
$$
COPY stores TO '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/export/stores.csv'
DELIMITER ',' CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_checks() AS
$$
COPY checks TO '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/export/checks.csv'
DELIMITER ',' CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_transactions() AS
$$
COPY transactions TO '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/export/transactions.csv'
DELIMITER ',' CSV header;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_date_of_analysis() AS
$$
COPY date_of_analysis TO '/Users/bizarrol/project/SQL/SQL3/SQL3_RetailAnalitycs_v1.0-1/src/export/export_date_of_analysis.csv'
DELIMITER ',' CSV header
$$ LANGUAGE SQl;

-- IMPORTS
CALL import_personal_information(',');
CALL import_cards(',');
CALL import_SKU_group(',');
CALL import_product_grid(',');
CALL import_stores(',');
CALL import_checks(',');
CALL import_transactions(',');
CALL import_date_of_analysis(',');

-- EXPORTS
CALL export_personal_information();
CALL export_cards();
CALL export_SKU_group();
CALL export_product_grid();
CALL export_stores();
CALL export_checks();
CALL export_transactions();
CALL export_date_of_analysis();
