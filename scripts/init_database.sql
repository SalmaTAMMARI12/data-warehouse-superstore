/*Create Database and Schemas*/

-- First of all, create a database called 'DataWarehouse' in postgres
DROP DATABASE IF EXISTS superstore_dw;
CREATE DATABASE superstore_dw;


-- In DataWarehouse database, create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
