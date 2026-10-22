-- =============================================================================
-- One-time, idempotent bootstrap of the three service schemas and users (R3).
--
-- This is a statement template. db/bootstrap.ps1 replaces each double-braced
-- <SERVICE>_DB_PASSWORD placeholder with an SQL-escaped password and pipes the result to the mysql
-- client on stdin, so no password ever appears on a command line or on disk.
-- Run it as a MySQL administrator. It never drops a schema or changes a server
-- variable; existing users get their password reset and their grants narrowed
-- to their own schema.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS cs6510_transaction
    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS cs6510_inventory
    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS cs6510_analytics
    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- Transaction Service
CREATE USER IF NOT EXISTS 'cs6510_transaction_svc'@'localhost' IDENTIFIED BY '{{TRANSACTION_DB_PASSWORD}}';
CREATE USER IF NOT EXISTS 'cs6510_transaction_svc'@'127.0.0.1' IDENTIFIED BY '{{TRANSACTION_DB_PASSWORD}}';
ALTER USER 'cs6510_transaction_svc'@'localhost' IDENTIFIED BY '{{TRANSACTION_DB_PASSWORD}}';
ALTER USER 'cs6510_transaction_svc'@'127.0.0.1' IDENTIFIED BY '{{TRANSACTION_DB_PASSWORD}}';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'cs6510_transaction_svc'@'localhost';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'cs6510_transaction_svc'@'127.0.0.1';
GRANT ALL PRIVILEGES ON `cs6510_transaction`.* TO 'cs6510_transaction_svc'@'localhost';
GRANT ALL PRIVILEGES ON `cs6510_transaction`.* TO 'cs6510_transaction_svc'@'127.0.0.1';

-- Inventory Service
CREATE USER IF NOT EXISTS 'cs6510_inventory_svc'@'localhost' IDENTIFIED BY '{{INVENTORY_DB_PASSWORD}}';
CREATE USER IF NOT EXISTS 'cs6510_inventory_svc'@'127.0.0.1' IDENTIFIED BY '{{INVENTORY_DB_PASSWORD}}';
ALTER USER 'cs6510_inventory_svc'@'localhost' IDENTIFIED BY '{{INVENTORY_DB_PASSWORD}}';
ALTER USER 'cs6510_inventory_svc'@'127.0.0.1' IDENTIFIED BY '{{INVENTORY_DB_PASSWORD}}';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'cs6510_inventory_svc'@'localhost';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'cs6510_inventory_svc'@'127.0.0.1';
GRANT ALL PRIVILEGES ON `cs6510_inventory`.* TO 'cs6510_inventory_svc'@'localhost';
GRANT ALL PRIVILEGES ON `cs6510_inventory`.* TO 'cs6510_inventory_svc'@'127.0.0.1';

-- Analytics Service
CREATE USER IF NOT EXISTS 'cs6510_analytics_svc'@'localhost' IDENTIFIED BY '{{ANALYTICS_DB_PASSWORD}}';
CREATE USER IF NOT EXISTS 'cs6510_analytics_svc'@'127.0.0.1' IDENTIFIED BY '{{ANALYTICS_DB_PASSWORD}}';
ALTER USER 'cs6510_analytics_svc'@'localhost' IDENTIFIED BY '{{ANALYTICS_DB_PASSWORD}}';
ALTER USER 'cs6510_analytics_svc'@'127.0.0.1' IDENTIFIED BY '{{ANALYTICS_DB_PASSWORD}}';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'cs6510_analytics_svc'@'localhost';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'cs6510_analytics_svc'@'127.0.0.1';
GRANT ALL PRIVILEGES ON `cs6510_analytics`.* TO 'cs6510_analytics_svc'@'localhost';
GRANT ALL PRIVILEGES ON `cs6510_analytics`.* TO 'cs6510_analytics_svc'@'127.0.0.1';

-- Report what now exists.
SELECT SCHEMA_NAME AS schema_name, DEFAULT_CHARACTER_SET_NAME AS charset,
       DEFAULT_COLLATION_NAME AS collation
FROM information_schema.SCHEMATA
WHERE SCHEMA_NAME IN ('cs6510_transaction', 'cs6510_inventory', 'cs6510_analytics')
ORDER BY SCHEMA_NAME;
SELECT User AS user, Host AS host
FROM mysql.user
WHERE User IN ('cs6510_transaction_svc', 'cs6510_inventory_svc', 'cs6510_analytics_svc')
ORDER BY User, Host;
SHOW GRANTS FOR 'cs6510_transaction_svc'@'127.0.0.1';
SHOW GRANTS FOR 'cs6510_inventory_svc'@'127.0.0.1';
SHOW GRANTS FOR 'cs6510_analytics_svc'@'127.0.0.1';
