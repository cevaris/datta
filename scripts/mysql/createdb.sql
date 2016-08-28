CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;
CREATE TABLE IF NOT EXISTS test_table (
    test_varchar VARCHAR(20),
    test_char CHAR(1),
    test_datetime DATETIME,
    test_int INT,
    test_float FLOAT(12,12),
    test_blob BLOB
);

CREATE USER 'test_user'@'localhost' IDENTIFIED BY '$EcrEt0$aucE';
GRANT ALL PRIVILEGES ON * . * TO 'test_user'@'localhost';
FLUSH PRIVILEGES;