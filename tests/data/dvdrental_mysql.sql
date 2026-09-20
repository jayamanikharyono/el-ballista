-- GENERATED from tests/data/dvdrental (PostgreSQL dump) by tests/docker/gen_mysql_seed.py.
-- Do not edit by hand. Single source of truth = the .dat files under tests/data/dvdrental/;
-- this file only maps the same tab-separated COPY data into MySQL via LOAD DATA.
-- Postgres loads the identical .dat files natively via pg_restore.
-- The .dat files carry Postgres COPY framing (a trailing '\.' terminator line plus
-- blank lines) which pg_restore consumes but LOAD DATA would ingest as data rows
-- (text PK coerced to 0 -> duplicate-key error). tests/docker/mysql-prep.sh strips
-- that framing into /tmp/dvdrental/ at container init; this file reads the cleaned
-- copies, never /dvdrental/ directly.

SET FOREIGN_KEY_CHECKS = 0;
-- Keep STRICT so bad rows fail loudly instead of being coerced (e.g. text -> 0).
SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';

DROP TABLE IF EXISTS `actor`;
CREATE TABLE `actor` (
  `actor_id` INT,
  `first_name` VARCHAR(45),
  `last_name` VARCHAR(45),
  `last_update` DATETIME(6),
  PRIMARY KEY (`actor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `category`;
CREATE TABLE `category` (
  `category_id` INT,
  `name` VARCHAR(25),
  `last_update` DATETIME(6),
  PRIMARY KEY (`category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `country`;
CREATE TABLE `country` (
  `country_id` INT,
  `country` VARCHAR(50),
  `last_update` DATETIME(6),
  PRIMARY KEY (`country_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `city`;
CREATE TABLE `city` (
  `city_id` INT,
  `city` VARCHAR(50),
  `country_id` SMALLINT,
  `last_update` DATETIME(6),
  PRIMARY KEY (`city_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `address`;
CREATE TABLE `address` (
  `address_id` INT,
  `address` VARCHAR(50),
  `address2` VARCHAR(50),
  `district` VARCHAR(20),
  `city_id` SMALLINT,
  `postal_code` VARCHAR(10),
  `phone` VARCHAR(20),
  `last_update` DATETIME(6),
  PRIMARY KEY (`address_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `language`;
CREATE TABLE `language` (
  `language_id` INT,
  `name` CHAR(20),
  `last_update` DATETIME(6),
  PRIMARY KEY (`language_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `film`;
CREATE TABLE `film` (
  `film_id` INT,
  `title` VARCHAR(255),
  `description` TEXT,
  `release_year` SMALLINT,
  `language_id` SMALLINT,
  `rental_duration` SMALLINT,
  `rental_rate` DECIMAL(4,2),
  `length` SMALLINT,
  `replacement_cost` DECIMAL(5,2),
  `rating` ENUM('G','PG','PG-13','R','NC-17'),
  `last_update` DATETIME(6),
  `special_features` TEXT,
  `fulltext` TEXT,
  PRIMARY KEY (`film_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `film_actor`;
CREATE TABLE `film_actor` (
  `actor_id` SMALLINT,
  `film_id` SMALLINT,
  `last_update` DATETIME(6),
  PRIMARY KEY (`actor_id`, `film_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `film_category`;
CREATE TABLE `film_category` (
  `film_id` SMALLINT,
  `category_id` SMALLINT,
  `last_update` DATETIME(6),
  PRIMARY KEY (`film_id`, `category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `customer`;
CREATE TABLE `customer` (
  `customer_id` INT,
  `store_id` SMALLINT,
  `first_name` VARCHAR(45),
  `last_name` VARCHAR(45),
  `email` VARCHAR(50),
  `address_id` SMALLINT,
  `activebool` TINYINT(1),
  `create_date` DATE,
  `last_update` DATETIME(6),
  `active` INT,
  PRIMARY KEY (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `store`;
CREATE TABLE `store` (
  `store_id` INT,
  `manager_staff_id` SMALLINT,
  `address_id` SMALLINT,
  `last_update` DATETIME(6),
  PRIMARY KEY (`store_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `staff`;
CREATE TABLE `staff` (
  `staff_id` INT,
  `first_name` VARCHAR(45),
  `last_name` VARCHAR(45),
  `address_id` SMALLINT,
  `email` VARCHAR(50),
  `store_id` SMALLINT,
  `active` TINYINT(1),
  `username` VARCHAR(16),
  `password` VARCHAR(40),
  `last_update` DATETIME(6),
  `picture` LONGBLOB,
  PRIMARY KEY (`staff_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `inventory`;
CREATE TABLE `inventory` (
  `inventory_id` INT,
  `film_id` SMALLINT,
  `store_id` SMALLINT,
  `last_update` DATETIME(6),
  PRIMARY KEY (`inventory_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `rental`;
CREATE TABLE `rental` (
  `rental_id` INT,
  `rental_date` DATETIME(6),
  `inventory_id` INT,
  `customer_id` SMALLINT,
  `return_date` DATETIME(6),
  `staff_id` SMALLINT,
  `last_update` DATETIME(6),
  PRIMARY KEY (`rental_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `payment`;
CREATE TABLE `payment` (
  `payment_id` INT,
  `customer_id` SMALLINT,
  `staff_id` SMALLINT,
  `rental_id` INT,
  `amount` DECIMAL(5,2),
  `payment_date` DATETIME(6),
  PRIMARY KEY (`payment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Data load: identical tab-separated .dat files, \N = NULL (MySQL LOAD DATA default).
LOAD DATA INFILE '/tmp/dvdrental/3057.dat' INTO TABLE `actor`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`actor_id`, `first_name`, `last_name`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3059.dat' INTO TABLE `category`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`category_id`, `name`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3069.dat' INTO TABLE `country`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`country_id`, `country`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3067.dat' INTO TABLE `city`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`city_id`, `city`, `country_id`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3065.dat' INTO TABLE `address`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`address_id`, `address`, `address2`, `district`, `city_id`, `postal_code`, `phone`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3073.dat' INTO TABLE `language`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`language_id`, `name`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3061.dat' INTO TABLE `film`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`film_id`, `title`, `description`, `release_year`, `language_id`, `rental_duration`, `rental_rate`, `length`, `replacement_cost`, `rating`, `last_update`, `special_features`, `fulltext`);
LOAD DATA INFILE '/tmp/dvdrental/3062.dat' INTO TABLE `film_actor`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`actor_id`, `film_id`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3063.dat' INTO TABLE `film_category`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`film_id`, `category_id`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3055.dat' INTO TABLE `customer`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`customer_id`, `store_id`, `first_name`, `last_name`, `email`, `address_id`, @activebool, `create_date`, `last_update`, `active`)
  SET `activebool` = IF(@activebool = 't', 1, IF(@activebool = 'f', 0, NULL));
LOAD DATA INFILE '/tmp/dvdrental/3081.dat' INTO TABLE `store`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`store_id`, `manager_staff_id`, `address_id`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3079.dat' INTO TABLE `staff`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`staff_id`, `first_name`, `last_name`, `address_id`, `email`, `store_id`, @active, `username`, `password`, `last_update`, `picture`)
  SET `active` = IF(@active = 't', 1, IF(@active = 'f', 0, NULL));
LOAD DATA INFILE '/tmp/dvdrental/3071.dat' INTO TABLE `inventory`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`inventory_id`, `film_id`, `store_id`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3077.dat' INTO TABLE `rental`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`rental_id`, `rental_date`, `inventory_id`, `customer_id`, `return_date`, `staff_id`, `last_update`);
LOAD DATA INFILE '/tmp/dvdrental/3075.dat' INTO TABLE `payment`
  FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  (`payment_id`, `customer_id`, `staff_id`, `rental_id`, `amount`, `payment_date`);

-- staff.picture parity: Postgres COPY text format encodes bytea as '\\x<hex>';
-- LOAD DATA's ESCAPED BY only collapses the leading '\\\\' to '\', leaving the
-- 18 ASCII chars '\x8950…' in the blob while Postgres holds the decoded 8 bytes.
-- Decode here so both engines hold identical bytes. The LEFT(..) guard makes this
-- a no-op on re-run (decoded bytes start with 0x89, never '\x'). Only staff.picture
-- is bytea in this dataset (tsvector/text[] stay text by design — see extraction_matrix).
UPDATE `staff` SET `picture` = UNHEX(SUBSTRING(`picture`, 3))
  WHERE LEFT(`picture`, 2) = '\\x' AND LENGTH(`picture`) > 2;

SET FOREIGN_KEY_CHECKS = 1;
