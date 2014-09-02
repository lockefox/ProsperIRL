DROP TABLE IF EXISTS `prices`;
CREATE TABLE `prices` (
	`quote_date`	DATE NOT NULL,
	`symbol`		VARCHAR(6) NOT NULL,
	`open_price`	FLOAT(5,2) NULL,
	`high_price`	FLOAT(5,2) NULL,
	`low_price`		FLOAT(5,2) NULL,
	`close_price`	FLOAT(5,2) NOT NULL,
	`volume`		BIGINT(12) NULL,
	`adj_close`		FLOAT(5,2) NULL,
	PRIMARY KEY (quote_date,symbol))
ENGINE=InnoDB DEFAULT CHARSET=latin1