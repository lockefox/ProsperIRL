DROP TABLE IF EXISTS `prices`;
CREATE TABLE `prices` (
	`quote_date`	DATE NOT NULL,
	`symbol`		VARCHAR(8) NOT NULL,
	`open_price`	FLOAT(6,2) NOT NULL,
	`high_price`	FLOAT(6,2) NOT NULL,
	`low_price`		FLOAT(6,2) NOT NULL,
	`close_price`	FLOAT(6,2) NOT NULL,
	`volume`		BIGINT(12) NOT NULL,
	`ex_dividend`	FLOAT(6,4) NULL,
	`split_ratio`	FLOAT(6,4) NULL,
	`adj_open`		FLOAT(6,2) NULL,
	`adj_close`		FLOAT(6,2) NULL,
	`adj_high`		FLOAT(6,2) NULL,
	`adj_low`		FLOAT(6,2) NULL,
	`adj_volume`	BIGINT(12) NOT NULL,
	PRIMARY KEY (quote_date,symbol))
ENGINE=InnoDB DEFAULT CHARSET=latin1