DROP TABLE IF EXISTS `prices_calcs`;
CREATE TABLE `prices_calcs` (
	`quote_date`	DATE NOT NULL,
	`symbol`		VARCHAR(6) NOT NULL,
	`fast_macd_fast` FLOAT(4,8) NULL,
	`fast_macd_slow` FLOAT(4,8) NULL,
	`std_macd_fast`	FLOAT(4,8) NULL,
	`std_macd_slow` FLOAT(4,8) NULL,
	`slow_macd_fast` FLOAT(4,8) NULL,
	`slow_macd_slow` FLOAT(4,8) NULL,
	`pe_ratio`		FLOAT(6,4) NULL,
	-- MORE TODO --
	PRIMARY KEY (quote_date,symbol))
ENGINE=InnoDB DEFAULT CHARSET=latin1
