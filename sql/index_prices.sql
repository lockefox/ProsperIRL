DROP TABLE IF EXISTS `index_prices`;
CREATE TABLE `index_prices` (
	`quote_date`	DATE,
	`index_symbol`	VARCHAR(6) NOT NULL,
	`open_price`	FLOAT(5,2) NULL,
	`high_price`	FLOAT(5,2) NULL,
	`low_price`		FLOAT(5,2) NULL,
	`close_price`	FLOAT(5,2) NOT NULL,
	`adj_close`		FLOAT(5,2) NULL,
	PRIMARY KEY (date,index_symbol))
ENGINE=InnoDB DEFAULT CHARSET=latin1