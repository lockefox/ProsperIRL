DROP TABLE IF EXISTS `companies`;
CREATE TABLE `companies` (
	`symbol`	VARCHAR(6) NOT NULL,
	`exchange`	ENUM('NASDAQ','NYSE','AMEX','OTHER') NOT NULL,
	`name` 		TINYTEXT NOT NULL,
	`IPOyear`	int(4) NULL,
	`sector`	VARCHAR(32)	NOT NULL,
	`industry`	VARCHAR(32)	NOT NULL,
	`summary`	TINYTEXT NULL,
	PRIMARY KEY (symbol))
ENGINE=InnoDB DEFAULT CHARSET=latin1
