DROP TABLE IF EXISTS `companies`;
CREATE TABLE `companies` (
	`symbol`	VARCHAR(8) NOT NULL,
	`exchange`	ENUM('NASDAQ','NYSE','AMEX','OTHER') NOT NULL,
	`name` 		TINYTEXT NOT NULL,
	`IPOyear`	int(4) NULL,
	`sector`	VARCHAR(32) NULL,
	`industry`	VARCHAR(64) NULL,
	`summary`	TINYTEXT NULL,
	PRIMARY KEY (symbol))
ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE INDEX company_sector ON companies(sector);
CREATE INDEX company_industry ON companies(industry);
CREATE INDEX company_exchange ON companies(exchange);
CREATE INDEX company_IPO ON companies(IPOyear)