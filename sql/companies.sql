DROP TABLE IF EXISTS `companies`;
CREATE TABLE `companies` (
	`symbol`	VARCHAR(6) NOT NULL,
	`exchange`	VARCHAR(6) NOT NULL,
	`name` 		VARCHAR(128) NOT NULL,
	`IPOyear`	int(4) NULL,
	`sector`	VARCHAR(32)	NOT NULL,
	`industry`	VARCHAR(32)	NOT NULL,
	`summary`	VARCHAR(128) NULL,
	PRIMARY KEY (symbol))
ENGINE=InnoDB DEFAULT CHARSET=latin1
