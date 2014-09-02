DROP TABLE IF EXISTS `index_listing`;
CREATE TABLE `index_listing` (
	`index_symbol`	VARCHAR(8) NOT NULL,
	`index_name`	TINYTEXT NOT NULL,
	`index_source`	TINYTEXT NOT NULL,
	PRIMARY KEY (index_symbol))
ENGINE=InnoDB DEFAULT CHARSET=latin1
