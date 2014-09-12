DROP TABLE IF EXISTS `quandl_sources`;
CREATE TABLE `quandl_sources` (
	`source`	TINYTEXT NOT NULL,
	`code`		TINYTEXT NOT NULL,
	`name`		TINYTEXT NULL,
	`frequency`	ENUM('none','daily','weekly','monthly','quarterly','annual') NULL DEFAULT 'none' ,
	`from_date` DATE NULL,
	`to_date`	DATE NULL,
	`updated_at` DATETIME NOT NULL,
	`id`		INT(8) NULL,
	`columns`	MEDIUMTEXT NULL,
	PRIMARY KEY (source(255),code(255)))
	ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE INDEX source_freshness ON quandl_sources(updated_at)