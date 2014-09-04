DROP TABLE IF EXISTS `quantl_sources`;
CREATE TABLE `quantl_sources` (
	`source`	TINYTEXT NOT NULL,
	`code`		TINYTEXT NOT NULL,
	`name`		TINYTEXT NULL,
	`frequency`	ENUM('none','daily','weekly','monthly','quarterly','annual') 'none',
	`startdate` DATE NULL,
	`updated_at` DATETIME NOT NULL,
	`id`		INT(8) NULL,
	`columns`	MEDIUMTEXT NULL,
	PRIMARY KEY (source,code))
	ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE INDEX source_freshness ON quantl_sources(updated_at)