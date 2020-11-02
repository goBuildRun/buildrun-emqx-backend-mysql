DROP TABLE IF EXISTS `mqtt_retain`;
CREATE TABLE `mqtt_retain` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `topic` varchar(200) DEFAULT NULL,
  `msgid` varchar(60) DEFAULT NULL,
  `sender` varchar(100) DEFAULT NULL,
  `node` varchar(100) DEFAULT NULL,
  `qos` int(2) DEFAULT NULL,
  `payload` blob,
  `arrived` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `mqtt_retain_key` (`topic`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

