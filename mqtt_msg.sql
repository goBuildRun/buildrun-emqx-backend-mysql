DROP TABLE IF EXISTS `mqtt_msg`;
CREATE TABLE `mqtt_msg` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `msgid` varchar(100) DEFAULT NULL,
  `topic` varchar(1024) DEFAULT NULL,
  `sender` varchar(1024) DEFAULT NULL,
  `node` varchar(60) DEFAULT NULL,
  `qos` int(11)  DEFAULT '0',
  `retain` tinyint(2) DEFAULT NULL,
  `payload` blob,
  `arrived` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

