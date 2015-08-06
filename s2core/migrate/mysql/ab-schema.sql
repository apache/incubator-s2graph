use graph_dev;


-- ----------------------------
--  Table structure for `experiments`
-- ----------------------------
DROP TABLE IF EXISTS `experiments`;
CREATE TABLE `experiments` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `service_name` varchar(128) NOT NULL,
  `title` varchar(64) NOT NULL,
  `experiment_key` varchar(128) NOT NULL,
  `description` varchar(255) NOT NULL,
  `experiment_type` varchar(64) NOT NULL DEFAULT 'user_id_modular',
  `total_modular` int NOT NULL DEFAULT 100,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_service_name_experiment_key` (`service_name`, `experiment_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ----------------------------
--  Table structure for `buckets`
-- ----------------------------
DROP TABLE IF EXISTS `buckets`;
CREATE TABLE `buckets` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `experiment_id` integer NOT NULL,
  `uuid_mods` varchar(64) NOT NULL,
  `traffic_ratios` varchar(64) NOT NULL,
  `http_verb` varchar(8) NOT NULL,
  `api_path` text NOT NULL,
  `uuid_key` varchar(128) NOT NULL,
  `uuid_placeholder` varchar(64) NOT NULL,
  `request_body` text NOT NULL,
  `timeout` int NOT NULL DEFAULT 1000,
  `impression_id` varchar(128) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_impression_id` (`impression_id`),
  INDEX `idx_experiment_id` (`experiment_id`),
  INDEX `idx_impression_id` (`impression_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
