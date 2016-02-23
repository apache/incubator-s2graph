CREATE DATABASE IF NOT EXISTS graph_dev;

CREATE USER 'graph'@'%' IDENTIFIED BY 'graph';

GRANT ALL PRIVILEGES ON graph_dev.* TO 'graph'@'%' identified by 'graph';

flush privileges;

use graph_dev;


SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
--  Table structure for `services`
-- ----------------------------
DROP TABLE IF EXISTS `services`;
CREATE TABLE `services` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `service_name` varchar(64) NOT NULL,
  `access_token` varchar(64) NOT NULL,
  `cluster` varchar(255) NOT NULL,
  `hbase_table_name` varchar(255) NOT NULL,
  `pre_split_size` integer NOT NULL default 0,
  `hbase_table_ttl` integer,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_service_name` (`service_name`),
  INDEX `idx_access_token` (`access_token`),
  INDEX `idx_cluster` (cluster(75))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ----------------------------
--  Table structure for `services_columns`
-- ----------------------------
DROP TABLE IF EXISTS `service_columns`;
CREATE TABLE `service_columns` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `service_id` integer NOT NULL,
  `column_name` varchar(64) NOT NULL,
  `column_type` varchar(8) NOT NULL,
  `schema_version` varchar(8) NOT NULL default 'v2',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_service_id_column_name` (`service_id`, `column_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE service_columns add FOREIGN KEY(service_id) REFERENCES services(id) ON DELETE CASCADE;


-- ----------------------------
--  Table structure for `column_metas`
-- ----------------------------
DROP TABLE IF EXISTS `column_metas`;
CREATE TABLE `column_metas` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `column_id` integer NOT NULL,
  `name` varchar(64) NOT NULL,
  `seq` tinyint	NOT NULL,
  `data_type` varchar(8) NOT NULL DEFAULT 'string',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_column_id_name` (`column_id`, `name`),
  INDEX `idx_column_id_seq` (`column_id`, `seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE column_metas ADD FOREIGN KEY(column_id) REFERENCES service_columns(id) ON DELETE CASCADE;

-- ----------------------------
--  Table structure for `labels`
-- ----------------------------

DROP TABLE IF EXISTS `labels`;
CREATE TABLE `labels` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `label` varchar(64) NOT NULL,
  `src_service_id` integer NOT NULL,
  `src_column_name` varchar(64) NOT NULL,
  `src_column_type` varchar(8) NOT NULL,
  `tgt_service_id` integer NOT NULL,
  `tgt_column_name` varchar(64) NOT NULL,
  `tgt_column_type` varchar(8) NOT NULL,
  `is_directed` tinyint	NOT NULL DEFAULT 1,
  `service_name` varchar(64),
  `service_id` integer NOT NULL,
  `consistency_level` varchar(8) NOT NULL DEFAULT 'weak',
  `hbase_table_name` varchar(255) NOT NULL DEFAULT 's2graph',
  `hbase_table_ttl` integer,
  `schema_version` varchar(8) NOT NULL default 'v2',
  `is_async` tinyint(4) NOT NULL default '0',
  `compressionAlgorithm` varchar(64) NOT NULL DEFAULT 'lz4',
  `options` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_label` (`label`),
  INDEX `idx_src_column_name` (`src_column_name`),
  INDEX	`idx_tgt_column_name` (`tgt_column_name`),
  INDEX `idx_src_service_id` (`src_service_id`),
  INDEX `idx_tgt_service_id` (`tgt_service_id`),
  INDEX `idx_service_name` (`service_name`), 
  INDEX `idx_service_id` (`service_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE labels add FOREIGN KEY(service_id) REFERENCES services(id);



-- ----------------------------
--  Table structure for `label_metas`
-- ----------------------------
DROP TABLE IF EXISTS `label_metas`;
CREATE TABLE `label_metas` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `label_id` integer NOT NULL,
  `name` varchar(64) NOT NULL,
  `seq` tinyint	NOT NULL,
  `default_value` varchar(64) NOT NULL,
  `data_type` varchar(8) NOT NULL DEFAULT 'long',
  `used_in_index` tinyint	NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_label_id_name` (`label_id`, `name`),
  INDEX `idx_label_id_seq` (`label_id`, `seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE label_metas ADD FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE;


-- ----------------------------
--  Table structure for `label_indices`
-- ----------------------------
DROP TABLE IF EXISTS `label_indices`;
CREATE TABLE `label_indices` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `label_id` int(11) NOT NULL,
  `name` varchar(64) NOT NULL DEFAULT '_PK',
  `seq` tinyint(4) NOT NULL,
  `meta_seqs` varchar(64) NOT NULL,
  `formulars` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_label_id_seq` (`label_id`,`meta_seqs`),
  UNIQUE KEY `ux_label_id_name` (`label_id`,`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE label_indices ADD FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE;


-- ----------------------------
--  Table structure for `experiments`
-- ----------------------------
DROP TABLE IF EXISTS `experiments`;
CREATE TABLE `experiments` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `service_id` integer NOT NULL,
  `service_name` varchar(128) NOT NULL,
  `name` varchar(64) NOT NULL,
  `description` varchar(255) NOT NULL,
  `experiment_type` varchar(8) NOT NULL DEFAULT 'u',
  `total_modular` int NOT NULL DEFAULT 100,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_service_id_name` (`service_id`, `name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ALTER TABLE experiments ADD FOREIGN KEY(service_id) REFERENCES service(id) ON DELETE CASCADE;


-- ----------------------------
--  Table structure for `buckets`
-- ----------------------------
DROP TABLE IF EXISTS `buckets`;
CREATE TABLE `buckets` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `experiment_id` integer NOT NULL,
  `modular` varchar(64) NOT NULL,
  `http_verb` varchar(8) NOT NULL,
  `api_path` text NOT NULL,
  `uuid_key` varchar(128),
  `uuid_placeholder` varchar(64),
  `request_body` text NOT NULL,
  `timeout` int NOT NULL DEFAULT 1000,
  `impression_id` varchar(64) NOT NULL,
  `is_graph_query` tinyint NOT NULL DEFAULT 1,
  `is_empty` tinyint NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_impression_id` (`impression_id`),
  INDEX `idx_experiment_id` (`experiment_id`),
  INDEX `idx_impression_id` (`impression_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;


-- ----------------------------
--  Table structure for `counter`
-- ----------------------------
DROP TABLE IF EXISTS `counter`;
CREATE TABLE `counter` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `use_flag` tinyint(1) NOT NULL DEFAULT '0',
  `version` smallint(1) NOT NULL DEFAULT '1',
  `service` varchar(64) NOT NULL DEFAULT '',
  `action` varchar(64) NOT NULL DEFAULT '',
  `item_type` int(11) NOT NULL DEFAULT '0',
  `auto_comb` tinyint(1) NOT NULL DEFAULT '1',
  `dimension` varchar(1024) NOT NULL,
  `use_profile` tinyint(1) NOT NULL DEFAULT '0',
  `bucket_imp_id` varchar(64) DEFAULT NULL,
  `use_exact` tinyint(1) NOT NULL DEFAULT '1',
  `use_rank` tinyint(1) NOT NULL DEFAULT '1',
  `ttl` int(11) NOT NULL DEFAULT '172800',
  `daily_ttl` int(11) DEFAULT NULL,
  `hbase_table` varchar(1024) DEFAULT NULL,
  `interval_unit` varchar(1024) DEFAULT NULL,
  `rate_action_id` int(11) unsigned DEFAULT NULL,
  `rate_base_id` int(11) unsigned DEFAULT NULL,
  `rate_threshold` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `svc` (`service`,`action`),
  KEY `rate_action_id` (`rate_action_id`),
  KEY `rate_base_id` (`rate_base_id`),
  CONSTRAINT `rate_action_id` FOREIGN KEY (`rate_action_id`) REFERENCES `counter` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `rate_base_id` FOREIGN KEY (`rate_base_id`) REFERENCES `counter` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
