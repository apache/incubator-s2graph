use graph_dev;

-- ----------------------------
--  Table structure for `services`
-- ----------------------------
DROP TABLE IF EXISTS `services`;
CREATE TABLE `services` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `service_name` varchar(64) NOT NULL,
  `cluster` varchar(255) NOT NULL,
  `hbase_table_name` varchar(255) NOT NULL,
  `pre_split_size` integer NOT NULL default 0,
  `hbase_table_ttl` integer,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_service_name` (`service_name`),
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
  `is_async` tinyint	NOT NULL DEFAULT 0,
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
  `id` integer NOT NULL AUTO_INCREMENT,
  `label_id` integer NOT NULL,
  `seq` tinyint	NOT NULL,
  `meta_seqs` varchar(64) NOT NULL,
  `formulars` varchar(255),
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_label_id_seq` (`label_id`, `meta_seqs`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE label_indices ADD FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE;

