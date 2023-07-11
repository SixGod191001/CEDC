CREATE TABLE proccess_log (
  category_type varchar(100) COLLATE utf8_bin DEFAULT NULL,
  sp_name varchar(100) COLLATE utf8_bin DEFAULT NULL,
  batchno bigint(20) DEFAULT NULL,
  start_timestampt timestamp NULL DEFAULT NULL,
  end_timestampt timestamp NULL DEFAULT NULL,
  status varchar(100) COLLATE utf8_bin DEFAULT NULL,
  message varchar(1000) COLLATE utf8_bin DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;