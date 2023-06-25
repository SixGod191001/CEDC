CREATE TABLE error_log (
  category_type varchar(100) COLLATE utf8_bin DEFAULT NULL,
  sp_name varchar(100) COLLATE utf8_bin DEFAULT NULL,
  batchno bigint(20) DEFAULT NULL,
  start_timestampt timestamp NULL DEFAULT NULL,
  end_timestampt timestamp NULL DEFAULT NULL,
  status varchar(100) COLLATE utf8_bin DEFAULT NULL,
  error_message text COLLATE utf8_bin
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
