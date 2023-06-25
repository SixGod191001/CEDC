CREATE DEFINER=`fara`@`%` PROCEDURE `sp_rpt_data_ctl`(IN p_category_type VARCHAR(100))
BEGIN
  /*
  创建者：liangjian
  创建时间：20230619
  修改时间：
  修改内容：此存储过程是用来调用支持报表数据的SP的主要接口
  */
  
  -- 定义参数
  DECLARE v_batchno BIGINT;
  DECLARE v_category_type VARCHAR(100);
  DECLARE v_seq_nm INT;
  DECLARE v_sp_name VARCHAR(100);
  DECLARE v_process_message VARCHAR(200);
  DECLARE v_error_message VARCHAR(500);
  DECLARE v_sub_sp VARCHAR(100);
  DECLARE v_sp_sql VARCHAR(100);
  DECLARE v_start_timestampt TIMESTAMP;
  DECLARE v_end_timestampt TIMESTAMP;
  DECLARE v_status VARCHAR(100);
  DECLARE i INT ; 

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
			GET DIAGNOSTICS CONDITION 1
			v_error_message = MESSAGE_TEXT; 

  END;  

			
  SET v_seq_nm = (SELECT MAX(sequence_number) FROM rpt_main_ctl);
  SET v_sp_name = 'sp_rpt_data_ctl';
  SET v_start_timestampt = CURRENT_TIMESTAMP;
  SET v_category_type = p_category_type;
  SET v_batchno = FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP), '%Y%m%d%H%i%s');

  SET v_process_message = 'start running';
  SET i = 1;
	
  INSERT INTO proccess_log(category_type, sp_name, batchno, start_timestampt, message)
  VALUES(v_category_type, v_sp_name, v_batchno, v_start_timestampt, v_process_message);
  

  

	
  WHILE i <= v_seq_nm DO    
    SELECT sp_name INTO v_sub_sp
    FROM rpt_main_ctl 
    WHERE active_flag=1 
    AND category_type=v_category_type 
    AND sequence_number=i;
        
    SET @v_sp_sql =  CONCAT('CALL ', v_sub_sp, '(', v_batchno, ')');
		
    PREPARE stmt FROM @v_sp_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
        
    UPDATE rpt_main_ctl SET active_flag=0 WHERE category_type=v_category_type AND sp_name=v_sub_sp;
        
    COMMIT;
    SET i = i + 1; 
  END WHILE;

  UPDATE rpt_main_ctl SET active_flag=1 WHERE category_type=v_category_type; 
  COMMIT;
  
 UPDATE proccess_log SET end_timestampt=CURRENT_TIMESTAMP, status='Successful', message=CONCAT(v_sp_name, ' have completed') WHERE sp_name=v_sp_name; 
  COMMIT;

IF LENGTH(v_error_message)>0 THEN 
    SET v_status='Failed';
    INSERT INTO error_log(category_type, sp_name, batchno, start_timestampt, end_timestampt, status, error_message) 
    VALUES(v_category_type, v_sp_name, v_batchno, v_start_timestampt, CURRENT_TIMESTAMP, v_status, v_error_message); 
END IF;		

END