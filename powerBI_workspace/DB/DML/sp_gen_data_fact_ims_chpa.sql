CREATE PROCEDURE sp_gen_data_fact_ims_chpa(IN cnt INT)
BEGIN
DECLARE i INT DEFAULT 1;
DECLARE V_YM VARCHAR(10);
DECLARE V_MKT VARCHAR(100);
DECLARE V_BRD VARCHAR(100);
DECLARE V_FOCUSTCOMPEGRP VARCHAR(100);
DECLARE V_PCKAGE VARCHAR(100);
DECLARE V_PACK_SIZE VARCHAR(100);
DECLARE V_Amount INT;
DECLARE V_QTY  INT;

DELETE FROM fact_ims_chpa;
WHILE i <= cnt DO
	
	SET V_YM=(SELECT YM FROM dim_calendar ORDER BY RAND() LIMIT 1);
	SET V_MKT= (SELECT CASE FLOOR(RAND() * 15)
                 WHEN 0 THEN 'Market research'
                 WHEN 1 THEN 'Advertising'
                 WHEN 2 THEN 'Product launch'
                 WHEN 3 THEN 'Public relations'
                 WHEN 4 THEN 'Digital marketing'
                 WHEN 5 THEN 'Content marketing'
                 WHEN 6 THEN 'Social media marketing'
                 WHEN 7 THEN 'Search engine optimization (SEO)'
                 WHEN 8 THEN 'Pay-per-click (PPC) advertising'
                 WHEN 9 THEN 'Customer acquisition cost (CAC)'
                 WHEN 10 THEN 'Return on investment (ROI)'
                 WHEN 11 THEN 'Conversion rate'
                 WHEN 12 THEN 'Lead generation'
                 WHEN 13 THEN 'Email marketing'
            ELSE 'Commodity market'
            END);
	SET V_BRD=(SELECT BrandCode FROM dim_brand ORDER BY RAND() LIMIT 1);
	SET V_FOCUSTCOMPEGRP=(SELECT CASE FLOOR(RAND() * 6)
                 WHEN 0 THEN 'Market share and growth rate'
                 WHEN 1 THEN 'Customer satisfaction'
                 WHEN 2 THEN 'New product development'
                 WHEN 3 THEN 'Marketing activities'
                 WHEN 4 THEN 'Healthcare policies'
                 WHEN 5 THEN 'Cost and benefit analysis'
                 ELSE 'Others'
                 END AS FocusCompeGrp);
SET V_PCKAGE=(SELECT CASE FLOOR(RAND() * 6)
                 WHEN 0 THEN 'Bottles'
                 WHEN 1 THEN 'Blister packs'
                 WHEN 2 THEN 'Creams'
                 WHEN 3 THEN 'Injection'
                 WHEN 4 THEN 'Sachets'
                 ELSE 'Others'
                 END);
	 SET V_PACK_SIZE=(SELECT CASE FLOOR(RAND() * 5)
                 WHEN 0 THEN 'Individual packaging'
                 WHEN 1 THEN 'Bottled packaging'
                 WHEN 2 THEN 'Container packaging'
                 WHEN 3 THEN 'Packaging specification'
                 ELSE 'Others'
                 END);
	SET V_Amount=FLOOR(RAND()*1000 +1000);
    SET V_QTY =FLOOR(RAND()*100 +100);									 
    INSERT INTO `fact_ims_chpa` (`YM`, `Defined_MKT`, `BrandCode`, `Product`, `FocusCompeGrp`, `Molecule`,`Corp`, `Manufactory`, `Flag_Aspen`, `IMSCHPA_Amount`, `IMSCHPA_QTY`, `PACKAGE`, `PACK_SIZE`, `NFCI_Code`, `NFCI_Description`,`etl_timestamp`)
    
	
	VALUES (V_YM, 
	        V_MKT,
			V_BRD,
			    CONCAT('Product ', i), 
			    V_FOCUSTCOMPEGRP, 
			    CONCAT('Molecule', i), 
				CONCAT('Corp', i), 
			    CONCAT('Manufacturer ', i%3 + 1), 
			    IF(i%2=0, 'Y', 'N'), 
			    V_Amount, 
			    V_QTY, 
			    V_PCKAGE, 				
			    V_PACK_SIZE, 
			    CONCAT('NFCI_Code', i),
				CONCAT('NFCI_Description', i),
				CURRENT_TIMESTAMP
				 );
    
     SET i = i + 1;
END WHILE;

	COMMIT;
END