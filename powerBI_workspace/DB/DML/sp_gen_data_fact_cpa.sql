CREATE PROCEDURE sp_gen_data_fact_cpa(IN cnt INT)
BEGIN
DECLARE i INT DEFAULT 1;
DECLARE V_YM VARCHAR(10);
DECLARE V_MKT VARCHAR(100);
DECLARE V_BRD VARCHAR(100);
DECLARE V_INSID VARCHAR(100);
DECLARE V_FOCUSTCOMPEGRP VARCHAR(100);
DECLARE V_CITYCODE VARCHAR(100);
DECLARE V_DROGFORM VARCHAR(100);
DECLARE V_ROUTE VARCHAR(100);
DECLARE V_Amount INT;
DECLARE V_QTY  INT;
DECLARE V_ProvinceCode VARCHAR(100);

DELETE FROM fact_cpa;
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
	SET V_INSID=(SELECT InsID FROM dim_party ORDER BY RAND() LIMIT 1);
	SET V_FOCUSTCOMPEGRP=(SELECT CASE FLOOR(RAND() * 6)
                 WHEN 0 THEN 'Market share and growth rate'
                 WHEN 1 THEN 'Customer satisfaction'
                 WHEN 2 THEN 'New product development'
                 WHEN 3 THEN 'Marketing activities'
                 WHEN 4 THEN 'Healthcare policies'
                 WHEN 5 THEN 'Cost and benefit analysis'
                 ELSE 'Others'
                 END AS FocusCompeGrp);
SELECT CityCode,ProvinceCode INTO V_CITYCODE,V_ProvinceCode FROM dim_report_line ORDER BY RAND() LIMIT 1;
SET V_DROGFORM=(SELECT CASE FLOOR(RAND() * 10)
                 WHEN 0 THEN 'Tablet'
                 WHEN 1 THEN 'Capsule'
                 WHEN 2 THEN 'Injection'
                 WHEN 3 THEN 'Inhaler'
                 WHEN 4 THEN 'Topical cream'
                 WHEN 5 THEN 'Transdermal patch'
                 WHEN 6 THEN 'Ointment'
                 WHEN 7 THEN 'Syrup'
                 WHEN 8 THEN 'Drops'
                 WHEN 9 THEN 'Suppository'
                 ELSE 'Others'
                 END);
	 SET V_ROUTE=(SELECT CASE FLOOR(RAND() * 10)
                 WHEN 0 THEN 'Oral'
                 WHEN 1 THEN 'Intravenous'
                 WHEN 2 THEN 'Intramuscular'
                 WHEN 3 THEN 'Subcutaneous'
                 WHEN 4 THEN 'Transdermal'
                 WHEN 5 THEN 'Ophthalmic'
                 WHEN 6 THEN 'Inhalation'
                 WHEN 7 THEN 'Intradermal'
                 WHEN 8 THEN 'Intravaginal'
                 WHEN 9 THEN 'Rectal'
                 ELSE 'Others'
                 END);
	SET V_Amount=FLOOR(RAND()*1000 +1000);
    SET V_QTY =FLOOR(RAND()*100 +100);								 
    INSERT INTO `fact_cpa` (`YM`, `Defined_MKT`, `BrandCode`, `InsID`, `Product`, `FocusCompeGrp`, `Molecule`, `Manufactory`, `Flag_Aspen`, `Flag_TargetHP`, `Amount`, `QTY`, `CityCode`, `DrugForm`, `Route`,`ProvinceCode`,`etl_timestamp`)
    
	VALUES (V_YM, 
	        V_MKT,
			    V_BRD,
        V_INSID,
			    CONCAT('Product ', i), 
			    V_FOCUSTCOMPEGRP, 
			    CONCAT('Molecule', i), 
			    CONCAT('Manufacturer ', i%3 + 1), 
			    IF(i%2=0, 'Y', 'N'), 
			    IF(i%2=0, 'N', 'Y'),
			    V_Amount, 
			    V_QTY, 
			    V_CITYCODE, 				
			    V_DROGFORM, 
			    V_ROUTE,
					V_ProvinceCode,
					CURRENT_TIMESTAMP
				 );
    
     SET i = i + 1;
END WHILE;

	COMMIT;
END