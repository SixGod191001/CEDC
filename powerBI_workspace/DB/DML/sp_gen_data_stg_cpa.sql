CREATE PROCEDURE sp_gen_data_stg_cpa(IN cnt INT)
BEGIN
DECLARE i INT DEFAULT 1;
DECLARE V_YM VARCHAR(10);
DECLARE V_Year VARCHAR(10);
DECLARE V_Quarter VARCHAR(10);
DECLARE V_MKT VARCHAR(100);
DECLARE V_BRD VARCHAR(100);
DECLARE V_INSID VARCHAR(100);
DECLARE V_FOCUSTCOMPEGRP VARCHAR(100);
DECLARE V_CITY VARCHAR(100);
DECLARE V_PROVINCE VARCHAR(100);
DECLARE V_DROGFORM VARCHAR(100);
DECLARE V_ROUTE VARCHAR(100);
DECLARE V_UNITS INT;
DECLARE V_VALUE INT;
DECLARE V_QUANTITY INT;
DECLARE V_REGION VARCHAR(100);
DECLARE V_HPCODE VARCHAR(100);
DECLARE V_PCKAGE VARCHAR(100);

DELETE FROM stg_cpa;
WHILE i <= cnt DO

SELECT YM,SUBSTR(Quarter,2,1),Year INTO V_YM,V_Quarter,V_Year FROM dim_calendar ORDER BY RAND() LIMIT 1;
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
SET V_BRD=(SELECT BrandCode FROM dim_product ORDER BY RAND() LIMIT 1);
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
SELECT City,Province INTO V_CITY,V_PROVINCE FROM dim_report_line ORDER BY RAND() LIMIT 1;
SET V_REGION=(SELECT Region FROM  dim_org  ORDER BY RAND() LIMIT 1);
SET V_HPCODE=(SELECT InsID_Aspen FROM dim_party  ORDER BY RAND() LIMIT 1);
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
SET V_PCKAGE=(SELECT CASE FLOOR(RAND() * 6)
             WHEN 0 THEN 'Bottles'
             WHEN 1 THEN 'Blister packs'
             WHEN 2 THEN 'Creams'
             WHEN 3 THEN 'Injection'
             WHEN 4 THEN 'Sachets'
             ELSE 'Others'
             END);
SET V_UNITS=FLOOR(RAND()*10 +10);
SET V_VALUE=FLOOR(RAND()*1000 +1000);
SET V_QUANTITY =FLOOR(RAND()*100 +100);

INSERT INTO `stg_cpa` (`province`,`city`,`Region`,`year`,`quater`,`month`,`CPA_HP_Code`,`ATCCode`,`DrugName`,`ProductName`,`Package`,`Specification`,`Units`,`Value`,`Quantity`,`DrugForm`,`Route`,`companyname`,`batchno`)
VALUES (V_PROVINCE, 
        V_CITY,
		V_REGION,
        V_Year,
		V_Quarter,
		V_YM,
		V_HPCODE,
		CONCAT('ATCCode ', i), 
		CONCAT('DrugName ', i), 
		CONCAT('Product ', i), 
		V_PCKAGE,
		CONCAT('Specification', i), 
		V_UNITS,
		V_VALUE,
		V_QUANTITY,
		V_DROGFORM,
		V_ROUTE,
		CONCAT('companyname ', i), 
		i
        );

SET i = i + 1;
END WHILE;

COMMIT;
END