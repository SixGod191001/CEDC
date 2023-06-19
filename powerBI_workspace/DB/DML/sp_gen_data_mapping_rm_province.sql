CREATE PROCEDURE sp_gen_data_mapping_rm_province(IN cnt INT)
BEGIN
DECLARE i INT DEFAULT 1;

DECLARE V_CITY VARCHAR(100);
DECLARE V_CITYCODE VARCHAR(100);
DECLARE V_PROVINCE VARCHAR(100);
DECLARE V_PROVINCECODE VARCHAR(100);
DECLARE V_RSM_CODE VARCHAR(100);
DECLARE V_REGION VARCHAR(100);
DECLARE V_BU VARCHAR(100);

DELETE FROM mapping_rm_province;
WHILE i <= cnt DO

SELECT CityCode,City,ProvinceCode,Province INTO V_CITYCODE,V_CITY,V_PROVINCECODE,V_PROVINCE FROM dim_geography ORDER BY RAND() LIMIT 1;
SELECT RSM_CODE,Region INTO V_RSM_CODE,V_REGION FROM dim_org ORDER BY RAND() LIMIT 1;

SET V_BU= (SELECT CASE FLOOR(RAND() * 15)
WHEN 0 THEN 'Cardiology'
WHEN 1 THEN 'Oncology'
WHEN 2 THEN 'Neurology'
WHEN 3 THEN 'Gastroenterology'
WHEN 4 THEN 'Immunology'
WHEN 5 THEN 'Infectious diseases'
WHEN 6 THEN 'Endocrinology'
WHEN 7 THEN 'Hematology'
WHEN 8 THEN 'Rheumatology'
WHEN 9 THEN 'Dermatology'
ELSE 'Others'
END);

INSERT INTO mapping_rm_province (ProvinceCode,Province,City_Code,City,rsm_TrtyCode,region,bu)
VALUES (V_PROVINCECODE,
V_PROVINCE,
V_CITYCODE,
V_CITY,
V_RSM_CODE,
V_REGION,
V_BU
);

SET i = i + 1;
END WHILE;

COMMIT;
END