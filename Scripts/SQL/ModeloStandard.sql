WITH SYS AS(
	SELECT 
		CAST(CAST(REPLACE(REPLACE(REPLACE(RAW.[CNPJ],'-',''),'/',''),'.','') AS BIGINT) AS VARCHAR) AS cpf_cnpj
		,MAX(RAW.Cod) AS id_source_system
	FROM
    OPENROWSET(
        BULK 'https://blobstorage.dfs.core.windows.net/container/raw/system/table/Year=2022/Month=07/Day=13/raw_system_table_origin.parquet',
        FORMAT = 'PARQUET'
    ) AS RAW
	WHERE RAW.Type = 0
	GROUP BY RAW.CNPJ
),
TRANSFORMADO AS (
	SELECT
		CAST(CAST(REPLACE(REPLACE(REPLACE([SYS].[CNPJ],'-',''),'/',''),'.','') AS BIGINT) AS NVARCHAR) AS [cpf_cnpj],
		REPLACE(REPLACE(REPLACE(REPLACE([SYS].[TYPE],'1','pf'),'0','pj'),'2','pj'),'5','outro') AS [type],
		[SYS].[cod] AS [id_source_system],
		[SYS].[trade_name] COLLATE SQL_Latin1_General_CP1251_CS_AS AS [customer_name],
		[SYS].[legal_name] COLLATE SQL_Latin1_General_CP1251_CS_AS AS [fantasy_name],
		CAST(NULL AS NVARCHAR) AS [mnemonico_customer],
		CAST(NULL AS NVARCHAR) AS [customer_address_code],
		[CITY].[state_acronym] AS [address_state],
		UPPER([CITY].[city_name]) COLLATE SQL_Latin1_General_CP1251_CS_AS AS [address_city_name],
		CAST(NULL AS NVARCHAR) AS [address_neighborhood], 
		REPLACE(REPLACE(REPLACE([SYS].[address_zip],'-',''),'/',''),'.','') AS [address_zip],
		CONCAT([SYS].[address],[SYS].[address_number]) AS [address],
		[SYS].[address_compl] AS [address_compl],
		CAST(NULL AS NVARCHAR) AS [country],
		CAST(NULL AS NVARCHAR) AS [customer_carrier_code],
		CAST(NULL AS NVARCHAR) AS [customer_group_mnemonico],
		REPLACE(REPLACE([SYS].[id2],'-',''),'.','') AS [state_registration],
		REPLACE(REPLACE([SYS].[id3],'-',''),'.','') AS [municipal_registration],
		[SYS].[credit_limit] AS [credit_limit],
		NULL AS [ADDRESS],
		CAST(NULL AS NVARCHAR) AS [previous_balance],
		CAST(NULL AS NVARCHAR) AS [current_balance],
		CAST(NULL AS NVARCHAR) AS [customer_credit_situation],
		CAST(NULL AS NVARCHAR) AS [customer_type],
		[SYS].[e_mail_2] AS [email_billing],
		[SYS].[date_created] AS [date_created],
		CAST(NULL AS NVARCHAR) AS [customer_group],
		CAST(NULL AS NVARCHAR) AS [economic_sector_ibope],
		CAST(NULL AS NVARCHAR) AS [dynamic_special_accounts_flag],
		CAST(NULL AS NVARCHAR) AS [dynamic_sale_type_classification],
		CAST(NULL AS NVARCHAR) AS [dynamic_executive],
		[SYS].[origem] AS [origin],
		CAST(NULL AS NVARCHAR) AS [id_record_deleted],
		HASHBYTES('MD5', CONCAT([CNPJ], [TYPE], [cod], [trade_name], [legal_name],
	    		  NULL, NULL, [state_acronym], [city_name], [address_zip], CONCAT([address],[address_number]),
				  [address_compl], NULL, NULL, NULL, [id2], [id3], [credit_limit], NULL, NULL, NULL, NULL, NULL, 
				  [e_mail_2], [date_created], [SYS].[origem], NULL)) AS hash
	FROM
		OPENROWSET(
        BULK 'https://blobstorage.dfs.core.windows.net/container/raw/system/table/Year=2022/Month=07/Day=13/raw_system_table_origin.parquet',
			FORMAT = 'PARQUET'
		) AS SYS
	LEFT JOIN OPENROWSET(
			BULK 'https://blobstorage.dfs.core.windows.net/container/raw/system/table/Year=2022/Month=07/Day=13/raw_system_table_origin.parquet',
			FORMAT = 'PARQUET'
	) AS CITY
		ON SYS.Cod_City = CITY.Cod_City
)
SELECT 
	T.*
FROM TRANSFORMADO AS T
INNER JOIN PULSAR_CNPJ_UNICO AS P_CNPJ_UNICO
	ON T.cpf_cnpj = P_CNPJ_UNICO.cpf_cnpj
	AND T.id_source_system = P_CNPJ_UNICO.id_source_system