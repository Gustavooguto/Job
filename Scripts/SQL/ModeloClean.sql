WITH SELECIONADOR_SYS1 AS(
	SELECT
		SYS1.cpf_cnpj
		,SYS1.origin
		,(CASE
			WHEN SYS1.deleted_id = 'S' THEN
				SYS1.address + 0
			ELSE
				SYS1.address + 10000000
		END) AS new_address
		,SYS1.id_source_system
		,SYS1.pipeline_triggertime AS pipeline_triggertime
	FROM STD.std_clientes AS SYS1
	INNER JOIN
	(
	SELECT
		SYS1.[cpf_cnpj]
		,MAX(CASE
			WHEN SYS1.deleted_id = 'S' THEN
				SYS1.address + 0
			ELSE
				SYS1.address + 100
		END) AS new_address
		,MAX(SYS1.[pipeline_triggertime]) AS pipeline_triggertime
    
	FROM STD.std_clientes AS SYS1
	WHERE SYS1.origin = 'some_api'
	GROUP BY SYS1.[cpf_cnpj]
	) SYS2
	ON SYS1.cpf_cnpj = SYS12.cpf_cnpj and (CASE
			WHEN SYS1.deleted_id = 'S' THEN
				SYS1.address + 0
			ELSE
				SYS1.address + 100
		END)=SYS1.new_address
		AND SYS1.pipeline_triggertime = SYS1.pipeline_triggertime
	WHERE SYS1.origin = 'some_api'
)
, SELECIONADOR_SYS2 AS (
	SELECT
		SYS2.[cpf_cnpj]
		,SYS2.origin
		,0 AS new_address
		,SYS2.id_source_system
		--,MAX(SYS2.id_source_system) AS id_source_system
		,MAX(SYS2.pipeline_triggertime) AS pipeline_triggertime
	FROM STD.std_clientes AS SYS2
	WHERE SYS2.origin = 'SYS2' 
	GROUP BY SYS2.[cpf_cnpj]
		,SYS2.origin
		,SYS2.id_source_system
)
,SELECIONADOR_SYS3_1 AS (
	SELECT
		SYS3.[cpf_cnpj]
		,SYS3.origin
		,0 AS new_address
		,SYS3.id_source_system
		,MAX(SYS3.pipeline_triggertime) AS pipeline_triggertime
	FROM STD.std_clientes AS SYS3
	WHERE SYS3.origin = 'SYS3_1'
	GROUP BY SYS3.[cpf_cnpj]
		,SYS3.origin
		,SYS3.id_source_system

)
,SELECIONADOR_SYS3_2 AS (
	SELECT
		SYS3.[cpf_cnpj]
		,SYS3.origin
		,0 AS new_address
		,SYS3.id_source_system
		,MAX(SYS3.pipeline_triggertime) AS pipeline_triggertime
	FROM STD.std_clientes AS SYS3
	WHERE SYS3.origin = 'SYS3_2'
	GROUP BY SYS3.[cpf_cnpj]
		,SYS3.origin
		,SYS3.id_source_system

)
,SELECIONADOR_SYS3_3 AS (
	SELECT
		SYS3.[cpf_cnpj]
		,SYS3.origin
		,0 AS new_address
		,SYS3.id_source_system
		,MAX(SYS3.pipeline_triggertime) AS pipeline_triggertime
	FROM STD.std_clientes AS SYS3
	WHERE SYS3.origin = 'SYS3_3'
	GROUP BY SYS3.[cpf_cnpj]
		,SYS3.origin
		,SYS3.id_source_system
)
,SELECIONADOR_SYS3_4 AS (
	SELECT
		SYS3.[cpf_cnpj]
		,SYS3.origin
		,0 AS new_address
		,SYS3.id_source_system
		,MAX(SYS3.pipeline_triggertime) AS pipeline_triggertime
	FROM STD.std_clientes AS SYS3
	WHERE SYS3.origin = 'SYS3_4'
	GROUP BY SYS3.[cpf_cnpj]
		,SYS3.id_source_system
		,SYS3.origin
)
,SELECIONADOR AS (
	SELECT
		SELECIONADOS.cpf_cnpj
		--,SELECIONADOS.origin
		,MAX(SELECIONADOS.new_address) AS new_address
		--,SELECIONADOS.id_source_systemm
		,MAX(SELECIONADOS.pipeline_triggertime) AS pipeline_triggertime
		,MIN(PRD.prioridade) AS prioridade
	FROM ( 
		SELECT *
		FROM SELECIONADOR_SYS1
		UNION ALL
		SELECT *
		FROM SELECIONADOR_SYS2
		UNION ALL
		SELECT *
		FROM SELECIONADOR_SYS3_1
		UNION ALL
		SELECT *
		FROM SELECIONADOR_SYS3_2
		UNION ALL
		SELECT *
		FROM SELECIONADOR_SYS3_3
		UNION ALL
		SELECT *
		FROM SELECIONADOR_SYS3_4
	) AS SELECIONADOS
	LEFT JOIN OPENROWSET(
	BULK 'https://blobstorage.dfs.core.windows.net/container/raw/parameters/OrdemPrioridade.parquet',
    FORMAT = 'PARQUET'
	) AS PRD
	ON SELECIONADOS.origin = PRD.origem_cliente
	GROUP BY 
		SELECIONADOS.cpf_cnpj
)
,TUDO AS (
	SELECT
		STD1.*
		,(CASE
			WHEN STD1.deleted_id = 'S' THEN
				STD1.address + 0
			WHEN STD1.deleted_id = 'N' THEN
				STD1.address + 100
			ELSE
				0
		END) AS new_address
		,PRD.prioridade
	FROM STD.std_clientes AS STD1
	LEFT JOIN OPENROWSET(
	BULK 'https://blobstorage.dfs.core.windows.net/container/raw/parameters/OrdemPrioridade.parquet',
    FORMAT = 'PARQUET'
	) AS PRD
	ON STD1.origin = PRD.origem_cliente
)
SELECT
	T.cpf_cnpj
	,T.person_type
	,T.id_source_system
	,T.customer_name
	,T.fantasy_name
	,T.mnemonico_customer
	,T.customer_address_code
	,T.address_state
	,T.address_city_name
	,T.address_neighborhood
	,T.address_zip
	,T.address
	,T.address_compl
	,T.country
	,T.customer_carrier_code
	,T.customer_group_mnemonico
	,T.state_registration
	,T.municipal_registration
	,T.credit_limit
	,T.address
	,T.previous_balance
	,T.current_balance
	,T.customer_credit_situation
	,T.customer_type
	,T.email_billing
	,T.date_created
	,T.customer_group
	,T.economic_sector_ibope
	,T.dynamic_special_accounts_flag
	,T.dynamic_sale_type_classification
	,T.dynamic_executive
	,T.origin
	,T.deleted_id
	,T.hash
FROM TUDO AS T
INNER JOIN SELECIONADOR AS S
	ON T.cpf_cnpj = S.cpf_cnpj
	AND T.new_address = S.new_address
	AND T.prioridade = S.prioridade