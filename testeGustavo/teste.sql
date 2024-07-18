SELECT
	primeiro_dia
	,ultimo_dia
	,[JAN],[FEV],[MAR],[ABR],[JUN],[JUL],[AGO],[SET],[OUT],[NOV],[DEZ]
	FROM(
		SELECT
			A.data AS first_day
			,A.data AS primeiro_dia
			,EOMONTH(A.data) AS ultimo_dia
			,A.nome_mes AS month_name
		FROM [db_syn_sc_ondemand].[CURATED].[param_calendar] AS A
		WHERE RIGHT(A.data,2) = 01
	) AS P
	PIVOT(
		COUNT(first_day)
		FOR month_name IN ([JAN],[FEV],[MAR],[ABR],[JUN],[JUL],[AGO],[SET],[OUT],[NOV],[DEZ])
) AS PVT
