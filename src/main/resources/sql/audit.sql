CREATE VIEW audit_train AS
SELECT  cast(getLineObject(content,1,',') as int)  AS age -- int
       ,getLineObject(content,2,',')  AS workclass -- string
       ,cast(getLineObject(content,3,',') as int)  AS fnlwgt -- bigint
       ,getLineObject(content,4,',')  AS education -- string
       ,cast(getLineObject(content,5,',') as int) AS education_num -- int
       ,getLineObject(content,6,',')  AS marital_status -- string
       ,getLineObject(content,7,',')  AS occupation -- string
       ,getLineObject(content,8,',')  AS relationship -- string
       ,getLineObject(content,9,',')  AS race -- string
       ,getLineObject(content,10,',') AS sex -- string
       ,cast(getLineObject(content,11,',') as int) AS capital_gain -- bigint
       ,cast(getLineObject(content,12,',') as int) AS capital_loss -- bigint
       ,cast(getLineObject(content,13,',') as int) AS hours_per_week -- bigint
       ,getLineObject(content,14,',') AS native_country -- string
       ,getLineObject(content,15,',') AS label -- string
FROM all_table;
