## Issues Faced & Learnings:

1. Cannot bulk load because the file "Global_Electronics_Retailer\Products.csv" could not be opened. Operating system error code 32(The process cannot access the file because it is being used by another process.)

2. Cannot obtain the required interface ("IID_IColumnsInfo") from OLE DB provider "BULK" for linked server "(null)" --- Due to FIELDTERMINATOR & ROWTERMINATOR 

3. Cannot bulk load because the maximum number of errors (10) was exceeded --- Bulk Insert failed (Due to Price in Decimal type with $)

4. Products - Prices column with '$', change schema to NVARCHAR for bronze layer.

    ```alter table bronze.erp_Products alter column erp_UnitCostUSD NVARCHAR(15);
    alter table bronze.erp_Products alter column erp_UnitPriceUSD NVARCHAR(15);```

5. Cannot bulk load because the file "Products" could not be opened. Operating system error code 80(The file exists.) --- ERRORFILE file can't be overwritten

6. Use KEEPNULLS parameter in BULK INSERT statement in order to hold Null values. Example: [19,Germany,Berlin,1295,] with missing last field will be skipped by SQL Server enginer during the Bulk Insert unless KEEPNULLS is used.

7. TRUNCATE & INSERT to be performed rather than DELETE(row-wise operation --- slow). But, TRUNCATE doesn't work if there exists foreign Key constraints

8. Bulk load data conversion error (type mismatch or invalid character for the specified codepage) for row 6498, column 5 (cst_StateCode) --- Due to field length issue

9. API creation on "render.com" - Used FastAPI in Python, Github to create an API with the Subcategories data. It takes few seconds to invoke the api as it will be automatically in sleep mode due to inactivity.

10. pandas.errors.IntCastingNaNError: Cannot convert non-finite values (NA or inf) to integer --- Use error = 'coerce' to update them as Null

11. SQL - Windows Authentication - [ODBC Driver 18 for SQL Server]SSL Provider: The certificate chain was issued by an authority that is not trusted - Use this connection string:     

    ```connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;'```

12. Error connecting to SQL Server: 42000; - Due to "syntax error or access violation", Check the type mismacth between dataframe and SQL table field types

13. Use padding with Zero (0) when it requires string based comparison --- String comparison occurs alphabetically. Example, sales_data['OrderNumber'].astype(str).str.zfill(7)

14. Update multiple fields in a record of the table - Use Comma (,)

15. Pattern to identify only the Non-standard characetrs ([field_name] like '%[^a-zA-Z0-9 !"%#$&’’()*+,-./:;<=>?@]%')

16. String or binary data would be truncated in table {Insufficient size for the field specified during the creation of table}

17. Collate to identify the accent characters in the table - CAST(cst_State AS VARCHAR(100)) COLLATE SQL_Latin1_General_CP1253_CI_AI

18. SQL Server does not allow variable concatenation directly in FROM. Need to use a variable to store the value and use the variable.

19. sp_executesql() - For running commands that don’t allow variables directly (e.g., BULK INSERT, SELECT INTO, dynamic table names)

20. We cannot use CREATE VIEW immediately upon a DROP View statement. Another workaround is to use:  EXEC sp_executesql N'CREATE VIEW .....'

21. Windows Authentication to SQL Server doesn't work in Docker containers. connection string with credentials.

22. Airflow current version requires ```pyhton version < 3.12``` [This can be done by creating a venv in project folder with python version < 3.12]

23. Airflow with Docker installation and yaml file configuration

24. Xcom is useful for Cross communication between tasks for sharing the data. But when Airflow tries to push a Pandas DataFrame to XCom, PyArrow fails to serialize it and raises an error. PyArrow requires strict data validations. So, convert the df to a json or csv before passing the file location though Xcom to other task.

25. %40 is the URL-encoded form of @

26. AIRFLOW_CONN_MSSQL_DEFAULT=mssql+pyodbc://airflow_user:pwd@host/db?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes

27. Use Task groups to combine related taks with a single name

28. When we change a dag_id, multiple dags will be populated in the Airflow UI. Use "docker compose restart" to refresh the componenets and dags with updated dag id.

29. BULK INSERT does not support inserting into a different database than the one you're connected to when called via sp_executesql in dynamic SQL. [When executed dynamically, SQL Server evaluates BULK INSERT in the context of the current database session].