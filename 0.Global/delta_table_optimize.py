# Databricks notebook source
Databases=spark.sql("SHOW DATABASES")
for each in Databases.select('databaseName').collect():
    x=spark.sql(f"SHOW TABLES IN {each[0]}")
    for tb, db in zip(x.select('tableName').collect(), x.select('database').collect()):
        query=f"OPTIMIZE {db[0]}.{tb[0]}"
        t=spark.sql(query)
        print(query)
    
