# Databricks notebook source
# MAGIC %sh
# MAGIC ls /dbfs/FileStore/bronze

# COMMAND ----------

import subprocess
x=subprocess.run(["ls", "/dbfs/FileStore/bronze"], text=True)

# COMMAND ----------

x

# COMMAND ----------

x.result()

# COMMAND ----------


