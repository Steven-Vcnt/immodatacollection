# Databricks notebook source
!pip install delta-sharing

# COMMAND ----------

import delta_sharing

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "<profile-file-path>"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
client.list_all_tables()

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#<share-name>.<schema-name>.<table-name>"

# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
delta_sharing.load_as_pandas(table_url)

# If the code is running with PySpark, you can use `load_as_spark` to load the table as a Spark DataFrame.
delta_sharing.load_as_spark(table_url)

# COMMAND ----------

import delta_sharing
profile_json_txt = '{\
  "shareCredentialsVersion": 1,\
  "endpoint": "http://localhost:81/delta-sharing/",\
  "bearerToken": ""\
}'
delta_sharing_prof = delta_sharing.protocol.DeltaSharingProfile.from_json(profile_json_txt)

# COMMAND ----------

client = delta_sharing.SharingClient(delta_sharing_prof)

# COMMAND ----------

client.list_all_tables()
