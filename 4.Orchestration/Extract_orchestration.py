# Databricks notebook source
# do the usual import packages
import json
import sys

#Run the notebook and get the path to the table
#fetch the return value from the callee 001_TrainModel

source="/Repos/ImmoDataCollection/immodatacollection_uat/"

#Shared/4.Orchestration/Extract_orchestration
inits=[source+'0.Global/instanciate_cluster', source+'1.Extract/CastorusMainTable']
for init in inits:
  extract_maintTable= dbutils.notebook.run(init, 3600)
if extract_maintTable == 'Success':
  extract_details_notebook=[source+'1.Extract/castorusChangeTable', 
                            source+'1.Extract/Extract_bienici_image', 
                            source+'1.Extract/Extract_figaro_image', 
                            source+'1.Extract/Extract_avendrealouer_image']
  for each in extract_details_notebook:
    extract_details= dbutils.notebook.run(each, 3600)
    
  if extract_details == 'Success':
    transform=[source+'2.Transform/main_table', source+'2.Transform/image_table']
    for transfo in transform:
      transform_mainTable = dbutils.notebook.run(transfo, 3600)
    if transform_mainTable == 'Success':
      metaTable=dbutils.notebook.run(source+'2.Transform/meta_table', 3600)
      if metaTable == 'Success':
        print("Job ran successfully:", sys.exc_info()[0])
      else:
        raise "Notebook failed!"
        
      
      
      
  
#if returned_json == 'Success':
#  returned_json = dbutils.notebook.run('/Users/steven.vincent@pwc.com/1.Extract Notebooks/Finnhub_extract', 3600)
#  if returned_json == 'Success':
#    try:
#      #Create a Stream from the table
#      #Fetch the return value from the callee 002_CreateStream
#      transform_notebook=['/Users/steven.vincent@pwc.com/2.Transform/TickerMetaData','/Users/steven.vincent@pwc.com/2.Transform/refinitivTable']
#      for transfo in transform_notebook:
#        returned_json = dbutils.notebook.run(transfo, 3600)
#        if returned_json == 'Success':
#          print("Job ran successfully:", sys.exc_info()[0])
#        else:
#          raise "TickerMetaData Notebook failed!"
#    except:
#      print("Unexpected error:", sys.exc_info()[0])
#      raise
#  else:
#    print("Something went wrong finnhub notebook " + returned_json['message'])
#else: 
#  print("Something went wrong extract notebook " + returned_json['message'])
