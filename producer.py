#!/usr/bin/env python
# coding: utf-8

# In[1]:

# Importing the libraries
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.context import SparkContext
from subprocess import call
from pyspark.sql.types import DoubleType
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import  avg
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


#Creating the spark session!

spark = SparkSession.builder.appName("yelp").getOrCreate()


# In[13]:



path = "gs://bdl2021_final_project/yelp_train.json" # path for the test data


#encoding schema for data
schema = StructType([StructField('business_id', StringType(), True),
                     StructField('cool', StringType(), True),
                     StructField('date', StringType(), True),
                    StructField('funny', StringType(), True),
                   StructField('review_id', StringType(), True),
                   StructField('stars', DoubleType(), True),
                   StructField('text', StringType(), True),
                   StructField('useful', StringType(), True)])


#streaming data in json format
yelpDF = spark.readStream.format("json").schema(schema).load(path)


# In[16]:


#finally write stream!
query = yelpDF.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value") .writeStream   .format("kafka")   .option("kafka.bootstrap.servers", " 10.180.0.2:9092")   .option("topic","yelp2")   .option("checkpointLocation", "gs://project-bucket-yelp/logs")   .start()


# In[ ]:
query.awaitTermination()




