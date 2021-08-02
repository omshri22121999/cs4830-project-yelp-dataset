
#importing the reuired libraries
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

from pyspark.sql.functions import explode, split
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.feature import VectorAssembler
from subprocess import call
from pyspark.sql.functions import *

import time


 # creating the spark session
spark = SparkSession.builder.appName("yelp").getOrCreate()
spark.sparkContext.setLogLevel("OFF")


#schema on data that is coming from producer
schema = StructType([StructField('business_id', StringType(), True),
                     StructField('cool', StringType(), True),
                     StructField('date', StringType(), True),
                    StructField('funny', StringType(), True),
                   StructField('review_id', StringType(), True),
                   StructField('stars', DoubleType(), True),
                   StructField('text', StringType(), True),
                   StructField('useful', StringType(), True)])

#read stream  consumer 
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.180.0.2:9092").option("subscribe", "yelp2").option("maxOffsetsPerTrigger",5000).load() 
    
# to cconvert the data coming json format to data frame
df = df.withColumn("temp", from_json(col("value").cast("string"), schema)).select(col('temp.text'),col('temp.stars'))


# prep processing before sending it to model as input

#stripping all the special characters
df = df.withColumn("text_raw", regexp_replace("text", "[^a-zA-Z0-9\s+\']", ""))
# splitting the text into words
df = df.withColumn("text_split", split(trim("text_raw"),"\s+"))



#loading the model
model = PipelineModel.load("gs://project-bucket-yelp/model_lr_20000.model")


#transforming!
df = model.transform(df)

def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
	print("Batch ",epoch_id)
	start_time = time.time()
	df.select("text","stars","prediction").show(10)
	evaluator = MulticlassClassificationEvaluator(labelCol="stars", predictionCol="prediction")
	f1 = evaluator.evaluate(df,{evaluator.metricName: "f1"})
	accuracy = evaluator.evaluate(df,{evaluator.metricName: "accuracy"})
	cnt =  df.count()
	time_taken = time.time() - start_time
	print("For Batch {} , count {} , latency {} s , f1 score is {} , accuracy is {}".format(epoch_id,cnt,time_taken,f1,accuracy))
	pass


#final write stream for displaying
q = df.writeStream.foreachBatch(foreach_batch_function).start()  


q.awaitTermination()



