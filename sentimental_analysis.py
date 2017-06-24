import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'

from pyspark import SparkConf, SparkContext

from pyspark.streaming import StreamingContext

from pyspark.ml.feature import Tokenizer

from pyspark.ml.feature import StopWordsRemover

from pyspark.ml.feature import Word2Vec

from pyspark.ml.classification import LogisticRegression

from pyspark.streaming.kafka import KafkaUtils

from kafka import SimpleProducer, KafkaClient

from kafka import KafkaProducer

import sys

import csv


from pyspark.sql import Row, SparkSession

try:

    import json

except ImportError:

    import simplejson as json

def getSparkSessionInstance(sparkConf):

    if ('sparkSessionSingletonInstance' not in globals()):

        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()

    return globals()['sparkSessionSingletonInstance']

def sentiment_train():

    rdd = spark_context.textFile("/user/SentimentalData/Subset100k.csv")
    # Remove Header
    header = rdd.first();
    rdd = rdd.filter(lambda row: row != header)

    spark = getSparkSessionInstance(rdd.context.getConf())
    
    r = rdd.mapPartitions(lambda x : csv.reader(x))

    parts = r.map(lambda x : Row(sentence=str.strip(x[3]), label=int(x[1])))

    partsDF = spark.createDataFrame(parts)

    partsDF.show(truncate=False)

    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

    tokenized = tokenizer.transform(partsDF)

    tokenized.show(truncate=False)  

    remover = StopWordsRemover(inputCol="words", outputCol="base_words")

    base_words = remover.transform(tokenized)

    base_words.show(truncate=False)

    train_data_raw = base_words.select("base_words", "label")

    train_data_raw.show(truncate=False)

    base_words = train_data_raw.select("base_words")

    base_words.show(truncate=False)

    base_words_rdd = base_words.rdd

    base_words_map = base_words_rdd.flatMap(lambda x: x[0])

    base_word_M = base_words_map.map(lambda x : (x,1))

    base_word_R = base_word_M.reduceByKey(lambda a,b : a + b)
    
    #Vectorize

    word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features")

    model = word2Vec.fit(train_data_raw)

    final_train_data = model.transform(train_data_raw)

    final_train_data.show()

    final_train_data = final_train_data.select("label", "features")

    final_train_data.show(truncate=False)

    #Logistic Regression

    lr = LogisticRegression(maxIter=1000, regParam=0.001, elasticNetParam=0.0001)

    lrModel = lr.fit(final_train_data)

    lrModel.transform(final_train_data).show()

    return lrModel

    #read_tweets()

    def sentiment_validate(lrModel):
    rdd = spark_context.textFile("/user/SentimentalData/Subset100k.csv")

    header = rdd.first();
    rdd = rdd.filter(lambda row: row != header)

    spark = getSparkSessionInstance(rdd.context.getConf())
    
    r = rdd.mapPartitions(lambda x : csv.reader(x))

    parts = r.map(lambda x : Row(sentence=str.strip(x[3]), label=int(x[1])))

    partsDF = spark.createDataFrame(parts)
    
    partsDF.show(truncate=False)
    
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

    tokenized = tokenizer.transform(partsDF)

    tokenized.show(truncate=False)

    remover = StopWordsRemover(inputCol="words", outputCol="base_words")

    base_words = remover.transform(tokenized)

    base_words.show(truncate=False)

    train_data_raw = base_words.select("base_words", "label")

    train_data_raw.show(truncate=False)

    base_words = train_data_raw.select("base_words")

    base_words.show(truncate=False)

    word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features")

    model = word2Vec.fit(train_data_raw)

    final_train_data2 = model.transform(train_data_raw)

    final_train_data2.show()
    
     final_train_data2 = final_train_data2.select("label", "features")

    final_train_data2.show(truncate=False)

    lrModel.transform(final_train_data2).show()

    return lrModel

    def read_tweets():
    
    context = StreamingContext(spark_context, 60)

    dStream = KafkaUtils.createDirectStream(context, ["twitter"], {"metadata.broker.list": "localhost:9092"})

    dStream.foreachRDD(p1)

    context.start()

    context.awaitTermination()

    def p1(time,rdd):

    rdd=rdd.map(lambda x: json.loads(x[1]))
    records=rdd.collect()
    
    records = [element["text"] for element in records if "text" in element]
    
    if not records:
        print("Empty List")
    else:
        print("Non-empty list")
        rdd = spark_context.parallelize(records)
        spark = getSparkSessionInstance(rdd.context.getConf())
        rdd = rdd.map(lambda x: x.upper())
        rdd_comey = rdd.filter(lambda x: "COMEY" in x).map(lambda x: [x, "COMEY"]) #(lambda x: x == "COMEY" or x == "maga" or x == "dictator" or x == "impeach" or x == "drain" or x == "swamp")
        rdd_maga = rdd.filter(lambda x: "MAGA" in x).map(lambda x: [x, "MAGA"])
        rdd_dictator = rdd.filter(lambda x: "DICTATOR" in x).map(lambda x: [x, "DICTATOR"])
        rdd_impeach = rdd.filter(lambda x: "IMPEACH" in x).map(lambda x: [x, "IMPEACH"])
        rdd_drain = rdd.filter(lambda x: "DRAIN" in x).map(lambda x: [x, "DRAIN"])
        rdd_swamp = rdd.filter(lambda x: "SWAMP" in x).map(lambda x: [x, "SWAMP"])

        rdd_final = rdd_comey.union(rdd_maga).union(rdd_dictator).union(rdd_impeach).union(rdd_drain).union(rdd_swamp)

        parts = rdd_final.map(lambda x : Row(sentence=str.strip(x[0]), label=x[1], time_stamp=time))

        partsDF = spark.createDataFrame(parts)

        partsDF.show(truncate=False)
    
        tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

        tokenized = tokenizer.transform(partsDF)

        tokenized.show(truncate=False)

        remover = StopWordsRemover(inputCol="words", outputCol="base_words")
        
        base_words = remover.transform(tokenized)

        base_words.show(truncate=False)

        train_data_raw = base_words.select("base_words", "label", "time_stamp")

        train_data_raw.show(truncate=False)

        base_words = train_data_raw.select("base_words")

        base_words.show(truncate=False)

        word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features")

        model = word2Vec.fit(train_data_raw)

        final_train_data3 = model.transform(train_data_raw)

        final_train_data3.show()

        final_train_data3 = final_train_data3.select("label", "features", "time_stamp")

        final_train_data3.show(truncate=False)

        tweetsDataFrame = global_lrModel.transform(final_train_data3)

        tweetsDataFrame.createOrReplaceTempView("ml_keywords")
        tweetsDataFrame = spark.sql("select label, prediction, count(*) as total, time_stamp from ml_keywords group by label, prediction, time_stamp order by total")
        tweetsDataFrame.write.mode("append").saveAsTable("ml_keywords_table")
if __name__ == "__main__":

spark_context = SparkContext(appName="Sentiment")

global_lrModel = sentiment_train()
read_tweets()

       
