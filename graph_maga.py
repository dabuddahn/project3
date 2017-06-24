from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import sys
import numpy as np

def sparksession(sparkConf):
    if ('sparksessioninit' not in globals()):
       globals()['sparksessioninit']=SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparksessioninit']

def graph():
    spark = sparksession(spark_context.getConf())
    time, date = sys.argv[1:]
    interval_time = ("%s %s" %(date, time))
    query = "select prediction, sum(total) as global_total from ml_keywords_table \
             where label = 'MAGA' group by prediction"
    data = spark.sql(query)
    data_format = data.toPandas()

    fig1, ax1 = plt.subplots()

    ax1.pie(data_format['global_total'], labels=data_format['prediction'], autopct='%1.1f%%', shadow=True, startangle=135)

    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    plt.show()

if __name__ == "__main__":

    print("Stating to plot graph") 

    spark_context = SparkContext(appName="Graph of problem a")

    graph()
