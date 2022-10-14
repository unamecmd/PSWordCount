from pyspark.sql import SparkSession
from operator import add
import re

appName = "PySparkWordCount"
master = "local"

# Create Spark Session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

data_file_path = 'C:/Users/Bhanderi/PycharmProjects/PSWordCount/samplefile.txt'

lines = spark.read.text(data_file_path).rdd.map(lambda x: x[0])

#lines = spark.read.text("C:/Users/Bhanderi/PycharmProjects/PSWordCount/samplefile.txt").rdd.map(lambda x: x[0])
counts = lines.flatMap(lambda x: x.split(' ')) \
                  .filter(lambda x: re.sub('[^a-zA-Z]+', '', x)) \
                  .filter(lambda x: len(x)>1 ) \
                  .map(lambda x: x.upper()) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add) \
                  .sortByKey()

output = counts.collect()
for (word, count) in output:
  print("%s = %i" % (word, count))

spark.stop()