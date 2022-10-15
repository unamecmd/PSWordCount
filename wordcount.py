from pyspark.sql import SparkSession
from operator import add
import re
import datetime

appName = "PySparkWordCount"
master = "local"
now = datetime.datetime.now()
output_file_name = "output_"+(now.strftime("%Y_%m_%d_%H_%M_%S"))

# Create Spark Session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

bucket_dir= 'gs://pnkj_t1_bucket/'
output_dir = bucket_dir+'output/'
sampledata_dir = bucket_dir+'sampledata/'
outputfile_dir = output_dir+output_file_name
data_file_path = sampledata_dir+'samplefile.txt'

lines = spark.read.text(data_file_path).rdd.map(lambda x: x[0])

counts = lines.flatMap(lambda x: x.split(' ')) \
                  .filter(lambda x: re.sub('[^a-zA-Z]+', '', x)) \
                  .filter(lambda x: len(x)>1 ) \
                  .map(lambda x: x.upper()) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add) \
                  .sortByKey()

counts.saveAsTextFile(outputfile_dir)

output = counts.collect()
for (word, count) in output:
  print("%s = %i" % (word, count))

spark.stop()