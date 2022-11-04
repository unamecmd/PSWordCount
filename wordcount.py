from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,explode,count
import datetime
import sys

if __name__ == "__main__":

    appName = "PySparkWordCount"
    master = "local"
    now = datetime.datetime.now()
    output_file_name = "output_" + (now.strftime("%Y_%m_%d_%H_%M_%S"))

    #Create Spark Session
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .getOrCreate()

    n = len(sys.argv)
    print("Number of received arguments : " + str(n))

    if (n != 3):
        print("Missing arguments")
    elif (n == 3):
        for i in range(1,n):
            print("Argument : " + str(i) + sys.argv[i])
        data_file_path = sys.argv[1]
        output_dir = sys.argv[2]

    outputfile_dir = output_dir + output_file_name

    #lines = spark.read.text(data_file_path).rdd.map(lambda x: x[0])
    dflines = spark.read.text(data_file_path)

    # dfwords = dflines.flatMap(lambda x: x.split(' ')) \
    #     .filter(lambda x: re.sub('[^a-zA-Z]+', '', x)) \
    #     .filter(lambda x: len(x) > 1) \
    #     .map(lambda x: x.upper()) \
    #     .map(lambda x: (x, 1)) \
    #     .reduceByKey(add) \
    #     .sortByKey()


    dfwords = dflines.withColumn('words', split(col('value'), ' ')) \
        .withColumn('word', explode(col('words'))) \
        .drop('value', 'words').groupBy('word').agg(count('word') \
                                                    .alias('count')).orderBy('count', ascending=False)

    output=dfwords.collect()
    for (word, count) in output:
        print("%s = %i" % (word, count))

    outDF = spark.createDataFrame(data=output, schema=["Word", "Count"])
    outDF.show()

    print("output dataframe length ")
    print((outDF.count(), len(outDF.columns)))

    outDF.write.csv(outputfile_dir)
    spark.stop()

