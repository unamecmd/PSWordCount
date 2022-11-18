from google.cloud import bigquery

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

appName = "PySparkBigQuery"
master = "local"
bucket = "pnkj_t1_bucket/temp"
client = bigquery.Client()

# Create Spark Session #    .config('spark.jars','guava-11.0.1.jar,gcsio-1.9.0-javadoc.jar') \
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .config('spark.jars.packages','com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.5,com.google.guava:guava:r05') \
    .config('temporaryGcsBucket',bucket) \
    .config('spark.jars','guava-11.0.1.jar,gcsio-1.9.0-javadoc.jar') \
    .getOrCreate()




employeesDF = spark.read.format("bigquery").option('project','pnkj-t1').option("table","pnkj-t1.assign3_dataset.emplyees_table").load()
employeesDF.cache()
employeesDF.show()
employeesDF.printSchema()


departmentsDF = spark.read.format("bigquery").option('project','pnkj-t1').option("table","pnkj-t1.assign3_dataset.departments_table").load()
departmentsDF.show()
departmentsDF.printSchema()


employeesDF.createOrReplaceTempView("employees")
departmentsDF.createOrReplaceTempView("departments")

##join DF usinf spark.sql
#result1DF = spark.sql("Select DEPARTMENT_NAME, SUM(SALARY) FROM employees, departments WHERE employees.DEPARTMENT_ID == departments.DEPARTMENT_ID GROUP BY DEPARTMENT_NAME")

### join dataframes
#result0DF=employeesDF.join(departmentsDF, employeesDF.DEPARTMENT_ID == departmentsDF.DEPARTMENT_ID)
result0DF=employeesDF.join(departmentsDF, on="DEPARTMENT_ID")
print("Tables Join dataframe")
result0DF.show()

# Saving the data to BigQuery
#result0DF.write.format('bigquery').option('table', 'pnkj-t1.assign3_dataset.joined_table').mode('overwrite').save()

result1DF=employeesDF.join(departmentsDF, on="DEPARTMENT_ID").groupby("DEPARTMENT_ID").agg(sum("SALARY").alias("TOTAL_SALARY")).filter("DEPARTMENT_ID == '20'")
print("TOTAL SALARY FOR DEPARTMENT = 20")
result1DF.show()

# Saving the data to BigQuery
result1DF.write.format('bigquery').option('table', 'pnkj-t1.assign3_dataset.task1_out_table').mode('overwrite').save()

result2DF=employeesDF.join(departmentsDF, on="DEPARTMENT_ID").groupby("DEPARTMENT_ID").agg(sum("SALARY").alias("TOTAL_SALARY")).orderBy("TOTAL_SALARY")
print("TOTAL SALARY FOR DEPARTMENT ASEC SORT BY TOTAL SALARY")
result2DF.show()

# Saving the data to BigQuery
result2DF.write.format('bigquery').option('table', 'pnkj-t1.assign3_dataset.task2_out_table').mode('overwrite').save()


dataset_ref = bigquery.DatasetReference('pnkj-t1', 'assign3_dataset')
table_ref = dataset_ref.table('task2_out_table')
bucket_name = 'pnkj_t1_bucket/output/Assignment3_Output/'

destination_uri = "gs://{}/{}".format(bucket_name, "task2_output.csv")



#Exporting BigQuery Table in GCS Bucket
extract_job = client.extract_table('pnkj-t1.assign3_dataset.task2_out_table', destination_uri , location="us-central1")
extract_job.result()



