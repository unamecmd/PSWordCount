from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,explode,count
import datetime
import sys

if __name__ == "__main__":

    appName = "PySparkWordCount"
    master = "local"
    now = datetime.datetime.now()
    task1_output_file_name = "task1_output_" + (now.strftime("%Y_%m_%d_%H_%M_%S"))
    task2_output_file_name = "task2_output_" + (now.strftime("%Y_%m_%d_%H_%M_%S"))

    #Create Spark Session
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .getOrCreate()

    n = len(sys.argv)
    print("Number of received arguments : " + str(n))

    if (n != 5):
        print("Missing arguments")
    elif (n == 5):
        for i in range(1,n):
            print("Argument : " + str(i) + sys.argv[i])
        employees_data_file_path = sys.argv[1]
        departments_data_file_path = sys.argv[2]
        task1_output_dir = sys.argv[3]
        task2_output_dir = sys.argv[4]

    task1_outputfile_dir = task1_output_dir + task1_output_file_name
    task2_outputfile_dir = task2_output_dir + task2_output_file_name

    employeesDF = spark.read.csv(employees_data_file_path, inferSchema=True,header=True)
    # print("Employee database schema")
    # employeesDF.printSchema()
    employeesDF.show()

    departmentsDF = spark.read.csv(departments_data_file_path, inferSchema=True,header=True)
    departmentsDF.show()
    # print("Department database schema")
    # departmentsDF.printSchema()

    departmentsDF.createOrReplaceTempView("department")
    # sql1DF = spark.sql("SELECT * FROM department")
    # sql1DF.show()

    employeesDF.createOrReplaceTempView("employees")
    # sql2DF = spark.sql("SELECT * FROM employees")
    # sql2DF.show()

    resultDF = spark.sql("Select ")
    #Total salary for dept # 20
    #sql3DF = spark.sql("SELECT DEPARTMENT_ID,SUM(SALARY) AS TOTAL_SALARY FROM employees WHERE DEPARTMENT_ID='20'")
    sql3DF = spark.sql("SELECT SUM(SALARY) AS TOTAL_SALARY FROM employees WHERE DEPARTMENT_ID='20'")
    print("Total salary for dept # 20")
    sql3DF.show()



    #List of Dept, Total salary for that dept in ascending order of total salary
    sql4DF = spark.sql("SELECT DEPARTMENT_ID,SUM(SALARY) AS TOTAL_SALARY from employees GROUP BY DEPARTMENT_ID ORDER BY SUM(SALARY)")
    print("#List of Dept, Total salary for that dept in ascending order of total salary")
    sql4DF.show()

    pre_t2DF = employeesDF.groupBy("DEPARTMENT_ID").sum("SALARY")
    t2DF = pre_t2DF.sort("SALARY")
    print("Task 2 by other way")
    t2DF.show()


    sql3DF.write.csv(task1_outputfile_dir)
    sql4DF.write.csv(task2_outputfile_dir)

    spark.stop()

