import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, max as spark_max, sum as spark_sum
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


spark = SparkSession.builder.appName("EmployeesExercise").getOrCreate()