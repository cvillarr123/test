from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, LongType

 

# Create a Spark session
spark = SparkSession.builder.appName("JavaUDFExample").config("spark.jars", "/home/hadoopadmin/test/test.jar").getOrCreate()
sc=spark.sparkContext
#spark.sparkContext.addJar("/home/hadoopadmin/test/test.jar")
spark.udf.registerJavaFunction("numAdd", "com.test.oneid.AddNumber", LongType())
spark.udf.registerJavaFunction("numMultiply", "com.test.oneid.MultiplyNumber", LongType())
import json
j = {'num1':2, 'num2':3}
a=[json.dumps(j)]
jsonRDD = sc.parallelize(a)
df = spark.read.json(jsonRDD)
df.registerTempTable("numbersdata")
df1=spark.sql("SELECT  numAdd(num2) AS num2 , numMultiply(num1) as num1 from numbersdata")
df1.show(10)
