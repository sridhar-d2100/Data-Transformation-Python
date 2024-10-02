
#there are more api added on top of spark .we are utilsing the required ones based on the requirment 

# gernally dataframe preferred over rdd because ,more in-build methods ,catalyst optimiser is present ,support ml ,spark sql,

#read a csv ,json ,or connect with jdbc connection to read the inputs 
# Reading a CSV file
from pyspark import SparkContext, SparkConf
from pyspark.sql import Window
from pyspark.sql.functions import rank, col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


conf = SparkConf().setAppName("AppName").setMaster("local")
conf = SparkConf().setAppName("AppName").setMaster("yarn").set("spark.submit.deployMode", "cluster")
conf = SparkConf().setAppName("AppName").setMaster("yarn").set("spark.submit.deployMode", "client")
conf = SparkConf().setAppName("AppName").setMaster("spark://master_host:7077")


sc = SparkContext(conf=conf)

#set master can be local or yarn ,i
csvDF = spark.read.csv("path/to/csv", header=True, inferSchema=True)

# Reading a JSON file
jsonDF = spark.read.json("path/to/json")

# Writing a DataFrame to Parquet
df.write.parquet("output_path")

# Writing a DataFrame to a CSV file
df.write.csv("output_path", header=True)

# Reading from JDBC
jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/db").option("dbtable", "table").option("user", "root").option("password", "password").load()

# Writing to JDBC
df.write.format("jdbc").option("url", "jdbc:mysql://localhost/db").option("dbtable", "table").option("user", "root").option("password", "password").save()




# handling skew data 
# Repartition data based on specific columns (e.g., department)
repartitionedDF = df.repartition(10, "department")
df.repartition(num_partitions)
df.cache()

# Writing DataFrame to a bucketed table
df.write.bucketBy(10, "department").sortBy("salary").saveAsTable("bucketed_table")
df.write.partitionBy("date_column").format("parquet").saveAsTable("table_name")


# Coalescing reduces the number of partitions
coalescedDF = df.coalesce(5)




# Create a window specification
windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())

# Apply the window function (rank)
rankedDF = df.withColumn("rank", rank().over(windowSpec))

# Show ranked DataFrame
rankedDF.show()


# Create or replace a temp view
df.createOrReplaceTempView("people")

# Query using Spark SQL
results = spark.sql("SELECT * FROM people WHERE age > 20")


# List tables in Spark session catalog
tables = spark.catalog.listTables()

# Cache a table
spark.catalog.cacheTable("people")

# Uncache a table
spark.catalog.uncacheTable("people")




# mainly the udf ,it serizals the code ,and worker has the code ,so whatever comes to excutor ,it excutes the code 
def squared(x):
    return x * x

# Register it as a UDF
squared_udf = udf(squared, IntegerType())

# Apply the UDF to a DataFrame column
df_with_squares = df.withColumn("squared_column", squared_udf(df["original_column"]))

#map, filter, or User-Defined Functions (UDFs), 
df_with_squares.show()


