from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# dataframe
names = spark.read.schema(schema).option("sep", " ").csv("/Users/hkumar2/Downloads/SparkCourse/Marvel+Names")

# dataframe
lines = spark.read.text("/Users/hkumar2/Downloads/SparkCourse/Marvel+Graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
leastPopular = connections.sort(func.col("connections")).first()

min_connection_count = leastPopular[1]

leastPopularName = names.filter(func.col("id") == leastPopular[0]).select("name").first()

print(leastPopularName[0] + " is the least popular superhero with " + str(leastPopular[1]) + " co-appearances.")

# least popular superheroes dataframe
least_popular = connections.filter(func.col("connections") == min_connection_count)

# joining the least popular dataframe with the names dataframe to get the names of the superheroes
# that are least popular

least_popular_names = least_popular.join(names, least_popular.id == names.id).select('name')

least_popular_names.show()

