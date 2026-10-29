from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max

spark = SparkSession.builder.appName("ActorDirectorDF").getOrCreate()

df = spark.read.csv("infoActors.csv", header=True, inferSchema=True, escape='\\')

df = df.repartition(8, "actor", "director")

grouped = df.groupBy("actorName", "directorName") \
    .agg(
        count("*").alias("count"),
        avg("averageRating").alias("avgRating"),
        min("averageRating").alias("minRating"),
        max("averageRating").alias("maxRating")
    )

filtered = grouped.filter(col("count") >= 2)

ordered = filtered.orderBy(col("count").desc())

# Mostrar top 20
top20 = ordered.select("actorName", "directorName", "count", "avgRating", "minRating", "maxRating").limit(20)
top20.show(truncate=False)

# Cerrar sesión
spark.stop()
