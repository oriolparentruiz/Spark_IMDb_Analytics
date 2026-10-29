from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys

sc = SparkContext(appName="ActorDirectorRDD")
spark = SparkSession(sc)

infoActors = spark.read.option("delimiter", ',').option("header", True).csv('infoActors.csv').repartition(8, 'actor', 'director').rdd


pairs = infoActors.map(lambda row: (
    (row['actorName'], row['directorName']),
    (1, float(row['averageRating']), float(row['averageRating']), float(row['averageRating']))
))

def reduce_func(a, b):
    count = a[0] + b[0]
    sum_rating = a[1] + b[1]
    min_rating = min(a[2], b[2])
    max_rating = max(a[3], b[3])
    return (count, sum_rating, min_rating, max_rating)

reduced = pairs.reduceByKey(reduce_func)


filtered = reduced.filter(lambda x: x[1][0] >= 2)


result = filtered.map(lambda x: (
    x[0][0],  # actorName
    x[0][1],  # directorName
    x[1][0],  # count
    x[1][1] / x[1][0],  # avg
    x[1][2],  # min
    x[1][3]   # max
))

ordered = result.sortBy(lambda x: -x[2])

for item in ordered.take(20):
    print(item)

sc.stop()
