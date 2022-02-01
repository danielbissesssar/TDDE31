from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="assignment_2")
sqlsc = SQLContext(sc)

textfile = sc.textFile("BDA/input/temperature-readings.csv")

parts = textfile.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1],value=float(p[3])))
schema = sqlsc.createDataFrame(tempReadings)
schema.registerTempTable("schema")

year_filter = schema.filter((schema.year >= 1950) & (schema.year <= 2014) & (schema.value > 10)).select('station','year','month').distinct()
num = year_filter.groupBy('year','month').agg(F.count('station').alias('count')).orderBy('count', ascending=False).show()
