from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="assignment_1")
sqlsc = SQLContext(sc)

textfile = sc.textFile("BDA/input/temperature-readings.csv")

parts = textfile.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(year=p[1].split("-")[0],value=float(p[3])))
schema = sqlsc.createDataFrame(tempReadings)
schema.registerTempTable("schema")

year_temp = schema.filter((schema.year>=1950)&(schema.year<=2014))
max_temp = year_temp.groupBy('year').agg(F.max('value').alias('Max')).orderBy('Max', ascending=False).show()
min_temp = year_temp.groupBy('year').agg(F.min('value').alias('Min')).orderBy('Min', ascending=False).show()
