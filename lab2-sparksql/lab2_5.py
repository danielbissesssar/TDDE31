from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="assignment_5")
sqlsc = SQLContext(sc)

stationfile = sc.textFile("BDA/input/stations-Ostergotland.csv")
percfile = sc.textFile("BDA/input/precipitation-readings.csv")

stationparts = stationfile.map(lambda l: l.split(";"))
percparts = percfile.map(lambda l: l.split(";"))

stationReadings = stationparts.map(lambda p: Row(station=p[0]))
percReadings = percparts.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], day=p[1].split("-")[2], perc=float(p[3])))
stationschema = sqlsc.createDataFrame(stationReadings)
percschema = sqlsc.createDataFrame(percReadings)
stationschema.registerTempTable("stationschema")
percschema.registerTempTable("percschema")

perc_filter = percschema.filter((percschema.year >= 1993) & (percschema.year <= 2016))
joined = stationschema.join(perc_filter, ['station'], 'inner')
summed = joined.groupBy('station','year','month').agg(F.sum('perc').alias('Sum_Month'))
summed_final = summed.groupBy('year','month').avg('Sum_Month')
final = summed_final.orderBy(['year','month'], ascending=False).show()
