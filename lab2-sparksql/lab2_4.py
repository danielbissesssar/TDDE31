from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="assignment_4")
sqlsc = SQLContext(sc)

tempfile = sc.textFile("BDA/input/temperature-readings.csv")
percfile = sc.textFile("BDA/input/precipitation-readings.csv")

tempparts = tempfile.map(lambda l: l.split(";"))
percparts = percfile.map(lambda l: l.split(";"))

tempReadings = tempparts.map(lambda p: Row(station=p[0], temp=float(p[3])))
percReadings = percparts.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], day=p[1].split("-")[2], perc=float(p[3])))
tempschema = sqlsc.createDataFrame(tempReadings)
percschema = sqlsc.createDataFrame(percReadings)
tempschema.registerTempTable("tempschema")
percschema.registerTempTable("percschema")

temp_group = tempschema.groupBy('station').agg(F.max('temp').alias('Max_Temp'))
temp_filter = temp_group.filter((temp_group.Max_Temp >= 25) & (temp_group.Max_Temp <= 30))

perc_group = percschema.groupBy('station','year','month','day').agg(F.sum('perc').alias('Sum_Perc'))

station_group = perc_group.groupBy('station').agg(F.max('Sum_Perc').alias('Max_Perc'))
perc_filter = station_group.filter((station_group.Max_Perc <= 200) & (station_group.Max_Perc >= 100))
join = perc_filter.join(temp_filter, ['station'], 'inner').select('station','Max_Perc','Max_Temp').orderBy('station', ascending = False).show()
