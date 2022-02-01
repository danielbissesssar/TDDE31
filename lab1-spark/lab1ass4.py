from pyspark import SparkContext

sc = SparkContext(appName = "ass4")
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
percip_file =sc.textFile("BDA/input/precipitation-readings.csv")

lines_temp = temp_file.map(lambda line: line.split(";"))
lines_percip = percip_file.map(lambda line: line.split(";"))
val_temp = lines_temp.map(lambda x: (x[0], float(x[3])))
val_percip = lines_percip.map(lambda x: ((x[0],x[1]), float(x[3])))
val_percip = val_percip.groupByKey()
val_percip = val_percip.map(lambda x: (x[0][0], sum(x[1])))
filtered_temp = val_temp.filter(lambda x: x[1]>=25 and x[1]<=30)
filtered_percip = val_percip.filter(lambda x: x[1]>=100 and x[1]<=200)
filtered_temp  = filtered_temp.reduceByKey(lambda a,b: a if a>=b else b)
filtered_percip = filtered_percip.reduceByKey(lambda a,b: a if a>=b else b)
final_stations = filtered_temp.join(filtered_percip)
final_stations.saveAsTextFile("BDA/output/final_stations")

