from pyspark import SparkContext
sc = SparkContext(appName = "assignment5")

stations_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
percip_file = sc.textFile("BDA/input/precipitation-readings.csv")

lines_stations = stations_file.map(lambda line: line.split(";"))
lines_percip = percip_file.map(lambda line: line.split(";"))
stations = lines_stations.map(lambda x: (int(x[0]))).collect()

stationlist = sc.broadcast(stations)
percip = lines_percip.map(lambda x: ((x[0],x[1][0:7]), float(x[3])))
percip = percip.filter(lambda x: int(x[0][0]) in stationlist.value)
percip = percip.filter(lambda x: int(x[0][1][0:4])>=1993 and int(x[0][1][0:4])<=2016)
percip = percip.reduceByKey(lambda a,b: a+b)
percip = percip.map(lambda x: (x[0][1], (x[1],1)))

percip = percip.reduceByKey(lambda v1, v2: (v1[0]+v2[0],v1[1]+v2[1]))
percip_avrg = percip.map(lambda x: (x[0], x[1][0]/x[1][1]))


percip_avrg.saveAsTextFile("BDA/output/percip_avrg")

