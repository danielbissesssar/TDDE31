from pyspark import SparkContext

#assignment 1
sc = SparkContext(appName = "assignment1")
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temp_file.map(lambda line: line.split(";"))
year_temp = lines.map(lambda x: (x[1][0:4], float(x[3])))
year_temp = year_temp.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)
max_temp = year_temp.reduceByKey(lambda a,b: a if a>=b else b)
min_temp = year_temp.reduceByKey(lambda a,b: a if a<=b else b)
max_temp_sorted = max_temp.sortBy(ascending = False, keyfunc = lambda k: k[1])
min_temp_sorted = min_temp.sortBy(ascending = True, keyfunc = lambda k: k[1])
max_temp_sorted.saveAsTextFile("BDA/output/max_temperature")
min_temp_sorted.saveAsTextFile("BDA/output/min_temperature")