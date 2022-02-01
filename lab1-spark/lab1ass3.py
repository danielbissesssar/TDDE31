from pyspark import SparkContext
sc2 = SparkContext(appName = "assignment2")
temp_file = sc2.textFile("BDA/input/temperature-readings.csv")
lines = temp_file.map(lambda line: line.split(";"))
year_temp = lines.map(lambda x: ((x[1][0:10],x[0]), float(x[3])))
year_temp = year_temp.filter(lambda x: int(x[0][0][0:4])>=1960 and int(x[0][0][0:4])<=2014)
max_temp = year_temp.reduceByKey(lambda a,b: a if a>=b else b)
min_temp = year_temp.reduceByKey(lambda a,b: a if a<=b else b)
maxmin_temp = max_temp.join(min_temp)
maxmin_temp_avrg = maxmin_temp.map(lambda x: ((x[0][0][0:7],x[0][1]), (x[1][0]+x[1][1]/2,1)))
maxmin_temp_grouped = maxmin_temp_avrg.reduceByKey(lambda v1, v2: (v1[0]+v2[0],v1[1]+v2[1]))
maxmin_avrg =maxmin_temp_grouped.map(lambda x: (x[0], x[1][0]/x[1][1]))
maxmin_avrg.saveAsTextFile("BDA/output/month_avrg")


