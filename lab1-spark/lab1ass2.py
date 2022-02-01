from pyspark import SparkContext

#assignment 2
sc2 = SparkContext(appName = "assignment2")
temp_file = sc2.textFile("BDA/input/temperature-readings.csv")
lines = temp_file.map(lambda line: line.split(";"))
year_temp = lines.map(lambda x: (x[1][0:7], float(x[3])))
year_temp = year_temp.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014)
month_filtered = year_temp.filter(lambda x: float(x[1])>=10)
month_count = month_filtered.groupByKey()
month_count = month_count.map(lambda x:(x[0],len(x[1])))
month_count.saveAsTextFile("BDA/output/month_count")

sc3 = SparkContext(appName = "assignment2.1")
temp_file = sc3.textFile("BDA/input/temperature-readings.csv")
lines = temp_file.map(lambda line: line.split(";"))
year_temp = lines.map(lambda x: (x[1][0:7], (x[0], float(x[3]))))
year_temp = year_temp.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014)
month_filtered = year_temp.filter(lambda x: float(x[1][1])>=10)
month_count = month_filtered.groupByKey()
month_count_distinct = month_count.distinct().map(lambda x:(x[0],len(x[1][1])))
month_count_distinct.saveAsTextFile("BDA/output/year_month_count_distinct")
