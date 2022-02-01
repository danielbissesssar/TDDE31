from pyspark import SparkContext
import numpy as np
sc = SparkContext(appName = "exam")
data = sc.textFile("data.csv")
data = data.map(lambda line: line.split(";"))
#x[0] is assumed to be x, x[1] is assumed to be y
#cache since data will be accessed several times
data = data.map(lambda x: x[0], x[1]).cache()
#assume h is given
h = 1
#our kernel function as given in exam part 1
def density(X,x,h):
    return kernel((X-x)/h)
#I assume f(X = x|Y = y) is calculated as f(X = x)*f(Y = y)
#p(Y = 1) is computed by total number of nonzeros divided by total number of elements
#Since we only have to states for Y, p(Y = 0) = 1 - p(Y = 1)
#Then f(X = x), f(Y = 0) and f(Y = 1) are computed with kernel methods
#because of previous assumptions they are then combined with bayes theorem to return the probability
def predictory0(data, x, h):
    fXx = data.map(lambda x: 1, (density(x[0],x,h), 1))
    fXx = fXx.reduceByKey(lambda a,b: a[0]+b[0], a[1]+b[1])
    fXx = fXx.map(lambda x: x[0]/x[1]).collect()
    rawdatay = data.map(lambda x: x[1]).collect()
    py1 = np.count_nonzero(rawdatay)/len(rawdatay)
    py0 = 1-py1
    y0 = data.map(lambda x: 1, (density(x[1],0,h), 1))
    y0 = y0.reduceByKey(lambda a,b: a[0]+b[0], a[1]+b[1])
    y0 = y0.map(lambda x: x[0]/x[1]).collect()
    y1 = data.map(lambda x: 1, (density(x[1],1,h), 1))
    y1 = y1.reduceByKey(lambda a,b: a[0]+b[0], a[1]+b[1])
    y1 = y1.map(lambda x: x[0]/x[1]).collect()
    return(fXx*y0*py0/(fXx*y0*py0+fXx*y1*py1))
prediction = predictory0(data, 1, h)
print("The prediction is p(Y = 0|X = 1) = " + str(prediction))
