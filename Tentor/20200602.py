from pyspark import SparkContext
sc = SparkContext(appName = "exam")
data = sc.textFile("data.csv").cache()

w[0] = 0
w[1] = 0
e[0] = 1
e[1] = 1
#alpha given
alpha = 1
### x[0] = y, x[1] = pair of x-values, assume data to be our data points

def classifier(x, w):
    val = x[0]*w[0]+x[1]*w[0]
    returnval = 1
    if val < 0:
        returnval = -1
    return returnval
while abs(e[0])+abs(e[1])>1:
    data = data.map(lambda x: x[0], x[1], classifier(x[1],w))
    data = data.map(lambda x: x[0], x[1], x[0]!=x[2])
    e = data.map(lambda x: x[0]*x[1]*x[2]).reduce(sum)

    w[0] = w[0] + alpha*e[0]
    w[1] = w[0] + alpha*e[1]