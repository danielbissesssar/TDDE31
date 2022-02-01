########################### mapper
import sys
for line in sys.stdin:
    floats = line.split()
    for float in floats:
        print(float**2)


############################# combiner
import sys
for line in sys.stdin:
    floats = line.split()
    sumfloat = 0
    for float in floats:
        sumfloat = sumfloat + float
    print(sumfloat)

############################# reducer
import sys
import math
for line in sys.stdin:
    floats = line.split()
    sumfloat = 0
    for float in floats:
        sumfloat = sumfloat + float
    print(math.sqrt(sumfloat))

############################# question 9
import random
#k amount of clusters, K amount of random data points
K = 200
k = 4
KPoints = data.takeSample(False, K, 1)
#map with k different keys
KPoints = KPoints.map(lambda x:(random.randint(1,k),(x,1)))
#sum all k clusters
KPoints = KPoints.reduceByKey(lambda a, b: a[1]+b[1],a[2]+b[2])
#take average for each summed cluster
KPoints = KPoints.map(lambda x: x[1]/x[2])

########################## one booty boi




import numpy as np
iterations = 1000
def gradient(matrix, w):
    Y = matrix[:, 0]
    X = matrix[:, 1:]
    return ((1.0 / (1.0 + np.exp(-Y * X.dot(w))) - 1.0) * Y * X.T).sum(1)

def add(x, Y):
    x += Y
    return x
bootstrapiterations = 100
samplesize = 100
totalw = 0
for bootstrapiteration in range(bootstrapiterations):
    pointssample = points.takeSample(False, samplesize, 1)
    w = 2 * np.random.ranf(size=D) - 1
    for i in range(iterations):
        print("On interation %i" % (i + 1))
        w -= pointssample.map(lambda m: gradient(m, w)).reduce(add)
    totalw = totalw + w
print("Final expected w: " + str(totalw/bootstrapiterations))
 