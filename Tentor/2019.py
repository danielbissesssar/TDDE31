from pyspark import SparkContext

############## k-means algo 20190529
#converge distance, k, specified by user
#points rdd containing data
k = 0
points = 0
convergeDist = 0
points = points.cache()
def closestPoint(p, kPoints):
    bestIndex = 0
    currentDist = float("+inf")
    for i in range(len(kPoints)):
        tempDist = distance(p,1)
        if tempDist < currentDist:
            bestIndex = i
    return bestIndex

kPoints = points.takeSample(false, k)
tempDist = 1.0
while tempDist > convergeDist:
    data = points.map(lambda p: (closestPoint(p, kPoints), (p, 1)))
    pointStats = data.reduceByKey(lambda p1, p2: (p1[0]+p2[0], p1[1]+p2[1]))
    newPoints = pointStats.map(lambda p: p[0], (p[1][0]/p[1][1])).collect()
    tempDist = 0
    for i in range(len(kPoints)):
        tempDist += distance(kPoints[i], newPoints[i])
    for i in range(len(kPoints)):
        kPoints[i] = newPoints[i]


############## knn k-nearest-neighbors 20190529
#mydata rdd, p,k given by user
mydata = 0
p = 0
k = 0
def KNN(p, k, data):
    data = data.map(lambda x: (x[0], distance(p,x[1])))
    data = data.sortBy(lambda x: x[1])
    kPoints = data.take(k)
    sum = 0
    for i in range(k):
        sum += kPoints[i][0]
    fraction = sum/k
    if fraction > 0.5:
        result = 1
    else:
        result = 0
    return result
mydata = mydata.cache()
classification = KNN(p,k,mydata)

##################### logistic regression 20190822
import numpy as np
w = w * np.random.ranf(size=D) -1

def gradient(matrix, w):
    Y = matrix[:, 0]
    X = matrix[:, 1:]
    return((1.0 / (1.0 + np.exp(-Y * X.dot(w)))-1.0)* Y * X.T).sum(1)
def add(X,Y):
    X += Y
    return X

for i in range(iterations):
    w -= points.map(lambda x: gradient(x, w)).reduce(add)

print(w)

#################### cross validation 20