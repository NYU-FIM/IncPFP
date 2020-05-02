from pyspark import RDD, SparkConf, SparkContext
from operator import add
import os

if __name__ == '__main__':
    conf = SparkConf().setAppName("incPFP")\
        .setMaster("local[*]")
    sc = SparkContext(conf = conf)
    inFile = "transData"

    minsup = 2

    transDataFile = sc.textFile(inFile)
    
    # step 1 & 2: generate F-list
    transData = transDataFile.map(lambda r: r.split())
    bigFlist = transData.flatMap(lambda l: [(i, 1) for i in l])\
        .reduceByKey(add)\
        .filter(lambda i: i[1] >= minsup)\
        .sortBy(lambda i: i[1])\
        .collect()

    Flist = list(map(lambda t: t[0], bigFlist))
    #print(Flist)

    # step 3: grouping items


    sc.stop()
    
