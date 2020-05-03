from pyspark import RDD, SparkConf, SparkContext
from operator import add
import math
import os

def genHashGmap(Flist: list, parNum: int):
    gMap = {}
    for i, t in enumerate(Flist):
        gNum = i % parNum
        gMap[gNum] = gMap.get(gNum, []) + [t]
    return gMap

# successive grouping
def genSuccGMap(Flist: list, parNum: int):
    #gLen = math.ceil(len(Flist) / parNum)
    pass


def groupMap(gMap, tran):
    tranMap = {}
    for i, t in enumerate(tran):
        if t in gMap:
            gid = gMap[t] # successive partition
            tranMap[gid] = tran[:i + 1]
    return [(k, v) for k, v in tranMap.items()]
    

if __name__ == '__main__':
    conf = SparkConf()\
        .setAppName("incPFP")\
        .setMaster("local[*]")
    sc = SparkContext(conf = conf)
    inFile = "transData"

    minsup = 2

    transDataFile = sc.textFile(inFile)
    
    # step 1: generate F-list
    transData = transDataFile.map(lambda r: r.split())
    bigFlist = transData\
        .flatMap(lambda l: [(i, 1) for i in l])\
        .reduceByKey(add)\
        .filter(lambda i: i[1] >= minsup)\
        .sortBy(lambda i: i[1])\
        .collect()

    Flist = list(map(lambda t: t[0], bigFlist))

    # step 2: grouping items
    gLen = math.ceil(len(Flist) / sc.defaultParallelism)
    gMap = {t: math.floor(i / gLen) for i, t in enumerate(Flist)}
    
    groupedTrans = transData\
        .flatMap(lambda t:groupMap(gMap, t))\
        .groupByKey()\
        .map(lambda t: (t[0], list(t[1])))
    # print(groupedTrans)

    # step 3: 
    

    sc.stop()
    
