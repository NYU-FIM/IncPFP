from pyspark import RDD, SparkConf, SparkContext
from operator import add
import math
import os
import fpm

def sortEachTran(t, flist):
    sortedTran = []
    for i in reversed(flist):
        for j in t:
            if j == i:
                sortedTran.append(j)
                
    return sortedTran

def computFlist(trans, head, min_sup):
    count = {}
    newTrans = []
    for t in trans:
        if t[-1] == head:
            newTrans.append(t[:-1])
            for i in t[:-1]:
                count[i] = count.get(i, 0) + 1
    flist = []
    for k, v in count.items():
        if v >= min_sup:
            flist.append((k, v))
           
    flist.sort(key = lambda t: t[1])
    returnFlist = list(map(lambda t: t[0], flist))
    return returnFlist, newTrans


def localFPM(gidTrans, gList, min_sup):
    gid, trans = gidTrans
    itemList = gList[gid]
    res = []
    #itemList is sorted already
    for i in itemList:
        flist, newtrans = computFlist(trans, i, min_sup)
        # print("item:", i)
        # print("flist:", newtrans)
        # print("trans", newtrans)
        localFpt = fpm.FPTree(gid, flist, min_sup)
        for t in newtrans:
            localFpt.addTrans(t)
        res += [(i, *j) for j in localFpt.mine()]
    # print("res", res)
    return res + [(i,) for i in itemList]

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
    gMap = {}
    gList = {}
    for i, t in enumerate(Flist):
        gid = math.floor(i / gLen)
        gMap[t] = gid
        gList[gid] = gList.get(gid, []) + [t]
    
    
    groupedTrans = transData\
        .map(lambda t: sortEachTran(t, Flist))\
        .flatMap(lambda t:groupMap(gMap, t))\
        .groupByKey()\
        .map(lambda t: (t[0], list(t[1])))
    print("gMap", gMap)
    print("gList", gList)
    print("Grouped Trans", groupedTrans.collect())

    # step 3: 
    staticRunFIs = groupedTrans.flatMap(lambda t:\
        localFPM(t, gList, minsup))
    
    print(staticRunFIs.collect())

    sc.stop()
    
