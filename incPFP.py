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

def inclocalFPM(gidTrans, gList, min_sup, incFlist):
    gid, trans = gidTrans
    itemList = gList[gid]
    res = []
    #itemList is sorted already
    for i in itemList:
        if i not in incFlist:
            continue
        flist, newtrans = computFlist(trans, i, min_sup)
        # local fp-tree is the conditional pattern base of item i
        localFpt = fpm.FPTree(gid, flist, min_sup)
        for t in newtrans:
            localFpt.addTrans(t)
        # res.append((i, localFpt))
        res += [(i, *j) for j in localFpt.mine()] 
    return res


# returns a list of tuples: (item, conditional FP-tree)
def localFPM(gidTrans, gList, min_sup):
    gid, trans = gidTrans
    itemList = gList[gid]
    res = [(i,) for i in itemList]
    #itemList is sorted already
    for i in itemList:
        flist, newtrans = computFlist(trans, i, min_sup)
        # local fp-tree is the conditional pattern base of item i
        localFpt = fpm.FPTree(gid, flist, min_sup)
        for t in newtrans:
            localFpt.addTrans(t)
        # better to find a way to store the tree?
        res += [(i, *j) for j in localFpt.mine()] 
    return res

def genHashGmap(Flist: list, parNum: int):
    gMap = {}
    for i, t in enumerate(Flist):
        gNum = i % parNum
        gMap[gNum] = gMap.get(gNum, []) + [t]
    return gMap

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



    minsupperc = 0.4

    transDataFile = sc.textFile(inFile)
    numTrans = transDataFile.count()
    minsup = minsupperc * numTrans
    
    # step 1: generate F-list
    transData = transDataFile.map(lambda r: r.split())
    bigFlist = transData\
        .flatMap(lambda l: [(i, 1) for i in l])\
        .reduceByKey(add)\
        .filter(lambda i: i[1] >= minsup)\
        .sortBy(lambda i: i[1])\
        .collect()
    print(bigFlist)

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
    # print("gMap", gMap)
    # print("gList", gList)
    # print("Grouped Trans", groupedTrans.collect())

    # step 3: 
    staticRunFIs = groupedTrans.flatMap(lambda t:\
        localFPM(t, gList, minsup))
    print(staticRunFIs.collect())

    incTrans = sc.textFile('incData')

    incTransData = incTrans.map(lambda r: r.split())
    incBigFlist = incTransData\
        .flatMap(lambda l: [(i, 1) for i in l])\
        .reduceByKey(add)\
        .filter(lambda i: i[1] >= minsup)\
        .collect() # no need to sort for the incFlist

    incFlist = list(map(lambda t: t[0], incBigFlist))
    
    # update global flist
    for i, c in incBigFlist:
        for ind, j in enumerate(bigFlist):
            if j == i:
                bigFlist[ind] = (j[0], j[1] + c)
    

    print(bigFlist)
    totalTransData = transData.union(incTransData)

    incGroupedTrans = totalTransData\
        .map(lambda t: sortEachTran(t, Flist))\
        .flatMap(lambda t:groupMap(gMap, t))\
        .groupByKey()\
        .map(lambda t: (t[0], list(t[1])))
    newFIs = incGroupedTrans.flatMap(lambda t:\
        inclocalFPM(t, gList, minsup, incFlist))

    print(totalTransData.collect())
    print(newFIs.collect())

    #TODO: use heap if want to remove duplicates
    # in the updated FIs

    sc.stop()
    
