import copy
from itertools import chain, combinations

# powerset with empty set excluded
def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(1, len(s)+1))

# a minimum fp-tree structure for gid-based
# FPGrowth
class FPTree:
    #constructor
    def __init__(self, gid, flist, min_sup):
        self.gid = gid
        self.flist = flist # flist should be least freq->most freq
        self.root = Node('')
        self.min_sup = min_sup
        self.linkListHead = {}
        self.linkListTail = {}
    
    # private method to add an item in a transaction
    # returns the added node
    def _addOrIncNode(self, item, where):
        nodeToAdd = where.findChild(item)
        if nodeToAdd is not None:
            nodeToAdd.incNode()
            return nodeToAdd
        newNode = Node(item, parent = where) # to modify
        where.addChild(newNode)
        if item not in self.linkListHead:
            self.linkListHead[item] = newNode
        else:
            self.linkListTail[item].next = newNode
        self.linkListTail[item] = newNode
        return newNode

    # method to return a new transaction
    # sorted according to F-list
    def _fpsort(self, trans: list):
        sortedTran = []
        for i in reversed(self.flist):
            for j in trans:
                if j == i:
                    sortedTran.append(j)

        # print(sortedTran)
        return sortedTran

    def addTrans(self, newTran: list):
        newTran = self._fpsort(newTran)
        node = self.root
        for i in newTran:
            node = self._addOrIncNode(i, node)

    def display(self):
        self._printsubtree(self.root)

    # display nodes
    def _printsubtree(self, start):
        print(start)
        if len(start.children) == 0:
            return
        for i in start.children:
            self._printsubtree(i)

    def _getTran(self, start):
        itemList = []
        c = start.count
        p = start.parent
        while p.item != '':
            itemList.append(p.item)
            p = p.parent
        return itemList, c

    def _mineOneBranch(self, start):
        occurrences = start.count
        if occurrences < self.min_sup:
            return []
        itemList = []
        p = start.parent
        while p.item != '':
            itemList.append(p.item)
            p = p.parent
        return powerset(itemList)

    def _mineSingleItem(self, start):
        item = start.item
        newTrans = []
        newflistDic = {}

        s = start
        while s is not None:
            itemList, c = self._getTran(s)
            for i in itemList:
                newflistDic[i] = newflistDic.get(i, 0) + c
            if len(itemList) != 0:
                for _ in range(c):
                    newTrans.append(itemList)
            s = s.next
        newflist = []
        for k, v in newflistDic.items():
            if v >= self.min_sup:
                newflist.append((k, v))

        newflist.sort(key = lambda k: k[1]) # might need to maintain the lexicographic order
        newTreeFlist = [i[0] for i in newflist]
        print("node", start)
        print("flist", newTreeFlist)

        itemFpt = FPTree(self.gid, newTreeFlist, self.min_sup)
        for t in newTrans:
            itemFpt.addTrans(t)
        if start.next is None:
            return [(item, *i) for i in itemFpt._mineOneBranch(start)]
        
        print("sub-FPtree")
        itemFpt.display()
        return [(item, *i) for i in itemFpt.mine()]

            

    # return a list of tuples (freq itemsets)
    def mine(self):
        result = [(i, ) for i in self.flist]
        for header in self.flist:
            start = self.linkListHead[header]
            # print(start)
            result += self._mineSingleItem(start)
        return result


# Node structure for fp-tree nodes
class Node:
    def __init__(self, item: str, count = 1,\
        mynext = None, parent = None):

        self.item = item
        self.count = count
        self.next = mynext
        self.parent = parent
        self.children = []

    # increment the count
    def incNode(self):
        self.count += 1

    # private method to find a child node
    # returns the node if found
    # otherwise returns none
    def findChild(self, tofind):
        for i in self.children:
            if i.item == tofind:
                return i
        return None
    
    # add a child
    def addChild(self, child):
        self.children.append(child)

    def __repr__(self):
        return '(' + self.item + ', ' + str(self.count) + ')'
        
if __name__ == '__main__':
    # suppose transactions are 
    # [f, a, c, d, g, i, m, p]
    # [a, b, c, f, l, m, o]
    # [b, f, h, j, o, w]
    # [b, c, k, s, p]
    # [a, f, c, e, l, p, m, n]
    # min support = 3
    # f-list = [a, b, m, p, c, f]
    fpt = FPTree(0, ['a', 'b', 'm', 'p', 'c', 'f'], 3)
    fpt.addTrans(['f', 'a', 'c', 'd', 'g', 'i', 'm', 'p'])
    fpt.addTrans(['a', 'b', 'c', 'f', 'l', 'm', 'o'])
    fpt.addTrans(['b', 'f', 'h', 'j', 'o', 'w'])
    fpt.addTrans(['b', 'c', 'k', 's', 'p'])
    fpt.addTrans(['a', 'f', 'c', 'e', 'l', 'p', 'm', 'n'])
    fims = fpt.mine()
    print(fims)



    # fpt.display()

    # for i in fpt.linkListHead.values():
    #     start = i
    #     while start.next is not None:
    #         print(start)
    #         start = start.next


