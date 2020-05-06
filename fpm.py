# a minimum fp-tree structure for gid-based
# FPGrowth
class FPTree:
    #constructor
    def __init__(self, gid, flist):
        self.gid = gid
        self.flist = flist # flist should be least freq->most freq
        self.root = Node('')
        self.linkListHead = {}
        self.linkListTail = {}
        for i in flist:
            newNode = Node(i)
            self.linkListHead[i] = newNode
            self.linkListTail[i] = newNode
    
    # private method to add an item in a transaction
    # returns the added node
    def _addOrIncNode(self, item, where):
        nodeToAdd = where.findChild(item)
        if nodeToAdd is not None:
            nodeToAdd.incNode()
            return nodeToAdd
        newNode = Node(item, parent = where) # to modify
        where.addChild(newNode)
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

        print(sortedTran)
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
    fpt = FPTree(0, ['a', 'b', 'm', 'p', 'c', 'f'])
    fpt.addTrans(['f', 'a', 'c', 'd', 'g', 'i', 'm', 'p'])
    fpt.addTrans(['a', 'b', 'c', 'f', 'l', 'm', 'o'])
    fpt.addTrans(['b', 'f', 'h', 'j', 'o', 'w'])
    fpt.addTrans(['b', 'c', 'k', 's', 'p'])
    fpt.addTrans(['a', 'f', 'c', 'e', 'l', 'p', 'm', 'n'])
    fpt.display()
