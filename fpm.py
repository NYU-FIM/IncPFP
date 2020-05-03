import copy

# a minimum fp-tree structure for gid-based
# FPGrowth
class FPTree:
    #constructor
    def __init__(self, gid, flist):
        self.gid = gid
        self.flist = flist # flist should be least freq->most freq
        self.root = FPTree.Node('')
    
    # private method to add an item in a transaction
    # returns the added node
    def _addOrIncNode(self, item, where: FPTree.Node):
        nodeToAdd = where.findChild(item)
        if nodeToAdd:
            nodeToAdd.incNode()
            return nodeToAdd
        newNode = FPTree.Node(item, parent = where) # to modify
        where.addChild(newNode)
        return newNode

    # method to return a new transaction
    # sorted according to F-list
    def _fpsort(self, trans: list):
        sortedTran = []
        for i in reversed(self.flist):
            for j in trans:
                if j == i:
                    sortedTran.append(j)
        return sortedTran

    def addTrans(self, newTran: list):
        newTran = self._fpsort(newTran)
        node = self.root
        for i in newTran:
            node = self._addOrIncNode(i, node)


    # Node structure for fp-tree nodes
    class Node:
        def __init__(self, item: str, count = 1,\
            mynext = None, parent = None,\
            children = []):

            self.item = item
            self.count = count
            self.next = mynext
            self.parent = parent
            self.children = children

        # increment the 
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
        