#!/usr/bin/python
"""
   Copyright (c) 2013, Yue.YE, ACT, Beihang University.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import sys
from strategy import Rule
import op
import node
import util
import expression
import codegen
import copy

count = 0

class Op(object):
    id = None
    cost = 0
    mrq_cost = 0
    map_phase = None
    reduce_phase = None
    node_list = None
    child_list = None
    table_list = None
    parent = None
    ic_list = None	# input correlation nodes list
    oc_list = None	# output correlation nodes list
    pk_list = None
    pk_dic = None
    map_output = {}
    map_filter = {}
    reduce_output = {}
    reduce_filter = {}
    output_node = None
    is_composite = False
    is_free_vertex = False
    alpha = 1
    beta = 1

    def __init__(self):
	self.id = []
	self.map_phase = []
	self.reduce_phase = []
	self.node_list = []
	self.child_list = []
	self.table_list = []
	self.parent = None
	self.pk_dic = {}
	self.ic_list = []
	self.oc_list = []
	self.map_output = {}
	self.map_filter = {}
	self.output_node = None
    
    def getID(self):
	if len(self.id) > 0:
 	    return str(self.id[0])
	return None

    def isSp(self):
	if isinstance(self, SpjOp) and self.is_sp:
	    return True
	return False

    def __addMapOutput__(self, _node, tn):
	if tn not in self.map_output.keys():
	    self.map_output[tn] = []
	tmp = _node.getMapOutput(tn)
	for exp1 in tmp:
	    flag = False
	    for exp2 in self.map_output[tn]:
		if exp1.compare(exp2):
		    flag = True
		    break
	    if flag is False:
		self.map_output[tn].append(exp1)
	#self.map_output[tn].extend(_node.getMapOutput(tn))

    def __addMapFilter__(self, _node, tn):
	'''if tn not in self.map_output.keys():
	    self.map_output[tn] = []
	self.map_output[tn].extend(_node.getMapOutput(tn))
	print "map_output", tn, self.map_output[tn]'''
	if tn not in self.map_filter.keys():
	    self.map_filter[tn] = []
	print "map_filter", tn, self.map_filter[tn]
	exp = _node.getMapFilter(tn)
	if exp is not None and exp.where_condition_exp is not None:
	    print exp.where_condition_exp.evaluate()
	    self.map_filter[tn].append(exp.where_condition_exp)

    def __addPK__(self, _node, tn, flag, _group, _join):
	pk_list = _node.getPartitionKey(flag)
	if tn not in self.pk_dic.keys():
	    self.pk_dic[tn] = []
	for item in pk_list:
	    for pk in item:
	        if isinstance(pk, expression.Constant):
		    print "pk", pk.evaluate()
		    new_pk = copy.deepcopy(pk)
		    self.pk_dic[0].append(new_pk)
	        elif isinstance(pk, expression.Column) and (_group or pk.table_name == _join):
	            print "pk", pk.evaluate(), _group, pk.table_name, _join
		    new_pk = copy.deepcopy(pk)
	            new_pk.table_name = tn
		    self.pk_dic[tn].append(new_pk)

    def __addPKDic__(self, _node):
	'''for tn in _node.table_list:
	    self.pk_dic[tn] = []
	tmp_list = _node.getPartitionKey(False)
	for exp in tmp_list:
	    if isinstance(exp, expression.Column):
		self.pk_dic[exp.table_name].append(exp)
	    else:
		for tn in self.pk_dic.keys():
		    self.pk_dic[tn].append(exp)'''
	'''print "add pk dic"
	tmp_list = _node.getPartitionKey(False)
	util.printExpList(tmp_list)
	print "self.parent", self.parent'''
	if isinstance(_node, node.JoinNode):
	    print "joinnode pkdic--------"
	    '''if isinstance(_node.left_child, node.SPNode) and isinstance(_node.left_child.child, node.TableNode):
		tn = _node.left_child.child.table_list[0]
		self.__addPK__(_node, tn, True, False, tn)
	    else:'''
	    if _node.left_child not in self.node_list and _node.left_child in self.child_list[0].node_list:
		tn = 0
	    else:
		tn = 1
	    self.__addPK__(_node, tn, False, False, "LEFT")
	    '''if isinstance(_node.right_child, node.SPNode) and isinstance(_node.right_child.child, node.TableNode):
		tn = _node.right_child.child.table_list[0]
		self.__addPK__(_node, tn, True, False, tn)
	    else:'''
	    if _node.right_child not in self.node_list and _node.right_child in self.child_list[0].node_list:
	        tn = 0
	    else:
	        tn = 1
	    self.__addPK__(_node, tn, False, False, "RIGHT")
		
	if isinstance(_node, node.GroupbyNode):
	    '''if isinstance(_node.child, node.SPNode) and isinstance(_node.child.child, node.TableNode):
		tn = _node.child.child.table_list[0]
		self.__addPK__(_node, tn, True, False, tn)
	    else:'''
	    self.__addPK__(_node, 0, False, True, "")
	if isinstance(_node, node.SPNode):
	    if isinstance(_node.child, node.TableNode):
		print "spnode pkdic--------------"
		tn = _node.child.table_list[0]
		print tn
		self.pk_dic[tn] = []
		if _node.parent is not None and _node.parent in self.node_list and (isinstance(_node.parent, node.JoinNode) or isinstance(_node.parent, node.GroupbyNode)):
		    self.__addPK__(_node.parent, tn, True, False, tn)
	    else:
		if _node.parent is not None and _node.parent in self.node_list:
		    if isinstance(_node.parent, node.GroupbyNode):
			self.__addPK__(_node.parent, 0, False, True, "")
		    elif isinstance(_node.parent, node.JoinNode):
			if _node.child in self.child_list[0].node_list:
			    tn = 0
			else:
			    tn = 1
			if _node == _node.parent.left_child:
			    self.__addPK__(_node.parent, tn, False, False, "LEFT"
)
			else:
			    self.__addPK__(_node.parent, tn, False, False, "RIGHT")
		else:
		    self.pk_dic[0] = []

    def __isPhaseBottom__(self, _node, phase):
	if isinstance(_node, node.JoinNode):
	    if _node.left_child is None and _node.right_child is None:
		return True
	    if _node.left_child not in self.node_list or _node.right_child not in self.node_list:
		return True
	    if _node.left_child in phase or _node.right_child in phase:
		return False
	    return self.__isPhaseBottom__(_node.left_child, phase) and self.__isPhaseBottom__(_node.right_child, phase)
	if _node.child is None:
	    return True
	if _node.child not in self.node_list:
	    return True
	if _node.child in phase:
	    return False
	return self.__isPhaseBottom__(_node.child, phase)

    def __getIOCorrelation__(self):
	print "map_phase", self.map_phase
	print "reduce_phase", self.reduce_phase
	for _node in self.reduce_phase:
	    if self.__isPhaseBottom__(_node, self.reduce_phase):
		self.ic_list.append(_node)
	    else:
		self.oc_list.append(_node)

    def getNodeList(self):
	for x in self.map_phase:
	    if x not in self.node_list:
		self.node_list.append(x)
	for x in self.reduce_phase:
	    if x not in self.node_list:
		self.node_list.append(x)
	for child in self.child_list:
	    child.getNodeList()

    def getTableList(self):
	for x in self.node_list:
	    if isinstance(x, node.SPNode) and isinstance(x.child, node.TableNode):
		for table in x.child.table_list:
		    if table not in self.table_list:
			self.table_list.append(table)

    def getOutputNode(self):
	if len(self.reduce_phase) > 0:
	    for _node in self.reduce_phase:
		if _node.parent is None or _node.parent not in self.node_list:
		    self.output_node = _node
		    return
	for _node in self.node_list:
	    if _node.parent is None or _node.parent not in self.node_list:
		self.output_node = _node
		return

    def criticalFun(self):
	# if fun(alpha, beta) > 0, return True
	return True
	#return False

    def reset(self):
	self.ic_list = []
	self.oc_list = []
	self.pk_dic = {}
	self.map_output = {}
	self.map_filter = {}
	self.node_list = []

    def postProcess(self):
	self.reset()
	self.getNodeList()
	self.getTableList()
	self.__getIOCorrelation__()
	print "ic_list:----------", self.ic_list
	print "oc_list:----------", self.oc_list
	self.getOutputNode()
	print "output_node:------", self.output_node
	for x in self.node_list:
	    if isinstance(x, node.SPNode) and isinstance(x.child, node.TableNode):
		self.__addMapOutput__(x, x.child.table_list[0])
		self.__addPKDic__(x)
	for x in self.map_phase:
	    if self.__isPhaseBottom__(x, self.map_phase) is False:
		continue
	    print "ismapphasebottom"
	    '''if isinstance(x, node.JoinNode):
		if isinstance(x.left_child, node.SPNode) and isinstance(x.left_child.child, node.TableNode):
		    self.__addMapOutput__(x.left_child, x.left_child.child.table_list[0])
		if isinstance(x.right_child, node.SPNode) and isinstance(x.right_child.child, node.TableNode):
		    self.__addMapOutput__(x.right_child, x.right_child.child.table_list[0])
	    if isinstance(x, node.GroupbyNode):
		if isinstance(x.child, node.SPNode) and isinstance(x.child.child, node.TableNode):
		    self.__addMapOutput__(x.child, x.child.child.table_list[0])
	    if isinstance(x, node.SPNode) and isinstance(x.child, node.TableNode):
		self.__addMapOutput__(x, x.child.table_list[0])'''

	    if isinstance(x, node.SPNode) and x.child not in self.node_list:
		self.__addMapFilter__(x, x.table_list[0])
	    
	    if isinstance(x, node.GroupbyNode) or isinstance(x, node.OrderbyNode) and x.child not in self.node_list:
		for tn in x.table_list:
		    self.__addMapFilter__(x, tn)
		
	    if isinstance(x, node.JoinNode) and x.left_child not in self.node_list and x.right_child not in self.node_list:
		for tn in x.table_list:
		    self.__addMapFilter__(x, tn)
	    self.__addPKDic__(x)
	print "pk_dic:"
	util.printExpDic(self.pk_dic)
	print "map_output:", self.map_output
	util.printExpDic(self.map_output)
	for child in self.child_list:
	    child.postProcess()

    @staticmethod
    def merge(op1, op2, rule_type, op1_child, op2_child):
	#print op1, op1.id, op2, op2.id, rule_type
	new_op = Rule(rule_type).merge(op1, op2)
	if new_op is None:
	    return None
	print "op2.parent:", op2, op2.id, op2.parent, op2.parent.id, op2.parent.child_list
	new_op.parent = op2.parent
	new_op.child_list.extend(op1.child_list)
	new_op.child_list.extend(op2.child_list)
	new_op.child_list.remove(op1)
	new_op.id.extend(op2.id)
	new_op.id.extend(op1.id)
	print "new_op:", new_op, new_op.id
	print "op2:", op2, op2.id
	if op1.pk_list is None or len(op1.pk_list) == 0:
	    new_op.pk_list = op2.pk_list
	else:
	    new_op.pk_list = op1.pk_list
	
	op1_child.extend(op1.child_list)
	op2_child.extend(op2.child_list)

	if new_op.parent is not None:
	    print "new_op.parent:", new_op.parent, new_op.parent.child_list, new_op.parent.id
	    op2_index = new_op.parent.child_list.index(op2)
	    new_op.parent.child_list[op2_index] = new_op
	for child in new_op.child_list:
	    child.parent = new_op

	return new_op

    @staticmethod
    def detach(new_op, op1, op2, op1_child, op2_child):
	if new_op.parent is not None:
	    new_op_index = new_op.parent.child_list.index(new_op)
	    new_op.parent.child_list[new_op_index] = op2
	for child in new_op.child_list:
	    if child in op1_child:
		child.parent = op1
	    elif child in op2_child:
		child.parent = op2

    def findRoot(self):
	p = self
	while p.parent is not None:
	    p = p.parent
	return p

    def isBottom(self):
	if self.parent is None:
	    return True
	if self == self.parent.child_list[-1]:
	    return self.parent.isBottom() and True
	return False

    def getCost(self):
	return cost

    def getMRQCost(self):
	all_cost = self.getCost()
	for child in self.child_list:
	    all_cost += child.getMRQCost()
	return all_cost

    def getLowCostMRQ(self, stop, _run=True):
	global count
	if len(self.child_list) == 0:
	#if len(self.child_list) == 0 and self.isBottom():
	    print "FINDROOT_BEGIN"
	    count = count + 1
	    #root_mrq = self.findRoot()
	    #root_mrq.__printAll__()
	    #root_mrq.postProcess()
	    #if root_mrq.id == [1] and root_mrq.child_list[0].id == [2, 3, 4, 5, 6, 7, 8, 9]:
	    #    root_mrq.__printAll__()
	    #	codegen.genCode(root_mrq, "testquery")
	    if count >= int(stop):
		root_mrq = self.findRoot()
		root_mrq.postProcess()
		#root_mrq.__printAll__()
		#op_list = codegen.genCode(root_mrq, "testquery")
		#op_id = [op.getID() for op in op_list]
		#print op_id
		codegen.run(root_mrq, "testquery", _run)
		exit(29)
		#return
	    #if root_mrq.getMRQCost() < cost:
	    #	mrq = copyMRQ(root_mrq)
	    #    print "END"
	    #    exit(29)
	    print "FINDROOT_END"
	    return
	if count >= int(stop):
	    return
	for child in self.child_list:
	    child.getLowCostMRQ(stop, _run)
	    if count >= int(stop):
		return
	    new_op = None
	    for i in range(1, 5):
		op1_child, op2_child = [], []
	        new_op = Op.merge(child, self, i, op1_child, op2_child)
		if new_op is not None:
		    new_op.getLowCostMRQ(stop, _run)
		    Op.detach(new_op, child, self, op1_child, op2_child)

    def optimize(self):
        minset_map = {}
        minset = [[]] * 10
        minset_num = 0
        free_vertexes = []
        self.preOptimize(free_vertexes, minset_map, minset, minset_num)
	print "minset_map:", minset_map
        self = self.mergeMinset(minset_map, minset, minset_num)
        self.mergeFreeVertex(free_vertexes)

    def mergeMinset(self, minset_map, minset, minset_num):
        if len(self.child_list) == 0:
            #root_mrq = self.findRoot()
            #root_mrq.__printAll__()
            return self
	if len(self.child_list) == 1:
	    child = self.child_list[0]
	    tmp = child.mergeMinset(minset_map, minset, minset_num)
	    if tmp is not None and tmp.is_free_vertex is False and minset_map[self] == minset_map[tmp]:
	        new_op = None
	        st_type = Rule.get_st(tmp, self)
		print "st_type:", st_type
                op1_child, op2_child = [], []
                new_op = Op.merge(tmp, self, st_type, op1_child, op2_child)
	        minset_map[new_op] = minset_map[self]
	        minset.append(new_op)
		self = new_op
	    return self
	if len(self.child_list) == 2:
	    [lchild, rchild] = self.child_list
            lop = lchild.mergeMinset(minset_map, minset, minset_num)
            rop = rchild.mergeMinset(minset_map, minset, minset_num)
            if lop is not None and lop.is_free_vertex is False and minset_map[self] == minset_map[lop]:
                new_op = None
                st_type = Rule.get_st(lop, self)
		print "st_type:", st_type
                op1_child, op2_child = [], []
                new_op = Op.merge(lop, self, st_type, op1_child, op2_child)
	        minset_map[new_op] = minset_map[self]
	        minset.append(new_op)
		self = new_op
            if rop is not None and rop.is_free_vertex is False and minset_map[self] == minset_map[rop]:
                new_op = None
                st_type = Rule.get_st(rop, self)
		print "st_type:", st_type
                op1_child, op2_child = [], []
                new_op = Op.merge(rop, self, st_type, op1_child, op2_child)
	        minset_map[new_op] = minset_map[self]
	        minset.append(new_op)
		self = new_op
	    return self
	        
        '''for child in self.child_list:
            child.mergeMinset(minset_map, minset, minset_num)
	    print "minset_map:", minset_map
	    print self, self.id, child, child.id
            if child.is_free_vertex or minset_map[self] != minset_map[child]:
                continue
            new_op = None
            st_type = Rule.get_st(child, self)
            op1_child, op2_child = [], []
            new_op = Op.merge(child, self, st_type, op1_child, op2_child)
	    minset_map[new_op] = minset_map[self]
	    minset.append(new_op)
	    self = new_op
            #new_op.mergeMinset(minset_map, minset, minset_num)'''

    def mergeFreeVertex(self, free_vertexes):
        for fv in free_vertexes:
            p = fv.getPrevNoneSPOp()
            q = fv.getPostNoneSPOp()
            if fv.criticalFun() and fv.alpha * fv.beta <= 1:
                Op.merge(p, fv, 4, [], [])
                #fv to p.reduce_phase
            elif fv.criticalFun() and fv.alpha * fv.beta > 1:
                Op.merge(fv, q, 3, [], [])
                #fv to q.reduce_phase
            elif fv.criticalFun() is False and fv.alpha * fv.beta <= 1:
                if isinstance(p, Op.Spj):
                    new_op1 = Op.merge(p, fv, 1, [], [])
                else:
                    new_op2 = Op.merge(p, fv, 2, [], [])
                #fv to p(3, 4)
            else:
                new_op1 = Op.merge(p, fv, 4, [], [])
                new_op2 = Op.merge(fv, q, 3, [], [])
                # compare cost
                #fv to (p,q)(3,4)

    def preOptimize(self, free_vertexes, minset_map, minset, minset_num):
        _node = self.node_list[0]
        if _node.parent is None:
            minset_map[self] = 0
	    minset[0] = [self]
	else:
            prev = self.getPrevNoneSPOp()
            if isinstance(_node, node.SPNode):
	        if isinstance(_node.child, node.TableNode):
		    minset_map[self] = minset_map[prev]
		    minset[minset_map[prev]].append(self)
		    return
	        self.child_list[0].preOptimize(free_vertexes, minset_map, minset, minset_num)
                return
            if Rule.__pk_compare__(self.pk_list, prev.pk_list):
                if isinstance(_node.parent, node.SPNode):
                    minset_map[self.parent] = minset_map[prev]
                    minset[minset_map[self.parent]].append(self.parent)
                minset_map[self] = minset_map[prev]
                minset[minset_map[prev]].append(self)
            else:
                if isinstance(_node.parent, node.SPNode):
                    self.parent.is_free_vertex = True
                    free_vertexes.append(self.parent)
                minset_num = minset_num + 1
                minset_map[self] = minset_num
                minset[minset_num].append(self)
        for child in self.child_list:
            child.preOptimize(free_vertexes, minset_map, minset, minset_num)

    def getPrevNoneSPOp(self):
        if self.parent is None:
            return None
        if isinstance(self.parent.node_list[0], node.SPNode) is False:
            return self.parent
        return self.parent.getPrevNoneSPOp()

    def getPostNoneSPOp(self):
        if len(self.child_list) > 1:
            return None
        if isinstance(self.child_list[0].node_list[0], node.SPNode) is False:
            return self.child_list[0].getPostNoneSPOp()
        return self.child_list[0].getPostNoneSPOp()

    def getReducePhasePostNode(self, _node, root, res=[]):
	if _node is None or _node not in self.node_list:
	    return
        if _node != root and _node in self.reduce_phase:
	    res.append(_node)
	    return
	if isinstance(_node, node.JoinNode):
	    self.getReducePhasePostNode(_node.left_child, root, res)
	    self.getReducePhasePostNode(_node.right_child, root, res)
	else:
	    self.getReducePhasePostNode(_node.child, root, res)

    def __print__(self):
	print self, self.id
	print "map:"
	for node in self.map_phase:
	    print node.name
	print "reduce:"
	for node in self.reduce_phase:
	    print node.name
	print "CHILD_LIST: ", self.child_list

    def __printAll__(self):
	self.__print__()
	for child in self.child_list:
	    child.__printAll__()

class SpjOp(Op):
    is_sp = False

    def __init__(self):
	super(op.SpjOp, self).__init__()
	pass

class SpjeOp(Op):
    def __init__(self):
	super(op.SpjeOp, self).__init__()
	pass

if __name__ == "__main__":
    op1 = op.SpjeOp()
    op2 = op.SpjOp()
    op3 = op.SpjeOp()
    op4 = op.SpjOp()
    op5 = op.SpjOp()
    op6 = op.SpjOp()
    print op1, op2, op3, op4, op5, op6
    op1.id.append(1)
    op2.id.append(2)
    op3.id.append(3)
    op4.id.append(4)
    op5.id.append(5)
    op6.id.append(6)
    op4.is_sp = True
    op5.is_sp = True
    op6.is_sp = True
    op1.child_list.append(op2)
    op2.child_list.append(op3)
    op2.child_list.append(op4)
    op2.parent = op1
    op3.child_list.append(op5)
    op3.child_list.append(op6)
    op3.parent = op2
    op4.parent = op2
    op5.parent = op3
    op6.parent = op3
    op1.getLowCostMRQ()
