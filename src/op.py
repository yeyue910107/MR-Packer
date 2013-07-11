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
import strategy
import op
import node
import util
import expression

class Op(object):
    id = None
    cost = 0
    mrq_cost = 0
    map_phase = None
    reduce_phase = None
    node_list = None
    child_list = None
    parent = None
    ic_list = None	# input correlation nodes list
    oc_list = None	# output correlation nodes list
    pk_list = None
    pk_dic = None
    map_output = {}
    map_filter = {}
    reduce_output = {}
    reduce_filter = {}
    is_composite = False

    def __init__(self):
	self.id = []
	self.map_phase = []
	self.reduce_phase = []
	self.node_list = []
	self.child_list = []
	self.parent = None
	self.pk_dic = {}

    def __addOutputAndFilter__(self, _node, tn):
	if tn not in self.map_output.keys():
	    self.map_output[tn] = []
	self.map_output[tn].extend(_node.getMapOutput(tn))
	print "map_output", tn, self.map_output[tn]
	if tn not in self.map_filter.keys():
	    self.map_filter[tn] = []
	print "map_filter", tn, self.map_filter[tn]
	exp = _node.getMapFilter(tn)
	if exp is not None:
	    print exp.evaluate()
	    self.map_filter[tn].append(exp)

    def __addPKDic__(self, _node):
	for tn in _node.table_list:
	    self.pk_dic[tn] = []
	tmp_list = _node.getPartitionKey(False)
	for exp in tmp_list:
	    if isinstance(exp, expression.Column):
		self.pk_dic[exp.table_name].append(exp)
	    else:
		for tn in self.pk_dic.keys():
		    self.pk_dic[tn].append(exp)
	
	print "pk_dic", self.pk_dic

    def __isReducePhaseBottom__(self, _node):
	pass

    def __getIOCorrelation__(self):
	pass

    def postProcess(self):
	for x in self.map_phase:
	    if x not in self.node_list:
		self.node_list.append(x)
	for x in self.reduce_phase:
	    if x not in self.node_list:
		self.node_list.append(x)
	print "node_list", self.node_list
	
	for x in self.map_phase:
	    if isinstance(x, node.SPNode) and x.child not in self.node_list:
		self.__addOutputAndFilter__(x, x.table_list[0])
	    if isinstance(x, node.GroupbyNode) or isinstance(x, node.OrderbyNode) and x.child not in self.node_list:
		for tn in x.table_list:
		    self.__addOutputAndFilter__(x, tn)
		self.__addPKDic__(x)
		
	    if isinstance(x, node.JoinNode) and x.left_child not in self.node_list and x.right_child not in self.node_list:
		for tn in x.table_list:
		    self.__OutputAndFilter__(x, tn)
		self.__addPKDic__(x)

    @staticmethod
    def merge(op1, op2, rule_type, op1_child, op2_child):
	#print op1, op1.id, op2, op2.id, rule_type
	new_op = strategy.Rule(rule_type).merge(op1, op2)
	if new_op is None:
	    return None

	new_op.parent = op2.parent
	new_op.child_list.extend(op1.child_list)
	new_op.child_list.extend(op2.child_list)
	new_op.child_list.remove(op1)
	new_op.id.extend(op2.id)
	new_op.id.extend(op1.id)
	if op1.pk_list is None or len(op1.pk_list) == 0:
	    new_op.pk_list = op2.pk_list
	else:
	    new_op.pk_list = op1.pk_list
	
	op1_child.extend(op1.child_list)
	op2_child.extend(op2.child_list)

	if new_op.parent is not None:
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

    def getLowCostMRQ(self):
	if len(self.child_list) == 0:
	#if len(self.child_list) == 0 and self.isBottom():
	    print "FINDROOT_BEGIN"
	    root_mrq = self.findRoot()
	    root_mrq.__printAll__()
	    #if root_mrq.getMRQCost() < cost:
	    #	mrq = copyMRQ(root_mrq)
	    print "FINDROOT_END"
	    return
	for child in self.child_list:
	    child.getLowCostMRQ()
	    new_op = None
	    for i in range(1, 2):
		op1_child, op2_child = [], []
	        new_op = Op.merge(child, self, i, op1_child, op2_child)
		if new_op is not None:
		    new_op.getLowCostMRQ()
		    Op.detach(new_op, child, self, op1_child, op2_child)

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
