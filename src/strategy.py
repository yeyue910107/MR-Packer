#! /usr/bin/python
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
import op
import util

class Rule:
    rule_type = 0

    def __init__(self, rule_type):
	self.rule_type = rule_type

    @staticmethod
    def __pklist_compare__(pk_list1, pk_list2):
	ret = True

        if pk_list1 is None or pk_list2 is None or len(pk_list1) == 0 or len(pk_list2) == 0:
            return True

        if len(pk_list1) != len(pk_list2):
            return False

        for i in range(0, len(pk_list1)):
            flag = False
            for j in range(0, len(pk_list2)):
                if pk_list1[i].compare(pk_list2[j]):
                    flag = True
            if flag is False:
                ret = False
        return ret

    @staticmethod
    def __pk_compare__(pk1, pk2):
	if pk1 is None or pk2 is None or len(pk1) == 0 or len(pk2) == 0:
	    return True
	for x in pk1:
	    for y in pk2:
		if Rule.__pklist_compare__(x, y):
		    return True
	return False

    def checkRule(self, op1, op2):
	print "check rule:******"
	print "op1:"
	op1.__print__()
	print "op2:"
	op2.__print__()
	print "pk1_list:"
	for pk in op1.pk_list:
	    util.printExpList(pk)
	print "pk2_list:"
	for pk in op2.pk_list:
	    util.printExpList(pk)
	if Rule.__pk_compare__(op1.pk_list, op2.pk_list) is False:
	    return False
	print "pk_compare is true!!!!!!!!"
	print "self.ruletype:", self.rule_type
	if self.rule_type == 1:
	    if isinstance(op1, op.SpjOp) and isinstance(op2, op.Op):
		print "use rule 1......"
		return True
	elif self.rule_type == 2:
	    if isinstance(op1, op.SpjeOp) and isinstance(op2, op.Op):
		print "use rule 2......"
		return True
	elif self.rule_type == 3:
	    if isinstance(op1, op.SpjOp) and op1.is_sp and isinstance(op2, op.Op):
		print "use rule 3......"
		return True
	elif self.rule_type == 4:
	    if isinstance(op1, op.Op) and isinstance(op2, op.SpjOp) and op2.is_sp:
		print "use rule 4......"
		return True
	return False

    @staticmethod
    def merge_type(op1, op2):
	if isinstance(op1, op.SpjeOp) or isinstance(op2, op.SpjeOp):
	    return True
	return False

    def merge(self, op1, op2):
	new_op = None
	if Rule.merge_type(op1, op2):
	    new_op = op.SpjeOp()
	else:
	    new_op = op.SpjOp()
	#return new_op
	#print new_op
	#print isinstance(op1, op.SpjeOp), isinstance(op2, op.Op)
	if self.checkRule(op1, op2) is False:
	    return None
	    # TODO ERROR
	#print new_op
	if self.rule_type == 1:
	    new_op.map_phase.extend(op1.map_phase)
	    new_op.map_phase.extend(op2.map_phase)
	    new_op.reduce_phase.extend(op1.reduce_phase)
	    new_op.reduce_phase.extend(op2.reduce_phase)
	elif self.rule_type == 2:
	    new_op.map_phase.extend(op1.map_phase)
	    new_op.map_phase.extend(op2.map_phase)
	    new_op.reduce_phase.extend(op1.reduce_phase)
	    new_op.reduce_phase.extend(op2.reduce_phase)
	    '''for _node in op2.map_phase:
		if _node not in new_op.reduce_phase:
		    new_op.reduce_phase.append(_node)
	    #new_op.reduce_phase.extend(op2.map_phase)'''
	elif self.rule_type == 3:
	    new_op.reduce_phase.extend(op2.reduce_phase)
	    new_op.reduce_phase.extend(op1.map_phase)
	    new_op.map_phase.extend(op2.map_phase)
	elif self.rule_type == 4:
	    new_op.map_phase.extend(op1.map_phase)
	    new_op.reduce_phase.extend(op1.reduce_phase)
	    new_op.reduce_phase.extend(op2.map_phase)
	return new_op

    @staticmethod
    def get_st(op1, op2):
	if op1.isSp() and op2.isSp():
	    return 1
	if op1.isSp() and op2.isSp() is False:
	    if op1.criticalFun():
		return 1
	    return 3
	if isinstance(op1, op.SpjOp) and op2.isSp():
	    if op2.criticalFun():
		return 1
	    return 4
	if isinstance(op1, op.SpjOp) and op1.isSp() is False and op2.isSp() is False:
	    return 1
	if isinstance(op1, op.SpjeOp):
	    if op2.isSp():
		if op2.criticalFun() is False:
		    return 4
	    return 2
	    
