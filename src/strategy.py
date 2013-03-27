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

class Rule:
    rule_type = 0

    def __init__(self, rule_type):
	self.rule_type = rule_type

    def checkRule(self, op1, op2):
	if self.rule_type == 1:
	    if isinstance(op1, op.SpjOp) and isinstance(op2, op.Op):
		return True
	elif self.rule_type == 2:
	    if isinstance(op1, op.SpjeOp) and isinstance(op2, op.Op):
		return True
	elif self.rule_type == 3:
	    if isinstance(op1, op.SpjOp) and op1.is_sp and isinstance(op2, op.Op):
		return True
	elif self.rule_type == 4:
	    if isinstance(op1, op.Op) and isinstance(op2, op.SpjOp) and op2.is_sp:
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
	return new_op
	print new_op
	print isinstance(op1, op.SpjeOp), isinstance(op2, op.Op)
	if self.checkRule(op1, op2) is False:
	    return None
	    # TODO ERROR
	print new_op
	if self.rule_type == 1:
	    new_op.map_phase.append(op1.map_phase)
	    new_op.map_phase.append(op2.map_phase)
	    new_op.reduce_phase.append(op1.reduce_phase)
	    new_op.reduce_phase.append(op2.reduce_phase)
	elif self.rule_type == 2:
	    new_op.map_phase.append(op1.map_phase)
	    new_op.map_phase.append(op2.map_phase)
	    new_op.reduce_phase.append(op2.reduce_phase)
	    new_op.reduce_phase.append(op2.map_phase)
	elif self.rule_type == 3:
	    new_op.reduce_phase.append(op2.reduce_phase)
	    new_op.reduce_phase.append(op1.map_phase)
	    new_op.map_phase.append(op2.map_phase)
	elif self.rule_type == 4:
	    new_op.map_phase.append(op1.map_phase)
	    new_op.reduce_phase.append(op1.reduce_phase)
	    new_op.reduce_phase.append(op2.map_phase)
	return new_op
