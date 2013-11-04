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

import util
import copy

class Expression():
    exp_type = None #expression type coule be Function, Constant, Column, etc.

    def __init__(self):
        pass
    
    def evaluate(self):
	pass

    def genTableName(self, node):
	pass

    def genIndex(self, node):
	pass
	
    def hasGroupbyFunc(self):
	pass

class Function(Expression):
    func_name = None
    para_list = []
    func_obj = None

    def __init__(self, func_name, para_list):
        self.func_name = func_name
	self.para_list = para_list
	self.setFuncObj(self)
    
    def getExpType(self):
	return "FUNC"

    def evaluate(self):
	ret = ""
	for para in self.para_list:
	    tmp = para.evaluate()
	    ret = ret + str(tmp) + ","
        ret = ret[:-1]
        return self.func_name + "(" + ret + ")"
	
    def setFuncObj(self, func_obj):
	self.func_obj = func_obj
	for para in self.para_list:
	    para.setFuncObj(self)

    def setParaList(self, para_list):
	self.para_list = para_list
	self.setFuncObj(self)

    def getValueType(self):
	return "DECIMAL"
	
    def replace(self, old, new):
	new_list = []
	for para in self.para_list:
	    if para.compare(old) is True:
		new_list.append(new)
	    else:
		new_list.append(old)
	self.setParaList(new_list)

    def compare(self, exp):
        if self.getExpType() != exp.getExpType():
            return False
        if self.func_name != exp.func_name:
            return False
        if len(self.para_list) != len(exp.para_list):
            return False
        for i in range(0, len(self.para_list)):
            if self.para_list[i].compare(exp.para_list[i]) is False:
                return False

        return True

    def genTableName(self, node):
	col_list = []
	self.getPara(col_list)
	for item in col_list:
	    item.genTableName(node)
	
    def getPara(self, col_list):
	for para in self.para_list:
	    if isinstance(para, Column):
		col_list.append(para)
	    elif isinstance(para, Function):
		para.getPara(col_list)
    
    def genIndex(self, node):
	para_list = self.para_list
	new_para_list = []
	for para in para_list:
	    if isinstance(para, Column):
		if para.column_name == "*":
		    # TODO error
		    pass
		if para.table_name in node.table_list:
		    new_para = para.genIndex(node)
		    if new_para is None:
			# TODO error
			pass
		    new_para_list.append(new_para)
		else:
		    new_para_list.append(para)
	    elif isinstance(para, Function):
		tmp_exp = para.genIndex(node)
		if tmp_exp is None:
		    # TODO error
		    pass
		new_para_list.append(tmp_exp)
	    elif isinstance(para, Constant):
		new_para_list.append(para)
	return Function(self.func_name, list(new_para_list))
	
    def addToSelectList(self, node, new_dic, new_list):
	if self is None or not isinstance(self, Function):
	    return
	if self.func_name in ["AND", "OR"]:
	    for para in self.para_list:
		if isinstance(para, Function):
		    para.addToSelectList(node, new_dic, new_list)
	else:
	    col_list = []
	    self.getPara(col_list)
	    for col in col_list:
		if col.table_name in node.table_list or col.table_name in node.table_alias_dic.values():
		    flag = False
		    for tmp in new_list:
			if tmp.table_name == col.table_name and tmp.column_name == col.column_name:
			    flag = True
			    break
		    if flag is False:
		        tmp = copy.deepcopy(col)
		        new_list.append(tmp)
		        new_dic[tmp] = None
				
    def hasGroupbyFunc(self):
	flag = False
	if self.func_name in util.agg_func_list:
	    return True
	for para in self.para_list:
	    flag |= para.hasGroupbyFunc()
	return flag
	
    def getGroupbyFuncName(self):
	if not isinstance(self, Function):
	    return None
	if self.func_name in util.agg_func_list:
	    return self.func_name
	for para in self.para_list:
	    func_name = para.getGroupbyFuncName()
	    if func_name is not None:
		return func_name

    def booleanFilter(self, node, rm_flag):
	ret_exp = None
	func_list = ["AND", "OR"]
	if not isinstance(self, Function):
	    # TODO error
	    return
	# debug
	print "self", self.func_name
	if self.func_name == "IS":
	    return None
	if self.func_name in func_list:
	    exp_list = []
	    tmp_flag = True
	    for exp in self.para_list:
		if isinstance(exp, Function):
		    # debug
		    print "para_list", exp.func_name
		    if self.func_name == "OR":
		        tmp_exp = exp.booleanFilter(node, False)
		    else:
		        tmp_exp = exp.booleanFilter(node, rm_flag)
		    print "tmp_exp", tmp_exp
		    if self.func_name == "OR" and tmp_exp is None:
		        tmp_flag = False
		    if tmp_exp is not None:
		        exp_list.append(tmp_exp)

	    if tmp_flag is False or len(exp_list) == 0:
		return None
	    elif len(exp_list) == 1:
		ret_exp = copy.deepcopy(exp_list[0])
		if rm_flag is True:
		    exp_list[0].removePara()
		return ret_exp
	    else:
		if rm_flag is True:
		    for exp in exp_list:
			exp.removePara()
		ret_exp = Function(self.func_name, exp_list)
		return ret_exp
	else:
	    exp_flag = True
	    for exp in self.para_list:
		print exp.evaluate()
		tmp_flag = False
		if isinstance(exp, Column):
		    print node.table_list
		    for table in node.table_list:
			if table == exp.table_name and util.isColumnInTable(exp.column_name, table):
			    tmp_flag = True
			    print "tmp_flag is true"
			    break
		    for key in node.table_alias_dic.keys():
			if key == exp.table_name and isColumnInTable(exp.column_name, node.table_alias_dic[key]):
			    tmp_flag = True
			    break
		elif isinstance(exp, Constant):
		    tmp_flag = True
		else:
		    if exp.booleanFilter(node, False) is not None:
			tmp_flag = True
		exp_flag = tmp_flag and exp_flag
	    print "exp_flag is ", exp_flag, "remove_flag is ", rm_flag
	    if exp_flag is True:
		ret_exp = copy.deepcopy(self)
		if rm_flag is True:
		    self.removePara()
		return ret_exp
	    else:
		return None			

    def groupbyWhereFilter(self):
	func_list = ["AND", "OR"]
	if not isinstance(self, Function):
	    # TODO error
	    pass
	if self.func_name in func_list:
	    exp_list = filter(lambda x:x is not None, [para.groupbyWhereFilter() for para in self.para_list])
	    if len(exp_list) == 0:
		return None
	    if len(exp_list) == 1:
		exp_list[0].removePara()
		return exp_list[0]
	    for exp in exp_list:
		exp.removePara()
	    return Function(self.func_name, exp_list)
	else:
	    if self.hasGroupbyFunc():
		return None
	    return self

    def selectListFilter(self, node, new_dic, new_list):
	col_list = []
	self.getPara(col_list)
	for col in col_list:
	    if col.table_name in node.table_list:
		flag = False
		for exp in new_list:
		    if exp.compare(col) is True:
			flag = True
			break
		if flag is False:
		    new_exp = copy.deepcopy(col)
		    new_list.append(new_exp)
		    new_dic[new_exp] = None

    def removePara(self):
	print "REMOVE PARA"
	print self.evaluate()
	print self.func_obj.evaluate()
	exp = self.func_obj
	new_list = []
	if not isinstance(exp, Function) or exp == self:
	    return
	#new_list = filter(lambda x:x != self, self.para_list)
	for item in exp.para_list:
	    if item == self:
		continue
	    else:
		new_list.append(item)
	exp.setParaList(new_list)

    def genJoinKey(self):
	ret_exp = None

	if self.func_name == "AND":
	    exp_list = []
	    for para in self.para_list:
		tmp = para.genJoinKey()
		if tmp is not None:
		    exp_list.append(tmp)
	    
	    if len(exp_list) == 0:
		return None
	    if len(exp_list) == 1:
		ret_exp = copy.deepcopy(exp_list[0])
		exp_list[0].removePara()
		return ret_exp
	    for exp in exp_list:
		exp.removePara()
	    return Function("AND", exp_list)
	else:
	    if self.func_name != "EQ":
		return None
	    if len(self.para_list) != 2:
		return None
	    if isinstance(self.para_list[0], Column) and isinstance(self.para_list[1], Column) and self.para_list[0].table_name != self.para_list[1].table_name:
		return self
	    return None

class Column(Expression):
    column_type = None
    column_name = None
    table_name = None
    func_obj = None
	
    def __init__(self, table_name, column_name):
	self.table_name = table_name
	self.column_name = column_name

    def getExpType(self):
	return "COL"

    def evaluate(self):
	if self.table_name == "":
	    return "UNKNOWN." + str(self.column_name)
	return str(self.table_name) + "." + str(self.column_name)

    def setFuncObj(self, func_obj):
	self.func_obj = func_obj

    def genTableName(self, node):
	if self.table_name != "":
	    table = util.searchTable(self.table_name)
	    if table is None and self.table_name in node.table_alias_dic.keys():
		table = util.searchTable(node.table_alias_dic[self.table_name])
	    if table is None:
		# TODO error
		return
	    print table.table_name, self.column_name
	    self.column_type = table.getColumnByName(self.column_name).column_type
	    return
	
	if self.column_name == "*":
	    return
	col_list = util.searchColumn(self.column_name)
	for col in col_list:
	    if col.table_schema.table_name in node.table_list:
		self.table_name = col.table_schema.table_name
		self.column_type = col.table_schema.getColumnByName(self.column_name).column_type
		return
	    for key in node.table_alias_dic.keys():
		if col.table_schema.table_name == self.table_alias_dic[key]:
		    self.table_name = key
		    self.column_type = col.table_schema.getColumnByName(self.column_name).column_type
		    return
	
    def genIndex(self, node):
	new_exp = None
	if self.column_name == "*":
	    new_exp = copy.deepcopy(self)
	    return new_exp
	if self.table_name != "":
	    if self.table_name not in node.table_alias_dic.keys():
		if self.table_name not in node.table_list:
		    # TODO error
		    pass
		tmp_table = util.searchTable(self.table_name)
		if tmp_table is None:
		    return None
		index = tmp_table.getColumnIndexByName(self.column_name)
		new_exp = Column(self.table_name, index)
		new_exp.column_name = int(new_exp.column_name)
		new_exp.column_type = self.column_type
	else:
	    tmp_column = util.searchColumn(self.column_name)
	    tmp_table = None
	    for tmp in tmp_column:
		if tmp.table_schema.table_name in node.table_list or tmp.table_schema.table_name in node.table_alias_dic.values():
		    tmp_table = searchTable(tmp.table_schema.table)
		    break
	    if tmp_table is None:
		return None
	    index = tmp_table.getColumnIndexByName(self.column_name)
	    new_exp = Column(tmp.table_schema.table_name, index)
	    new_exp.column_name = int(new_exp.column_name)
	    new_exp.column_name = self.column_type
	return new_exp

    def genJoinKey(self):
	return None

    def hasGroupbyFunc(self):
	return False

    def compare(self, exp):
	if isinstance(exp, Column) and self.column_type == exp.column_type and self.column_name == exp.column_name and self.table_name == exp.table_name:
	    return True
	return False

    def selectListFilter(self, select_list, node, new_dic, new_list):
	print node.table_list, node.table_alias_dic
	for table in node.table_list:
	    if table == self.table_name:
		if util.isColumnInTable(self.column_name, table):
		    flag = False
		    for exp in new_list:
			if exp.compare(self):
			    flag = True
			    break
		    if flag is False:
			new_exp = copy.deepcopy(self)
			new_list.append(new_exp)
			new_dic[new_exp] = select_list.exp_alias_dic[self]
		    break

	for table in node.table_alias_dic.keys():
	    if table == self.table_name:
		if util.isColumnInTable(self.column_name, node.table_alias_dic[table]):
		    flag = False
		    for exp in new_list:
			if exp.compare(self):
			    flag = True
			    break
		    if flag is False:
			new_exp = copy.deepcopy(self)
			new_list.append(new_exp)
			new_dic[new_exp] = select_list.exp_alias_dic[self]

class Constant(Expression):
    const_type = None
    const_value = None	

    def __init__(self, const_value, const_type):
	self.const_type = const_type
	self.const_value = const_value

    def getExpType(self):
	return "CONST"

    def evaluate(self):
	return str(self.const_value)

    def setFuncObj(self, func_obj):
	return

    def genJoinKey(self):
	return None

    def compare(self, exp):
	if isinstance(exp, Constant) and self.const_type == exp.const_type and self.const_value == exp.const_value:
	    return True
	return False
	
    def hasGroupbyFunc(self):
	return False

    def selectListFilter(self, select_list, node, new_dic, new_list):
	pass

class ExpressionParser:
    def __init__(self):
	pass

    def parse(self, token_list):
	length = len(token_list)
	if length == 1:
	    token = token_list[0]
	    t_content = token["content"]
	    t_name = token["name"]
	    if t_name == "ID":
		return Column("", t_content)
	    elif t_name == "ASTERISK":
		return Column("", "*")
	    elif t_name == "NUMBER":
		return Constant(t_content, "DECIMAL")
	    elif t_name == "QUOTED_STRING":
		t_content = t_content.strip("'")
		t_content = "\"" + t_content + "\""
		return Constant(t_content, "TEXT")
	    elif t_name == "RESERVED" and t_content.upper() == "NULL":
		return Constant(t_content, "NULL")
	
	if length == 3:
	    first = token_list[0]
	    second = token_list[1]
	    third = token_list[2]
	    if first["name"] == "ID" and second["name"] == "DOT" and third["name"] == "ID":
		return Column(first["content"], third["content"])
	    if first["name"] == "ID" and second["name"] == "DOT" and third["name"] == "ASTERISK":
		return Column(first["content"], "*")
	    if first["name"] == "ID" and second["name"] == "LPAREN" and third["name"] == "RPAREN":
		return Function(first["name"], [])
			
	# boolean exp: <, >, <>, like, is
	sub_level = 0
	lt_index = -1
	gt_index = -1
	eq_index = -1
	like_index = -1
	is_index = -1
	partition_index = -1
	partition_type = None
	for i in range(0, length):
	    token = token_list[i]
	    t_content = token["content"]
	    t_name = token["name"]
	    if t_name == "LPAREN":
		sub_level += 1
	    elif t_name == "RPAREN":
		sub_level -= 1
	
	    if sub_level == 0:
		if t_name == "RESERVED" and t_content == "LIKE":
		    partition_index = i
		    partition_type = "LIKE"
		elif t_name == "RESERVED" and t_content == "IS":
		    partition_index = i
		    partition_type = "IS"
		elif t_name in ["EQ", "GT", "LT", "NEQ", "GE", "LE"]:
		    partition_index = i
		    partition_type = t_name
				
	if partition_index > 0:
	    func_name = partition_type
	    before_list = token_list[:partition_index]
	    after_list = token_list[(partition_index + 1):]
	    before_exp = self.parse(before_list)
	    end_exp = self.parse(after_list)
	    return Function(func_name, [before_exp, end_exp])
	
	# plus or minus
	sub_level = 0
	plus_index = -1
	minus_index = -1
	for i in range(0, length):
	    token = token_list[i]
	    t_content = token["content"]
	    t_name = token["name"]
	    if t_name == "LPAREN":
		sub_level += 1
	    elif t_name == "RPAREN":
		sub_level -= 1
	
	    if sub_level == 0:
		if t_name == "PLUS":
		    plus_index = i
		elif t_name == "MINUS":
		    minus_index = i
		
	if plus_index > 0:
	    before_list = token_list[:plus_index]
	    after_list = token_list[(plus_index + 1):]
	    before_exp = self.parse(before_list)
	    end_exp = self.parse(after_list)
	    return Function("PLUS", [before_exp, end_exp])
	elif minus_index > 0:
	    before_list = token_list[:minus_index]
	    after_list = token_list[(minus_index + 1):]
	    before_exp = self.parse(before_list)
	    end_exp = self.parse(after_list)
	    return Function("MINUS", [before_exp, end_exp])
	
	# multiply or divide
	sub_level = 0
	mul_index = -1
	div_index = -1
	for i in range(0, length):
	    token = token_list[i]
	    t_content = token["content"]
	    t_name = token["name"]
	    if t_name == "LPAREN":
		sub_level += 1
	    elif t_name == "RPAREN":
		sub_level -= 1
	
	    if sub_level == 0:
		if t_name == "ASTERISK":
		    mul_index = i
		elif t_name == "DIVIDE":
		    div_index = i
	
	if mul_index > 0:
	    before_list = token_list[:mul_index]
	    after_list = token_list[(mul_index + 1):]
	    before_exp = self.parse(before_list)
	    end_exp = self.parse(after_list)
	    return Function("MULTIPLY", [before_exp, end_exp])
	elif div_index > 0:
	    before_list = token_list[:div_index]
	    after_list = token_list[(div_index + 1):]
	    before_exp = self.parse(before_list)
	    end_exp = self.parse(after_list)
	    return Function("DIVIDE", [before_exp, end_exp])
		
	# func() or ()
	first = token_list[0]
	t_name = first["name"]
	if t_name == "ID" or t_name in ["'SUM'", "'AVG'", "'MAX'", "'MIN'", "'COUNT'"]:
	    func_name = first["content"]
	    second = token_list[1]
	    last = token_list[-1]
	    if second["name"] != "LPAREN" or last["name"] != "RPAREN":
		# TODO error
		pass
	    sub_level = 0
	    para_list = []
	    tmp_para_list = []
	    for i in range(2, length - 1):
		token = token_list[i]
		para_list.append(token)
		t_content = token["content"]
		t_name = token["name"]
		if t_name == "LPAREN":
		    sub_level += 1
		elif t_name == "RPAREN":
		    sub_level -= 1
		
		if sub_level == 0 and t_name == "COMMA":
		    para_list = para_list[:-1]
		    tmp_para_list.append(copy.deepcopy(para_list))
		    para_list = []
	    tmp_para_list.append(para_list)
	
	    exp_list = []
	    for para in tmp_para_list:
		exp_list.append(self.parse(para))
	    return Function(func_name, exp_list)
		
	elif first["name"] == "LPAREN":
	    last = token_list[-1]
	    if last["name"] == "RPAREN":
		return self.parse(token_list[1:-1])
	    else:
		# TODO error
		pass
	
	else:
	    # TODO error
	    pass
