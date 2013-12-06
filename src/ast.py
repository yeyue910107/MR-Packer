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
import expression
import util
import node
import schema
import copy
from xml.dom import minidom, Node

class ASTreeNode:
    line_num = 0
    position_in_line = 0
    token_name = ""
    child_num = 0
    token_type = 0
    content = None
    child_list = None

    def __init__(self, line_num, position_in_line, token_name, child_num, token_type, content):
	self.line_num = line_num
	self.position_in_line = position_in_line
	self.token_name = token_name
	self.child_num = child_num
	self.token_type = token_type
	self.content = content
	self.child_list = []
	
    def appendChild(self, child):
	self.child_list.append(child)
	
    def toRootSelectNode(self):
	rs_node = node.RootSelectNode()
	
	if self.token_name != "T_SELECT":
	    # TODO error
            pass
	
	for child in self.child_list:

	    if child.token_name == "T_COLUMN_LIST":
		rs_node.select_list = SelectListParser(child)
	    elif child.token_name == "T_WHERE":
		rs_node.where_condition = WhereConditionParser(child)
	    elif child.token_name == "T_HAVING":
		rs_node.having_clause = WhereConditionParser(child)
	    elif child.token_name == "T_GROUP_BY":
		rs_node.groupby_clause = GroupbyParser(child)
	    elif child.token_name == "T_ORDER_BY":
		rs_node.orderby_clause = OrderbyParser(child)
	    elif child.token_name == "T_FROM":
		tmp_list = []
		for child_child in child.child_list:
		    if child_child.token_name != "T_SELECT":
			tmp_list.append(child_child)
		    else:
			tmp_list.append(child_child.toRootSelectNode())
		rs_node.from_list = tmp_list
	
	return rs_node
	
    def toPlanTree(self):
	rs_node = self.toRootSelectNode()
	rs_node.processFromList()
	pt = rs_node.toInitialQueryPlanTree()
	#DEBUG
	#pt.__print__()
	pt = pt.genOrderby()
	pt = pt.genGroupby()
	pt = pt.toBinaryJoinTree()
	return pt
	
class ASTreeNodeParser:
    source = None
    real_struct = None
    converted_str = None
    converted_exp_str = None

    def __init__(self):
	pass

    def convertItemToExp(self, item):
	pass
	
    def convertItemToExpList(self, item):
	pass

    def convertItemListToExpList(self, item_list):
	pass
	
    def convertItemToStr(self, item):
	pass

    def convertItemListToStr(self, item_list):
	pass

    def convertExpListToStr(self, exp):
	pass

class SelectListParser(ASTreeNodeParser):
    exp_list = None
    exp_alias_dic = None

    def __init__(self, select_list):
	if select_list is None:
	    self.exp_list = []
	    self.exp_alias_dic = {}
	self.source = select_list
	self.converted_str = self.convertItemListToStr(self.source)
	self.exp_alias_dic = {}
	self.exp_list = self.convertItemListToExpList(self.source)
	self.converted_exp_str = self.convertExpListToStr(self.exp_list)
	self.real_struct = None

    def convertItemListToExpList(self, select_list):
	ret_exp_list = []
	
	if select_list is None:
	    return ret_exp_list
	
	exp_parser = expression.ExpressionParser()
	for child in select_list.child_list:
	    if child.token_name == "T_SELECT_COLUMN":
		input_exp_list = []
		as_alias = None
		as_flag = False
	
		for grand_child in child.child_list:
		    token = {}
		    token["name"] = grand_child.token_name
		    token["content"] = grand_child.content
		
		    fin_flag = True
		    if as_flag == True:
			fin_flag = False
			as_alias = token["content"]
			continue
		    if token["name"] == "T_RESERVED" and token["content"] == "AS":
			as_flag = True
			fin_flag = False
		    if fin_flag == True:
			input_exp_list.append(token)
		exp = exp_parser.parse(input_exp_list)
		# setup alias
		# explicit: a AS b
		if as_alias is not None:
		    self.exp_alias_dic[exp] = as_alias
		
		# implicit: a, or a.b
		elif isinstance(exp, expression.Column):
		    self.exp_alias_dic[exp] = exp.column_name
		
		# no alias
		else:
		    self.exp_alias_dic[exp] = None
		ret_exp_list.append(exp)
	return ret_exp_list
			
    def convertItemListToStr(self, select_list):
	ret_str = ""
	if select_list is None:
	    return ret_str
		
	for child in select_list.child_list:
	    if child.token_name == "T_SELECT_COLUMN":
		tmp_str = ""
		for child in child.child_list:
			tmp_str += str(child.content)
		ret_str += tmp_str
		
	    elif child.token_name == "COMMA":
		ret_str += ", "
	return ret_str
	
    def convertExpListToStr(self, exp_list):
	ret_str = ""
	if exp_list is None:
	    return ret_str
	
	for exp in exp_list:
	    alias = str(self.exp_alias_dic[exp])
	    tmp_str = exp.evaluate()
	    ret_str += "[" + tmp_str + "]:" + alias + ", "
	
	return ret_str[:-2]
		
    def __print__(self):
	print "select_list:"
	print "exp_list: "
	util.printExpList(self.exp_list)
	print "exp_alias_dic: "
	util.printExpAliasDic(self.exp_alias_dic)
	return

class OnConditionParser(ASTreeNodeParser):
    on_condition_exp = None

    def __init__(self, on_condition):
	self.source = on_condition
	self.real_struct = None
	self.converted_str = self.convertItemListToStr(self.source)
	where_condition_parser = WhereConditionParser(None)
	self.on_condition_exp = where_condition_parser.convertItemListToExpList(self.source)
	self.converted_exp_str = where_condition_parser.convertExpListToStr(self.on_condition_exp)

    def convertItemToStr(self, item):
	if item.token_name == "T_COND_OR" or item.token_name == "T_COND_AND":
	    ret_str = ""
	    for child in item.child_list:
		ret_str += self.convertItemToStr(child)
		ret_str += " "
	    return ret_str
	else:
	    if item.token_name == "GTH":
		return ">"
	    elif item.token_name == "LTH":
		return "<"
	    elif item.token_name == "NOT_EQ":
		return "<>"
	    else:
		return item.content

    def convertItemListToStr(self, on_condition):
	ret_str = ""
	if on_condition is None:
	    return ret_str
	if len(on_condition) == 1:
	    return self.convertItemListToStr(on_condition[0])
	else:
	    for item in on_condition:
		ret_str += self.convertItemToStr(item)
		ret_str += ""
	    return ret_str

    def __print__(self):
	print "on_condition: ", self.on_condition_exp.evaluate()

class WhereConditionParser(ASTreeNodeParser):
    where_condition_exp = None

    def __init__(self, where_condition):
	self.source = where_condition
	self.real_struct = None
	self.converted_str = self.convertItemListToStr(self.source)
	self.where_condition_exp = self.convertItemListToExpList(self.source)
	self.converted_exp_str = self.convertExpListToStr(self.where_condition_exp)

    def convertItemToStr(self, item):
	if item.token_name == "T_COND_OR" or item.token_name == "T_COND_AND":
	    ret_str = ""
	    for child in item.child_list:
		ret_str += self.convertItemToStr(child)
		ret_str += ""
	    return ret_str
	else:
	    if item.token_name == "GTH":
		return ">"
	    elif item.token_name == "LTH":
		return "<"
	    elif item.token_name == "NOT_EQ":
		return "<>"
	    elif item.token_name == "GEQ":
		return ">="
	    elif item.token_name == "LEQ":
		return "<="
	    else:
		return item.content

    def convertItemListToStr(self, where_condition):
	ret_str = ""
	if where_condition is None:
	    return ret_str
	tmp_list = where_condition.child_list[1:]
	if len(tmp_list) == 1:
	    return self.convertItemToStr(tmp_list[0])
	else:
	    for item in tmp_list:
		ret_str += self.convertItemToStr(item)
		ret_str += ""
	    return ret_str
		
    def convertItemListToExpList(self, where_condition):
	if where_condition is None:
	    return None
	if isinstance(where_condition, ASTreeNode):
	    tmp_list = where_condition.child_list[1:]
	else:
	    tmp_list = where_condition
	if len(tmp_list) == 1:
	    item = tmp_list[0]
	    if item.token_name != "T_COND_OR" and item.token_name != "T_COND_AND":
		# TODO error
                pass
	    func_name = item.token_name[7:]
	    child_list = item.child_list
	    first_child = child_list[0]
	    if first_child.token_name == "T_COND_OR" or first_child.token_name == "T_COND_AND":
		exp_list = []
		for child in child_list:
		    exp_list.append(self.convertItemListToExpList([child]))
		return expression.Function(func_name, exp_list)
	    elif first_child.token_name == "T_RESERVED" and (first_child.content == "AND" or first_child.content == "OR"):
		return self.convertItemListToExpList(child_list[1:])
	    else:
		return self.convertItemListToExpList(child_list)
	
	else:
	    # cases like ( COND_AND )
	    if len(tmp_list) == 3:
		if tmp_list[0].token_name == "LPAREN" and tmp_list[2].token_name == "RPAREN" and (tmp_list[1].token_name == "T_COND_AND" or tmp_list[1].token_name == "T_COND_OR"):
		    return self.convertItemListToExpList([tmp_list[1]])
	    exp_parser = expression.ExpressionParser()
	    token_list = []
	    for child in tmp_list:
		token = {}
		token["name"] = child.token_name
		token["content"] = child.content
		token_list.append(token)
	    return exp_parser.parse(token_list)

    def convertExpListToStr(self, exp):
	if exp is None:
	    return None
	return exp.evaluate()

    def __print__(self):
	print "where_condition: ", self.where_condition_exp.evaluate()

class GroupbyParser(ASTreeNodeParser):
    groupby_list = None

    def __init__(self, groupby):
	self.source = groupby
	self.real_struct = None
	self.converted_str = self.convertItemListToStr(self.source)
	self.groupby_list = self.convertItemListToExpList(self.source)
	self.converted_exp_str = self.convertExpListToStr(self.groupby_list)
	
    def convertItemListToStr(self, groupby):
	ret_str = ""
	if groupby is None:
	    return ret_str
	for child in groupby.child_list:
	    ret_str += child.content
	    if child.token_name == "T_RESERVED":
		ret_str += " "
	
	return ret_str

    def convertItemListToExpList(self, groupby):
	if groupby is None:
	    return []
	groupby_list = groupby.child_list[2:]
	groupby_token_list = []
	groupby_item = []
	for item in groupby_list:
	    if item.token_name != "COMMA":
		token = {}
		token["name"] = item.token_name
		token["content"] = item.content
		groupby_item.append(token)
	    else:
		tmp = []
		for i in groupby_item:
		    tmp.append(i)
		groupby_token_list.append(tmp)
		groupby_item = []
	groupby_token_list.append(groupby_item)
	
	ret_exp_list = []
	exp_parser = expression.ExpressionParser()
	for item in groupby_token_list:
	    ret_exp_list.append(exp_parser.parse(item))
	
	return ret_exp_list
	
    def convertExpListToStr(self, exp_list):
	ret_str = ""
	if exp_list is None:
	    return ret_str
	for exp in exp_list:
	    ret_str += "(" + exp.evaluate() + ")" + ", "
	
	return ret_str[:-2]

    def __print__(self):
	print "groupby_list: "
	util.printExpList(self.groupby_list)

class OrderbyParser(ASTreeNodeParser):
    orderby_exp_list = None
    orderby_type_list = None

    def __init__(self, orderby):
	self.source = orderby
	self.orderby_type_list = []
	self.orderby_exp_list = self.convertItemListToExpList(self.source)
	
    def convertItemListToExpList(self, orderby):
	if orderby is None:
	    return []
	orderby_list = orderby.child_list[2:]
	orderby_token_list = []
	orderby_item = []
	for item in orderby_list:
	    if item.token_name != "COMMA":
		token = {}
		token["name"] = item.token_name
		token["content"] = item.content
		orderby_item.append(token)
	    else:
		tmp = []
		for i in orderby_item:
		    tmp.append(i)
		orderby_token_list.append(tmp)
		orderby_item = []
	orderby_token_list.append(orderby_item)
	
	ret_exp_list = []
	tmp_orderby_type_list = []
	exp_parser = ExpressionParser()
	for item in orderby_token_list:
	# orderby type: DESC or ASC (default is ASC)
	    orderby_type = None
	    if item[-1]["name"] == "RESERVED":
		orderby_type = item[-1]["content"]
		item = item[:-1]
	    else:
		orderby_type = "ASC"
	    tmp_orderby_type_list.append(orderby_type)
	    ret_exp_list.append(exp_parser.convertTokenListToExp(item))
	
	self.orderby_type_list = tmp_orderby_type_list
	return ret_exp_list

    def __print__(self):
	print "orderby_list: "
	util.printExpList(self.orderby_list)

def processSchemaFile(_schema):
	global global_table_dic
	file = open(_schema)
	text = file.readlines()
	file.close()
	
	for line in text:
	    line = line.rstrip()
	    token = line.split("|")
	    table_name = token[0].upper()
	    tmp_list = []
	    for col in token[1:]:
		if ":" in col:
		    col_name = col.split(":")[0].upper()
		    col_type = col.split(":")[1].upper()
		    if col_type not in ["INTEGER", "DECIMAL", "TEXT", "DATE"]:
			# TODO error
                        pass
		    column = schema.ColumnSchema(col_name, col_type)
		    tmp_list.append(column)
	    table = schema.TableSchema(table_name, tmp_list)
	    util.global_table_dic[table_name] = table

def fileToRoot(file):
    doc = minidom.parse(file)
    node = doc.documentElement
    root = None
    for child in node.childNodes:
	if child.nodeName == "node":
	    root = nodeToAST(child)
	    break
    return root
	
def nodeToAST(node):
    ast = None
    if node.nodeType == Node.ELEMENT_NODE:
	if node.nodeName == "node":
	    line_num = node.attributes.get('line').value
	    position_in_line = node.attributes.get('positioninline').value
	    token_name = node.attributes.get('tokenname').value
	    child_num = node.attributes.get('childcount').value
	    token_type = node.attributes.get('tokentype').value
		
	for child in node.childNodes:
	    if child.nodeName == "content":
		if token_type == "69":
		    content = child.childNodes[0].nodeValue
		else:
		    content = child.childNodes[0].nodeValue.upper()
	ast = ASTreeNode(line_num, position_in_line, token_name, child_num, token_type, content)
	for child in node.childNodes:
	    if child.nodeName != "content":
		child_ast = nodeToAST(child)
		if child_ast is not None:
		   ast.appendChild(child_ast)

    return ast

def astToQueryPlan(schema, file):
    processSchemaFile(schema)
    root = fileToRoot(file)
    pt = root.toPlanTree()
    #DEBUG
    #pt.__print__()
    pt.postProcess()
    pt.__print__()
    return pt
