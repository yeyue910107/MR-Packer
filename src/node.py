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
import ast
import copy
import schema
import op

op_id = 0

class Node(object):
    source = None
    select_list = None
    where_condition = None
    #join_condition = None
    groupby_clause = None
    having_clause = None
    orderby_clause = None
    table_list = []
    table_alias_dic = {}
    is_handler = None
    pk = None

    def __init__(self, name="node"):
        self.table_list = []
	self.table_alias_dic = {}

    def genGroupby(self):
        # explicit groupby
	gb = GroupbyNode()
	gb.source = self.source
	gb.is_handler = self.is_handler
	gb.select_list = copy.deepcopy(self.select_list)
	#self.groupby_clause = None
        if self.groupby_clause is not None:
            gb.groupby_clause = self.groupby_clause
            if self.having_clause is not None:
                gb.having_clause = self.having_clause
                self.having_clause = None

        # implicit groupby, with avg function
        else:
            if self.select_list is None:
                return self
            select_dic = self.select_list.exp_alias_dic
	    flag = False
	    for key in select_dic.keys():
		flag |= key.hasGroupbyFunc()
	    if flag is False:
		return self
	    gb.groupby_clause = ast.GroupbyParser(None)
	    gb.groupby_clause.groupby_list.append(expression.Constant(1, "INTEGER"))
		
	for table in self.table_list:
	    if table not in gb.table_list:
		gb.table_list.append(table)
	gb.table_alias_dic = self.table_alias_dic
	gb.child = self
	self.parent = gb
	return gb

    def genOrderby(self):
        if self.orderby_clause is not None:
            ob = OrderbyNode()
            ob.source = self.source
	    ob.is_handler = self.is_handler
            ob.select_list = None
            ob.orderby_clause = self.orderby_clause
            self.orderby_clause = None

            for table in self.table_list:
                ob.table_list.append(table)
            for table in self.table_alias_dic.keys():
                ob.table_alias.dic[table] = self.table_alias_dic[table]
            return ob
        else:
            return self

    def toBinaryJoinTree(self):
        return self
	
    ### post-processing
    def genProjectList(self):
	pass
		
    def checkSchema(self):
	pass
		
    def genTableName(self):
	if self.select_list is not None:
	    tmp_dic = self.select_list.exp_alias_dic
	    for exp in tmp_dic.keys():
		exp.genTableName(self)
		
	if self.where_condition is not None:
	    self.where_condition.where_condition_exp.genTableName(self)
	
    def predicatePushdown(self):
	pass

    def columnFilter(self):
	pass
		
    def processSelectStar(self):
	if self.select_list is not None:
	    select_dic = self.select_list.exp_alias_dic
	    exp_list = self.select_list.exp_list
	    new_select_dic = {}
	    new_exp_list = []
	    for exp in exp_list:
		if isinstance(exp, expression.Column):
		    if exp.column_name != "*":
			new_exp = copy.deepcopy(exp)
			new_exp_list.append(new_exp)
			new_select_dic[new_exp] = select_dic[exp]
		    else:
			for item in self.table_list:
			    table = util.searchTable(item)
			    if table is not None:
			        for col in table.column_list:
				    tmp_exp = expression.Column(item, col.column_name)
				    new_exp_list.append(tmp_exp)
				    new_select_dic[tmp_exp] = None
			for key in self.table_alias_dic.keys():
			    table = util.searchTable(self.table_alias_dic[key])
			    if table is not None:
				for col in table.column_list:
				    tmp_exp = expression.Column(item, col.column_name)
				    new_exp_list.append(tmp_exp)
				    new_select_dic[tmp_exp] = None
		elif isinstance(exp, expression.Function):
		    if exp.getGroupbyFuncName() == "COUNT":
			col_list = []
			exp.getPara(col_list)
			for col in col_list:
			    if col.column_name == "*":
				func_obj = col.func_obj
				new_con = expression.Constant(1, "INTEGER")
				func_obj.replace(col, new_con)
			new_exp_list.append(exp)
			new_select_dic[exp] = select_dic[exp]
		    else:
			new_exp = copy.deepcopy(exp)
			new_exp_list.append(new_exp)
			new_select_dic[new_exp] = select_dic[exp]
		else:
		    new_exp = copy.deepcopy(exp)
		    new_exp_list.append(new_exp)
		    new_select_dic[new_exp] = select_dic[exp]
	    self.select_list.exp_alias_dic = new_select_dic
	    self.select_list.exp_list = new_exp_list
		
    def genColumnIndex(self):
	pass
		
    def __genSelectIndex__(self):
	if self.select_list is None:
	    return
	new_select_dic = {}
	new_exp_list = []
	exp_dic = self.select_list.exp_alias_dic
	exp_list = self.select_list.exp_list
	
	for exp in exp_list:
	    new_exp = exp.genIndex(self)
	    new_exp_list.append(new_exp)
	    if isinstance(exp, expression.Column):
		new_select_dic[new_exp] = self.select_list.exp_alias_dic[exp]
	    else:
		new_select_dic[new_exp] = None
	
	self.select_list.exp_alias_dic = new_select_dic
	self.select_list.exp_list = new_exp_list
	
    def __genWhereIndex__(self):
	if self.where_condition is None:
	    return
	if isinstance(self.where_condition.where_condition_exp, expression.Function):
	    self.where_condition.where_condition_exp = self.where_condition.where_condition_exp.genIndex(self)

    def toOp(self):
	pass

    def getPartitionKey(self, to_origin=True):
	pass

    def postProcess(self):
	#pass
	self.genProjectList()
	self.__print__()
	#if self.checkSchema() is False:
	    # TODO error
	    # pass
	self.processSelectStar()
	#print "gen table name"
	self.genTableName()
	#print "predicate"
	self.predicatePushdown()
	#print "column filter"
	self.columnFilter()
	self.genTableName()
	self.genColumnIndex() 

    def __getOriginalExp__(self, exp, flag):
	pass

    def __print__(self):
	if self.select_list is not None:
	    self.select_list.__print__()
	if self.where_condition is not None:
	    self.where_condition.__print__()
	print "table_list: ", self.table_list, "table_alias_dic: ", self.table_alias_dic
	
class SPNode(Node):
    child = None
    parent = None
    composite = None
    table_alias = None
    in_table_list = []
    in_table_alias_dic = {}

    def __init__(self, name="sp"):
	self.name = name
        super(SPNode, self).__init__()

    def genGroupby(self):
        self.child = self.child.genGroupby()
        return super(SPNode, self).genGroupby()

    def genOrderby(self):
        self.child = self.child.genOrderby()
        return super(SPNode, self).genOrderby()

    def toBinaryJoinTree(self):
        self.child = self.child.toBinaryJoinTree()
        self.child.parent = self
        return super(SPNode, self).toBinaryJoinTree()
		
    def genProjectList(self):
	self.child.genProjectList()
	
	for item in self.child.table_list:
	    if item not in self.in_table_list:
		self.in_table_list.append(item)
	self.in_table_alias_dic = self.child.table_alias_dic
	
	project_list = []
	self.__genProjectList__(project_list)
	tmp_name = self.table_alias
	if tmp_name is not None:
	    if tmp_name in util.global_table_dic.keys():
	        while tmp_name in util.global_table_dic.keys():
		    tmp_name = tmp_name + "_1"
	    tmp_schema = schema.TableSchema(tmp_name, project_list)
	    util.global_table_dic[tmp_name] = tmp_schema
		
    def __genProjectList__(self, project_list):
	if self.child.select_list is None:
	    return
	select_list = self.child.select_list
	table_list = self.in_table_list
	table_alias_dic = self.in_table_alias_dic
	exp_dic = select_list.exp_alias_dic
		
	for exp in select_list.exp_list:
	    col_type = None
	    if isinstance(exp, expression.Column):
		if exp.column_name == "*":
		    __addTableList__(table_list, project_list)
		    __addTableList__(table_alias_dic.values, project_list)
		else:
		    col_list = util.searchColumn(exp.column_name)
		    for col in col_list:
			if col.table_schema.table_name in table_list or col.table_schema.table_name in table_alias_dic.values():
			    col_type = col.table_schema.getColumnByName(exp.column_name).column_type
		    if col_type is not None:
			if exp_dic[exp] is not None:
			    tmp_col = schema.ColumnSchema(exp_dic[exp], col_type)
			else:
			    tmp_col = schema.ColumnSchema(exp.column_name, col_type)
			project_list.append(tmp_col)
	    elif isinstance(exp, expression.Function):
		if exp_dic[exp] is not None:
		    col_type = exp.getValueType()
		    tmp_col = schema.ColumnSchema(exp_dic[exp], col_type)
		    project_list.append(tmp_col)
	    else:
		# TODO error
		pass
	
    def __addTableList__(table_list, project_list):
	for table in table_list:
	    tmp_table = searchTable(table)
	    if tmp_table is not None:
		for col in tmp_table.column_list:
		    tmp_col = copy.deepcopy(col)
		    project_list.append(tmp_col)
	
    def checkSchema(self):
	schema_checker = SchemaChecker(self)
	if schema_checker.checkSelectList(self) and schema_checker.checkWhere(self) and schema_checker.checkGroupby(self) and self.child.checkSchema():
	    return True
	return False
		
    def genTableName(self):
	super(SPNode, self).genTableName()
	self.child.genTableName()

    def predicatePushdown(self):
	print "spnode, predicate"
	'''
	new_exp = None
	print self.select_list.exp_list, self.child.select_list
	if self.where_condition is not None:
	    tmp_exp = copy.deepcopy(self.where_condition.where_condition_exp)
	    print "tmp_exp", tmp_exp.evaluate()
	    if tmp_exp is not None:
		col_list = []
		tmp_exp.getPara(col_list)
		select_dic = self.child.select_list.exp_alias_dic
		for item in col_list:
		    flag = False
		    for key in select_dic.keys():
			if item.column_name == select_dic[key]:
			    flag = True
			    item.table_name = ""
			    if isinstance(key, expression.Column):
				item.column_name = key.column_name
				item.table_name = key.table_name
			    else:
				tmp_func = item.func_obj
				new_para_list = []
				for para in tmp_func.para_list:
				    if not isinstance(para, expression.Column):
					new_para_list.append(para)
					continue
				    if para.column_name == item.column_name:
					new_para = copy.deepcopy(key)
					new_para_list.append(new_para)
				    else:
					new_para_list.append(para)
				tmp_func.setParaList(new_para_list)
			    break
						
			if isinstance(key, expression.Column):
			    if item.column_name == key.column_name:
				flag = True
				item.table_name = key.table_name
				break
					
			elif isinstance(key, expression.Function):
			    if item.column_name == select_dic[key]:
				flag = True
				item.table_name = ""
				break
		    if flag is False:
			# TODO error
			pass
		
		child_exp = tmp_exp.booleanFilter(self, True)
		if child_exp is not None:
		    print "child_exp", child_exp.evaluate()
		    if self.child.where_condition is None:
			self.child.where_condition = ast.WhereConditionParser(None)
			self.child.where_condition.where_condition_exp = copy.deepcopy(child_exp)
		    else:
			new_exp = copy.deepcopy(child_exp)
			para_list = []
			para_list.append(self.child.where_condition.where_condition_exp)
			para_list.append(new_exp)
			self.child.where_condition.where_condition_exp = expression.Function("AND", para_list)
	self.where_condition = None'''
	self.child.predicatePushdown()

    def updateSPList(self):
	if isinstance(self.parent, JoinNode):
	    if self.is_explicit:
		join_exp = self.parent.join_condition.on_condition_exp
	    else:
		join_exp = self.parent.join_condition.where_condition_exp
	    where_exp = self.parent.where_condition.where_condition_exp
	

    def columnFilter(self):
	new_select_dic = {}
	new_exp_list = []
	# if child is table node
	if isinstance(self.child, TableNode):
	    # TODO error
	    return
        print self.child
	child_exp_list = self.child.select_list.exp_list
	child_exp_dic = self.child.select_list.exp_alias_dic
	
	for exp in self.select_list.exp_list:
	    if isinstance(exp, expression.Column):
		for child_exp in child_exp_list:
		    if isinstance(child_exp, expression.Column):
			if child_exp_dic[child_exp] == exp.column_name and child_exp not in new_exp_list:
			    new_exp_list.append(child_exp)
			    new_select_dic[child_exp] = exp.column_name
			    break
			if child_exp.column_name == exp.column_name and child_exp not in new_exp_list:
			    new_exp_list.append(child_exp)
			    new_select_dic[child_exp] = None
			    break
		    elif isinstance(child_exp, expression.Function) and child_exp_dic[child_exp] == exp.column_name and child_exp not in new_exp_list:
			new_exp_list.append(child_exp)
			new_select_dic[child_exp] = None
			break
	    elif isinstance(exp, expression.Function):
		col_list = []
		exp.getPara(col_list)
		for col in col_list:
		    col.table_name = ""
	    else:
		new_exp_list.append(exp)
		new_select_dic[exp] = None
	
	self.child.select_list.exp_list = new_exp_list
	self.child.select_list.exp_alias_dic = new_select_dic
	print "22222sp column filter"
	util.printExpList(self.child.select_list.exp_list)
	self.child.columnFilter()
	
    def processSelectStar(self):
	super(SPNode, self).processSelectStar()
	self.child.processSelectStar()
	
    def genColumnIndex(self):
	self.__genSelectIndex__()
	self.__genWhereIndex__()
	self.child.genColumnIndex()

    def getMapOutput(self, table_name):
	'''if isinstance(self.child, TableNode):
	    return copy.deepcopy(self.select_list.exp_list)
	else:
	    return self.child.getMapOutput(table_name)'''
	return copy.deepcopy(self.select_list.exp_list)

    def getMapFilter(self, table_name):
	'''if isinstance(self.child, TableNode):
	    return self.where_condition
	else:
	    return self.child.getMapFilter(table_name)'''
	return self.where_condition

    def toOp(self):
	global op_id
	op_id = op_id + 1
	ret_op = op.SpjOp()
	ret_op.id = [op_id]
	ret_op.is_sp = True
	ret_op.node_list.append(self)
	ret_op.map_phase.append(self)
	ret_op.pk_list = self.getPartitionKey()
	# DEBUG
	print "SPNode, PK_LIST:"
	for pk in ret_op.pk_list:
	    util.printExpList(pk)
	return ret_op

    def getPartitionKey(self, to_origin=True):
	return []

    def __getOriginalExp__(self, exp, flag):
	if not isinstance(exp, expression.Column):
	    return exp
	if isinstance(self.child, TableNode):
	    return self.child.__getOriginalExp__(exp, flag)
	index = exp.column_name
        tmp_exp = self.child.select_list.exp_list[index]
        new_exp = copy.deepcopy(self.child.__getOriginalExp__(tmp_exp, False))
        return new_exp

    def __print__(self):
	print "SPNode:----------"
	super(SPNode, self).__print__()
	#print "table_list: ", self.table_list, "table_alias_dic: ", self.table_alias_dic
	if self.child is not None:
	    self.child.__print__()

class TableNode(Node):
    parent = None
    composite = None
    table_name = None
    table_alias = None

    def __init__(self, name="table"):
	self.name = name
        super(TableNode, self).__init__()
		
    def checkSchema(self):
	schema_checker = SchemaChecker(self)
	if searchTable(self.table_name) is not None and schema_checker.checkSelectList(self) and schema_checker.checkWhere(self):
	    return True
	return False
	
    def genColumnIndex(self):
	self.__genSelectIndex__()
	self.__genWhereIndex__()

    def toOp(self):
	return None

    def getPartitionKey(self, to_origin=True):
	return []

    def __getOriginalExp__(self, exp, flag):
	if not isinstance(exp, expression.Column):
	    return exp
	new_exp = copy.deepcopy(exp)
        new_exp.table_name = self.table_name
        return new_exp

    def __print__(self):
	print "TableNode:----------"
	super(TableNode, self).__print__()
	print "table_name: ", self.table_name, "table_alias: ", self.table_alias

class GroupbyNode(Node):
    child = None
    parent = None
    composite = None

    def __init__(self, name="groupby"):
	self.name = name
        super(GroupbyNode, self).__init__()

    def toBinaryJoinTree(self):
        self.child = self.child.toBinaryJoinTree()
        self.child.parent = self
        return super(GroupbyNode, self).toBinaryJoinTree()
		
    def genProjectList(self):
	self.child.genProjectList()

    def checkSchema(self):
	schema_checker = schema.SchemaChecker(self)
	if schema_checker.checkSelectList() and schema_checker.checkWhere() and schema_checker.checkGroupby() and (self.having_clause is not None and schema_checker.checkHaving()) and self.child.checkSchema():
	    return True
	return False
	
    def genTableName(self):
	super(GroupbyNode, self).genTableName()
	if self.groupby_clause is not None:
	    for exp in self.groupby_clause.groupby_list:
		exp.genTableName(self)
	self.child.genTableName()

    def predicatePushdown(self):
	print "groupby, predicate"
	print self.child.select_list
	if self.where_condition is not None:
	    new_where = self.where_condition.where_condition_exp.groupbyWhereFilter()
	    if new_where is not None:
		if self.child.where_condition is not None:
		    old_exp = self.child.where_condition.where_condition_exp
		    para_list = []
		    para_list.append(old_exp)
		    para_list.append(new_where)
		    new_exp = expression.Function("AND", para_list)
		else:
		    self.child.where_condition = WhereConditionParser(None)
		    new_exp = new_where
		self.child.where_condition.where_condition_exp = copy.deepcopy(new_exp)
		
	    if self.where_condition.where_condition_exp.hasGroupbyFunc() is False:
		self.where_condition = None
	
	if self.having_clause is not None:
	    if self.where_condition is None:
		self.where_condition = WhereConditionParser(None)
		self.where_condition.where_condition_exp = copy.deepcopy(self.having_clause.where_condition_exp)
	    else:
		para_list = []
		para_list.append(self.where_condition.where_condition_exp)
		para_list.append(self.having_clause.where_condition_exp)
		self.where_condition.where_condition_exp = expression.Function("AND", para_list)
	self.child.predicatePushdown()
		
    def columnFilter(self):
	print "groupby filter"
	new_select_dic = {}
	new_exp_list = []
	if self.groupby_clause is None:
	    # TODO error
	    return
		
	for exp in self.groupby_clause.groupby_list:
	    if isinstance(exp, expression.Function):
		col_list = []
		exp.getPara(col_list)
		for col in col_list:
		    GroupbyNode.__addExpToChildSelectList__(col, new_exp_list, new_select_dic)
	    elif isinstance(exp, expression.Column):
		GroupbyNode.__addExpToChildSelectList__(exp, new_exp_list, new_select_dic)
	    else:
		new_exp = copy.deepcopy(exp)
		new_exp_list.append(new_exp)
		new_select_dic[new_exp] = None
	
	if self.where_condition is not None:
	    col_list = []
	    __getGroupbyList__(self.where_condition.where_condition_exp, col_list)
	    for col in col_list:
		GroupbyNode.__addExpToChildSelectList__(col, new_exp_list, new_select_dic)
	
	if self.select_list is not None:
	    for exp in self.select_list.exp_list:
		if isinstance(exp, expression.Column):
		    GroupbyNode.__addExpToChildSelectList__(exp, new_exp_list, new_select_dic)
		    #new_select_dic[exp] = self.select_list.exp_alias_dic[exp]
		elif isinstance(exp, expression.Function):
		    col_list =  []
		    exp.getPara(col_list)
		    for col in col_list:
			GroupbyNode.__addExpToChildSelectList__(col, new_exp_list, new_select_dic)
	self.child.select_list.exp_alias_dic = new_select_dic
	self.child.select_list.exp_list = new_exp_list
	self.child.columnFilter()

    @staticmethod
    def __addExpToChildSelectList__(exp, new_exp_list, new_select_dic):
	flag = False
	for item in new_exp_list:
	    if exp.compare(item) is True:
		flag = True
		break
	if flag is False:
	    new_exp = copy.deepcopy(exp)
	    new_exp_list.append(new_exp)
	    new_select_dic[new_exp] = None
		
    def processSelectStar(self):
	super(GroupbyNode, self).processSelectStar()
	self.child.processSelectStar()
	
    def genColumnIndex(self):
	if self.select_list is None or self.child.select_list is None:
		# TODO error
	    return
	select_dic = self.child.select_list.exp_alias_dic
	exp_list = self.child.select_list.exp_list
	if self.groupby_clause is not None:
	    for exp in self.groupby_clause.groupby_list:
		if isinstance(exp, expression.Column):
		    self.__genColumnIndex__(exp, exp_list, select_dic)
						
	if self.where_condition is not None:
	    col_list = []
	    self.where_condition.where_condition_exp.getPara(col_list)
	    for col in col_list:
		self.__genColumnIndex__(col, exp_list, select_dic)
					
	for exp in self.select_list.exp_list:
	    if isinstance(exp, expression.Column):
		self.__genColumnIndex__(exp, exp_list, select_dic)
	    elif isinstance(exp, expression.Function):
		col_list = []
		exp.getPara(col_list)
		for col in col_list:
		    GroupbyNode.__genColumnIndex__(col, exp_list, select_dic)
	
	self.child.genColumnIndex()

    @staticmethod	
    def __genColumnIndex__(col, exp_list, select_dic):
	i = 0
	for exp in exp_list:
	    if isinstance(exp, expression.Column):
		if col.compare(exp) or col.column_name == select_dic[exp]:
		    col.column_name = i
		    break
		i += 1

    def getMapOutput(self, table_name):
	'''if self.child is not None and self.child.select_list is not None:
	    return copy.deepcopy(self.child.select_list.exp_list)
	return None'''
	return copy.deepcopy(self.select_list.exp_list)

    def getMapFilter(self, table_name):
	'''if self.child is not None:
	    return self.child.where_condition
	return None'''
	return self.where_condition

    def toOp(self):
	global op_id
	op_id = op_id + 1
	ret_op = op.SpjeOp()
	ret_op.id = [op_id]
	ret_op.node_list.append(self)
	ret_op.map_phase.append(self)
	ret_op.reduce_phase.append(self)
	ret_op.pk_list = self.getPartitionKey()
	# DEBUG
	print "GroupbyNode, PK_LIST:"
	for pk in ret_op.pk_list:
	    util.printExpList(pk)
	return ret_op

    def getPartitionKey(self, to_origin=True):
	ret_exp_list = []
	tmp_exp_list = []
	if self.groupby_clause is not None:
	    for exp in self.groupby_clause.groupby_list:
		if isinstance(exp, expression.Function):
		    tmp_list = []
	            exp.getPara(tmp_list)
		    tmp_exp_list.extend(tmp_list)
		else:
		    tmp_exp_list.append(exp)
	if to_origin is False:
	    return [tmp_exp_list]
        for exp in tmp_exp_list:
            new_exp = self.__getOriginalExp__(exp, False)
            if new_exp is not None and new_exp not in ret_exp_list:
		ret_exp_list.append(new_exp)
	    if isinstance(new_exp, expression.Column):
	        print "PartitionKey:", new_exp.table_name, new_exp.column_name
        return [ret_exp_list]

    '''def getPartitionKey(self):
	ret_exp_list = []
        gb_exp_list = []
        for exp in self.groupby_clause.groupby_list:
            new_exp = self.__getOriginalExp__(exp, False)
            tmp = []
            tmp.append(new_exp)
	    if isinstance(new_exp, expression.Column):
	        print "PartitionKey:", new_exp.table_name, new_exp.column_name
            ret_exp_list.append(tmp)
            gb_exp_list.append(new_exp)
        
        ret_exp_list.append(gb_exp_list)
        return ret_exp_list'''

    def __getOriginalExp__(self, exp, flag):
	if not isinstance(exp, expression.Column):
	    return exp
	index = exp.column_name
        tmp_exp = self.child.select_list.exp_list[index]
        new_exp = copy.deepcopy(self.child.__getOriginalExp__(tmp_exp, False))
        return new_exp

    def __print__(self):
	print "GroupbyNode:----------"
	super(GroupbyNode, self).__print__()
	if self.groupby_clause is not None:
	    self.groupby_clause.__print__()
	if self.child is not None:
	    self.child.__print__()
	
class OrderbyNode(Node):
    child = None
    parent = None
    composite = None
    output = None

    def __init__(self, name="orderby"):
	self.name = name
	super(OrderbyNode, self).__init__()
	
    def genOrderby(self):
	self.child = self.child.genOrderby()
	return super(OrderbyNode, self).genOrderby()
	
    def toBinJoinTree(self):
	self.child = self.child.genOrderby()
	self.child.parent = self
	return super(OrderbyNode, self).toBinJoinTree()

    def genProjectList(self):
	self.child.genProjectList()
	
    def checkSchema(self):
	schema_checker = SchemaChecker(self)
	if schema_checker.checkOrderby(self) and self.child.checkSchema():
	    return True
	return False
	
    def genTableName(self):
	super(OrderbyNode, self).genTableName()
	self.child.genTableName()
	
    def predicatePushdown(self):
	self.child.predicatePushdown()
	
    def columnFilter(self):
	new_select_dic = {}
	new_exp_list = []
	for exp in self.orderby_clause.orderby_exp_list:
	    if isinstance(exp, expression.Column):
		for child_exp in self.child.select_list.exp_list:
		    if exp.column_name == self.child.select_list.exp_alias_dic[child_exp]:
			new_exp = copy.deepcopy(child_exp)
			new_exp_list.append(new_exp)
			new_select_dic[new_exp] = None
	    else:
		new_exp = copy.deepcopy(exp)
		new_exp_list.append(new_exp)
		new_select_dic[new_exp] = None
			
	self.orderby_clause.orderby_exp_list = copy.deepcopy(new_exp_list)
	for child_exp in self.child.select_list.exp_list:
	    new_exp = copy.deepcopy(child_exp)
	    new_exp_list.append(new_exp)
	    new_select_list[new_exp] = self.child.select_list.exp_alias_dic[child_exp]
		
	self.child.select_list.exp_list = new_exp_list
	self.child.select_list.exp_alias_dic = new_select_dic

	self.child.columnFilter()
	
    def processSelectStar(self):
	super(OrderbyNode, self).processSelectStar()
	self.child.processSelectStar()
	
    def genColumnIndex(self):
	self.child.genColumnIndex()

    def getMapOutput(self, table_name):
	'''if self.child is not None and self.child.select_list is not None:
	    return copy.deepcopy(self.child.select_list.exp_list)
	return None'''
	return copy.deepcopy(self.select_list.exp_list)

    def getMapFilter(self, table_name):
	'''if self.child is not None:
	    return self.child.where_condition
	return None'''
	return self.where_condition

    def toOp(self):
	global op_id
	op_id = op_id + 1
	ret_op = op.SpjOp()
	ret_op.id = [op_id]
	ret_op.node_list.append(self)
	ret_op.map_phase.append(self)
	ret_op.reduce_phase.append(self)
	ret_op.pk_list = self.getPartitionKey()
	# DEBUG
	print "OrderbyNode, PK_LIST:"
	for pk in ret_op.pk_list:
	    util.printExpList(pk)
	return ret_op

    def getPartitionKey(self, to_origin=True):
	return []

    def __getOriginalExp__(self, exp):
	if not isinstance(exp, expression.Column):
	    return exp
	return None

    def __print__(self):
	print "OrderbyNode:----------"
	super(OrderbyNode, self).__print__()
	if self.orderby_clause is not None:
	    self.orderby_clause.__print__()
	if self.child is not None:
	    self.child.printNode()

class JoinNode(Node):
    is_explicit = None
    join_type = None
    join_condition = None
    left_composite = None
    right_composite = None
    left_child = None
    right_child = None
    parent = None

    def __init__(self, name="join"):
	self.name = name
	super(JoinNode, self).__init__()

    def adjustIndex(self, exp_list, table_name):
	filter_name = ""
	if table_name in self.left_child.table_list or table_name == "LEFT":
	    filter_name = "LEFT"
	else:
	    filter_name = "RIGHT"
	
	for x in self.select_list.tmp_exp_list:
	    if isinstance(x, expression.Column):
		if x.table_name != filter_name:
		    continue
		index = x.column_name
		if filter_name == "LEFT":
            	    old_exp = self.left_child.select_list.tmp_exp_list[index]
        	else:
            	    old_exp = self.right_child.select_list.tmp_exp_list[index]
			
		for exp in exp_list:
		    if exp.column_name == old_exp.column_name:
			x.column_name = exp_list.index(exp)
			break
	
	if self.where_condition is not None:
	    col_list = []
	    self.where_condition.on_condition_exp.getPara(col_list)
	    for x in col_list:
		if x.table_name != filter_name:
		    continue
		index = x.column_name
		if filter_name == "LEFT":
            	    old_exp = self.left_child.select_list.tmp_exp_list[index]
        	else:
            	    old_exp = self.right_child.select_list.tmp_exp_list[index]
			
		for exp in exp_list:
		    if exp.column_name == old_exp.column_name:
			x.column_name = exp_list.index(exp)
			break

    def setComposite(self, composite, node):
	if self.left_child == node:
	    self.left_composite = composite
	else:
	    self.right_composite = composite
		
    def genProjectList(self):
	self.left_child.genProjectList()
	self.right_child.genProjectList()
	
    def checkSchema(self):
	schema_checker = SchemaChecker(self)
	if self.is_explicit and schema_checker.checkJoin(self) and schema_checker.checkWhere(self) and schema_checker.checkSelectList(self) and self.left_child.checkSchema() and self.right_child.checkSchema():
	    return True
	return False
	
    def genTableName(self):
	super(JoinNode, self).genTableName()
	if self.is_explicit is True:
	    self.join_condition.on_condition_exp.genTableName(self)
	#else:
	#    self.join_condition.where_condition_exp.genTableName(self)
	self.left_child.genTableName()
	self.right_child.genTableName()
	
    def predicatePushdown(self):
	print "JoinNode, predicate"
	if self.where_condition is not None:
	    exp = self.where_condition.where_condition_exp
	    left_exp = exp.booleanFilter(self.left_child, True)
	    right_exp = exp.booleanFilter(self.right_child, True)
	    # DEBUG
	    print exp.evaluate(), left_exp, right_exp, "PREDICATE_PUSHDOWN"
	    if left_exp is not None:
		if self.left_child.where_condition is None:
		    self.left_child.where_condition = ast.WhereConditionParser(None)
		self.left_child.where_condition.where_condition_exp = copy.deepcopy(left_exp)
	    if right_exp is not None:
		if self.right_child.where_condition is None:
		    self.right_child.where_condition = ast.WhereConditionParser(None)
		self.right_child.where_condition.where_condition_exp = copy.deepcopy(right_exp)
	    if self.is_explicit is False:
		join_exp = exp.genJoinKey()
		if join_exp is not None:
		    print "join_exp not none:", join_exp.evaluate()
		    print "exp:", exp.evaluate()
		    if self.join_condition is None:
			self.join_condition = ast.WhereConditionParser(None)
		    self.join_condition.where_condition_exp = copy.deepcopy(join_exp)
		
	    col_list = []
	    exp.getPara(col_list)
	    if len(col_list) == 0:
		self.where_condition = None
	self.left_child.predicatePushdown()
	self.right_child.predicatePushdown()
	
    def columnFilter(self):
	self.__childColumnFilter__(self.left_child)
	self.__childColumnFilter__(self.right_child)
	self.left_child.columnFilter()
	self.right_child.columnFilter()
	
    def __childColumnFilter__(self, child):
	select_dic = {}
	exp_list = []
	if self.select_list is not None:
	    for exp in self.select_list.exp_list:
		print "child columnfilter:", exp.evaluate()
		exp.selectListFilter(self.select_list, child, select_dic, exp_list)
	util.printExpList(exp_list)
	if self.where_condition is not None:
	    self.where_condition.where_condition_exp.addToSelectList(child, select_dic, exp_list)
	if self.is_explicit is True:
	    self.join_condition.on_condition_exp.addToSelectList(child, select_dic, exp_list)
	elif self.join_condition is not None:
	    self.join_condition.where_condition_exp.addToSelectList(child, select_dic, exp_list)
	print "add to select list:"
	util.printExpList(exp_list)
	if child.select_list is None:
	    child.select_list = ast.SelectListParser(None)
	if isinstance(child, SPNode) and isinstance(child.child, TableNode) is False:
	    new_list = []
	    new_dic = {}
	    print "table_alias", child.table_alias
	    table = util.searchTable(child.table_alias)
	    print table
	    if table is None:
		# TODO error
		return
	    for col in table.column_list:
		flag = False
		for exp in exp_list:
		    if col.column_name == exp.column_name and exp.table_name == table.table_name:
			flag = True
			break
		if flag is True:
		    new_list.append(exp)
		    new_dic[exp] = None
	    print "new_list"
	    util.printExpList(new_list)
	    child.select_list.exp_alias_dic = new_dic
	    child.select_list.exp_list = new_list
	else:
	    child.select_list.exp_alias_dic = select_dic
	    child.select_list.exp_list = exp_list
	'''child.select_list.exp_alias_dic = select_dic
	child.select_list.exp_list = exp_list'''
	
    def processSelectList(self):
	super(JoinNode, self).processSelectStar()
	self.left_child.processSelectStar()
	self.right_child.processSelectStar()
	
    def updateSPList(self):
	if self.is_explicit:
	    join_exp = self.join_condition.on_condition_exp
	else:
	    join_exp = self.join_condition.where_condition_exp
	where_exp = self.where_condition.where_condition_exp

    '''def __genJoinKey__(self):
	print "genJoinKey..."
	if self.is_explicit is False:
	    # DEBUG
	    print "genJoinKey"
	    join_exp = self.join_condition.where_condition_exp.genJoinKey()
	    if join_exp is not None:
		self.join_condition.where_condition_exp = copy.deepcopy(join_exp)'''

    def genColumnIndex(self):
	#self.__genSelectIndex__()
	#self.__genWhereIndex__()
	#self.__genJoinIndex__()
	self.table_list = []
	self.__genChildColumnIndex__(self.left_child, False)
	self.__genChildColumnIndex__(self.right_child, True)
	self.left_child.genColumnIndex()
	self.right_child.genColumnIndex()

    def __genJoinIndex__(self):
	if self.join_condition is None:
	    return
	if self.is_explicit:
	    if isinstance(self.join_condition.on_condition_exp, expression.Function):
	        self.join_condition.on_condition_exp = self.join_condition.on_condition_exp.genIndex(self)
	else:
	    if isinstance(self.join_condition.where_condition_exp, expression.Function):
	        self.join_condition.where_condition_exp = self.join_condition.where_condition_exp.genIndex(self)
	
    
    def __genChildColumnIndex__(self, child, _type):
	left_select = None
	right_select = None
	join_exp = None
	child_name = (_type and "RIGHT" or "LEFT")
	
	if self.is_explicit is True:
	    join_exp = self.join_condition.on_condition_exp
	elif self.join_condition is not None:
	    join_exp = self.join_condition.where_condition_exp
		
	if child.select_list is not None:
	    select_dic = child.select_list.exp_alias_dic
	    exp_list = child.select_list.exp_list
	
	    # generate the index of join key
	    if join_exp is not None:
		col_list = []
		#print "CHILD_NAME"
		'''if isinstance(child, SPNode) and isinstance(child.child, TableNode):					
		    if self.is_explicit is True:
			self.join_condition.on_condition_exp = self.join_condition.on_condition_exp.genIndex(child)
			self.join_condition.on_condition_exp.getPara(col_list)
		    elif self.join_condition is not None:
			self.join_condition.where_condition_exp = self.join_condition.where_condition_exp.genIndex(child)
			self.join_condition.where_condition_exp.getPara(col_list)
		    print "CHILD_NAME"
		    for col in col_list:
			if col.table_name in child.table_list:
			    col.table_name = child_name
			    # DEBUG
			    print "CHILD_NAME", col.table_name
			    #print "col.table_name:", col.table_name
			    self.table_list.append(child_name)'''
		#else:
		join_exp.getPara(col_list)
		for col in col_list:
			# DEBUG
			#print "join_exp, __genColumnIndex__: ", [col.table_name, col.column_name]
			#print select_dic
		    self.__genColumnIndex__(col, exp_list, select_dic, self.table_list, child_name)
			#print "after:", [col.table_name, col.column_name]
		
		# generate the index of select list
	    for exp in self.select_list.exp_list:
		if isinstance(exp, expression.Function):
		    col_list = []
		    exp.getPara(col_list)
		    for col in col_list:
			self.__genColumnIndex__(col, exp_list, select_dic, self.table_list, child_name)
		elif isinstance(exp, expression.Column):
		    self.__genColumnIndex__(exp, exp_list, select_dic, self.table_list, child_name)
	
	    if self.where_condition is not None:
	        where_exp = self.where_condition.where_condition_exp
	        col_list = []
	        where_exp.getPara(col_list)
	        for col in col_list:
		    self.__genColumnIndex__(col, exp_list, select_dic, self.table_list, child_name)
		
    @staticmethod
    def __genColumnIndex__(col, exp_list, select_dic, table_list, table_name):
	for exp in exp_list:
	    if (isinstance(exp, expression.Column) and col.compare(exp)) or col.column_name == select_dic[exp]:
		col.table_name = table_name
		col.column_name = exp_list.index(exp)
		# DEBUG
		#print "__genColumnIndex__: ", [col.table_name, col.column_name]
		if table_name not in table_list:
		    table_list.append(table_name)
		    break

    def getMapOutput(self, table_name):
	'''if table_name == "LEFT" or table_name in self.left_child.table_list or table_name in self.left_child.table_alias_dict.values():
            return copy.deepcopy(self.left_child.select_list.exp_list)

        else:
            return copy.deepcopy(self.right_child.select_list.exp_list)'''
	return copy.deepcopy(self.select_list.exp_list)

    def getMapFilter(self, table_name):
        '''if table_name in self.left_child.table_list:
           return self.left_child.where_condition
        else:
            return self.right_child.where_condition'''
	return self.where_condition

    def toOp(self):
	global op_id
	op_id = op_id + 1
	ret_op = op.SpjOp()
	ret_op.id = [op_id]
	ret_op.node_list.append(self)
	ret_op.map_phase.append(self)
	ret_op.reduce_phase.append(self)
	ret_op.pk_list = self.getPartitionKey()
	# DEBUG
	print "JoinNode, PK_LIST:"
	for pk in ret_op.pk_list:
	    util.printExpList(pk)
	return ret_op

    def getPartitionKey(self, to_origin=True):
	if self.join_condition is None:
	    return None
	
	ret_exp_list = []
	tmp_list = []
	left_list = []
	right_list = []
	
	if self.is_explicit:
	    self.join_condition.on_condition_exp.getPara(tmp_list)
	else:
	    print "join_exp:", self.join_condition.where_condition_exp.evaluate()
	    self.join_condition.where_condition_exp.getPara(tmp_list)
	# print tmp_list
	print "tmp_list:"
	util.printExpList(tmp_list)
	if to_origin is False:
	    for tmp in tmp_list:
		if tmp.table_name == "LEFT":
		    left_list.append(tmp)
		else:
		    right_list.append(tmp)
	    return [left_list, right_list]

	for i in range(0, len(tmp_list)):
	    new_exp = self.__getOriginalExp__(tmp_list[i], True)
	    if new_exp is not None:
		if tmp_list[i].table_name == "LEFT":
		    left_list.append(new_exp)
		else:
		    right_list.append(new_exp)
	ret_exp_list.append(left_list)
	ret_exp_list.append(right_list)
	print "ret_exp_list"
	return ret_exp_list

    '''def getPartitionKey(self):
	if self.join_condition is None:
	    return None
	
	ret_exp_list = []
	tmp_list = []
	left_list = []
	right_list = []
	
	if self.is_explicit:
	    self.join_condition.on_condition_exp.getPara(tmp_list)
	else:
	    self.join_condition.where_condition_exp.getPara(tmp_list)
	# print tmp_list
	print "tmp_list:", tmp_list[0].table_name, tmp_list[1].table_name
	for i in range(0, len(tmp_list)):
	    new_exp = self.__getOriginalExp__(tmp_list[i], True)
	    if new_exp is None:
		continue
	    if tmp_list[i].table_name == "LEFT":
		left_list.append(new_exp)
	        print "PartitionKey:", new_exp.evaluate()
	    else:
		right_list.append(new_exp)
	        print "PartitionKey:", new_exp.evaluate()
	
	ret_exp_list.append(left_list)
	ret_exp_list.append(right_list)
	return ret_exp_list'''

    def __getOriginalExp__(self, exp, flag):
	print "111111111111111"
	if not isinstance(exp, expression.Column):
	    return exp
	index = exp.column_name
        table_name = exp.table_name
	print "__genOriginalExp__:", exp.evaluate(), table_name, index
        if table_name == "LEFT":
	    '''if flag and isinstance(self.left_child, SPNode) and isinstance(self.left_child.child, TableNode):
		tmp_exp = copy.deepcopy(exp)
		tmp_exp.table_name = self.left_child.select_list.exp_list[0].table_name
		new_exp = copy.deepcopy(self.left_child.child.__getOriginalExp__(tmp_exp, False))
	    else:'''            
	    index = exp.column_name
	    tmp_exp = self.left_child.select_list.exp_list[index]
	    new_exp = copy.deepcopy(self.left_child.__getOriginalExp__(tmp_exp, False))
        else:
	    '''if flag and isinstance(self.right_child, SPNode) and isinstance(self.right_child.child, TableNode):
		tmp_exp = copy.deepcopy(exp)
		tmp_exp.table_name = self.right_child.select_list.exp_list[0].table_name
		new_exp = copy.deepcopy(self.right_child.child.__getOriginalExp__(tmp_exp, False))
            else:'''
	    index = exp.column_name
	    tmp_exp = self.right_child.select_list.exp_list[index]
	    new_exp = copy.deepcopy(self.right_child.__getOriginalExp__(tmp_exp, False))
	print "new_exp", new_exp.evaluate()
        return new_exp

    def __print__(self):
	print "JoinNode:----------"
	super(JoinNode, self).__print__()
	print "is_explicit: ", self.is_explicit, "join_type: ", self.join_type
	if self.join_condition is not None:
	    self.join_condition.__print__()
	if self.left_child is not None:
	    print "left_child:"
	    self.left_child.__print__()
	if self.right_child is not None:
	    print "right_child:"
	    self.right_child.__print__()

class JoinNodeList(Node):
    is_explicit = None
    join_info = None
    children_list = None
    parent = None

    def __init__(self, name="joinlist"):
	self.name = name
	super(JoinNodeList, self).__init__()
		
    def genGroupby(self):
        tmp_list = self.children_list
        self.children_list = []
	
        for child in tmp_list:
	    self.children_list.append(child.genGroupby())
	
        return super(JoinNodeList, self).genGroupby()
	
    def genOrderby(self):
	tmp_list = self.children_list
	self.children_list = []
	
	for child in tmp_list:
	    self.children_list.append(child.genOrderby())
	return super(JoinNodeList, self).genOrderby()
	
    def toBinaryJoinTree(self):
        tmp_list = list(self.children_list)
        self.children_list = []
        for child in tmp_list:
	    self.children_list.append(child.toBinaryJoinTree())
		
        tmp_children_list = list(self.children_list)
        current_node = None
        tmp_index = -1
        for child in tmp_children_list:
	    if current_node is None:
	        current_node = child
		
	    else:
	        join_node = JoinNode()
	        join_node.source = self
	        join_node.is_explicit = self.is_explicit
	        join_node.left_child = copy.deepcopy(current_node)
	        join_node.left_child.parent = join_node
	        join_node.right_child = copy.deepcopy(child)
	        join_node.right_child.parent = join_node
		join_node.parent = self.parent
		
	        for t in current_node.table_list:
		    if t not in join_node.table_list:
		        join_node.table_list.append(t)
	        join_node.table_alias_dic = current_node.table_alias_dic
	
	        for t in child.table_alias_dic.keys():
		    if t not in join_node.table_alias_dic.keys():
		        join_node.table_alias_dic[t] = child.table_alias_dic[t]
	
	        for t in child.table_list:
		    if t not in join_node.table_list:
		        join_node.table_list.append(t)
	
	        if self.is_explicit == True:
		    tmp_list_join_condition = self.join_info[0]
		    tmp_list_join_type = self.join_info[1]
		
		    join_node.join_condition = tmp_list_join_condition[tmp_index]
		    join_node.join_type = tmp_list_join_type[tmp_index]
		
	        current_node = join_node
	    tmp_index += 1

        current_node.select_list = copy.deepcopy(self.select_list)
        current_node.where_condition = copy.deepcopy(self.where_condition)
	
        if self.is_explicit == False:
	    current_node.join_condition = copy.deepcopy(self.where_condition)
		
        return current_node

    def __print__(self):
	print "JoinNodeList:----------"
	super(JoinNodeList, self).__print__()
	for node in self.children_list:
	    node.__print__()

class RootSelectNode(Node):
    from_list = None
    converted_from_list = None

    def __init__(self):
	self.from_list = []
	super(RootSelectNode, self).__init__()

    def toInitialPlanTree(self, input, is_handler):
	node = None
	if input["type"] == "BaseTable":
	    node = SPNode()
	    node.source = input
	    node.is_handler = is_handler
	    # CHANGED
	    if node.is_handler:
	        node.select_list = self.select_list
                node.where_condition = self.where_condition
                node.groupby_clause = self.groupby_clause
                node.having_clause = self.having_clause
                node.orderby_clause = self.orderby_clause

	    '''node.select_list = self.select_list
            node.where_condition = self.where_condition
            node.groupby_clause = self.groupby_clause
            node.having_clause = self.having_clause
            node.orderby_clause = self.orderby_clause'''
	    node.child = TableNode()
	    node.child.select_list = ast.SelectListParser(None)
	    node.child.table_name = input["content"]
	    node.child.table_alias = input["alias"]
	    if node.child.table_alias == "":
		if node.child.table_name not in node.child.table_list:
		    node.child.table_list.append(node.child.table_name)
	    else:
		if node.child.table_alias not in node.child.table_list:
		    node.child.table_list.append(node.child.table_alias)
		node.child.table_alias_dic[node.child.table_alias] = node.child.table_name
	    print "table_list", node.child.table_list
	    node.table_list = node.child.table_list
	    node.table_alias_dic = node.child.table_alias_dic
	elif input["type"] == "SubQuery":
	    '''node = input["content"].toInitialQueryPlanTree()
	    node.table_alias = input["alias"]
	    if node.table_alias is not None and node.table_alias not in node.table_list:
		node.table_list = [node.table_alias]'''
	    node = SPNode()
	    node.source = input
	    node.is_handler = is_handler
	    if is_handler:	
	        node.select_list = self.select_list
                node.where_condition = self.where_condition
                node.groupby_clause = self.groupby_clause
                node.having_clause = self.having_clause
                node.orderby_clause = self.orderby_clause
	    node.child = input["content"].toInitialQueryPlanTree()
	    node.table_alias = input["alias"]
	    if node.table_alias is not None and node.table_alias not in node.table_list:
		node.table_list.append(node.table_alias)
	
	elif input["type"] == "JoinClause":
	    node = SPNode()
	    node.source = input
	    node.is_handler = is_handler
	    # CHANGED
	    '''if is_handler:
	        node.select_list = self.select_list
                node.where_condition = self.where_condition
                node.groupby_clause = self.groupby_clause
                node.having_clause = self.having_clause
                node.orderby_clause = self.orderby_clause'''

	    node.select_list = self.select_list
            node.where_condition = self.where_condition
            node.groupby_clause = self.groupby_clause
            node.having_clause = self.having_clause
            node.orderby_clause = self.orderby_clause
	    
	    node.child = JoinNodeList()
	    node.child.parent = node
	    tmp_children_list = []
	    for item in input["content"]:
		child = self.toInitialPlanTree(item, False)
		tmp_children_list.append(child)
	    node.child.children_list = list(tmp_children_list)
	    #node.select_list = self.select_list
	    node.where_condition = self.where_condition
            '''node.child.groupby_clause = self.groupby_clause
            node.child.having_clause = self.having_clause
            node.child.orderby_clause = self.orderby_clause'''
	    
	    node.child.is_explicit = True
	    node.child.join_info = []
	    node.child.join_info.append(input["on_condition"])
	    node.child.join_info.append(input["join_type"])
	
	    '''node.table_list = node.child.table_list
	    node.table_alias_dic = node.child.table_alias_dic'''
	return node
	
    def toInitialQueryPlanTree(self):
	node = None
	tmp_from_list = self.converted_from_list
	if len(tmp_from_list) == 1:
	    node = self.toInitialPlanTree(tmp_from_list[0], True)
	
	else:
	    '''node = SPNode()
	    node.select_list = self.select_list
	    node.where_condition = self.where_condition
	    node.groupby_clause = self.groupby_clause
	    node.having_clause = self.having_clause
	    node.orderby_clause = self.orderby_clause
	    
	    node.child = JoinNodeList()
	    node.child.parent = node
	    #node.child.select_list = node.select_list
	    node.child.select_list = ast.SelectListParser(None)
	    node.child.where_condition = node.where_condition
	    tmp_list = []
	    for item in tmp_from_list:
		converted_item = self.toInitialPlanTree(item, False)
		tmp_list.append(converted_item)
		for key in converted_item.table_alias_dic.keys():
		    if key not in node.child.table_alias_dic.keys():
			node.child.table_alias_dic[key] = converted_item.table_alias[key]
		
		for table in converted_item.table_list:
		    if table not in node.child.table_list:
			node.child.table_list.append(table)
	    node.child.children_list = list(tmp_list)
	
	    node.child.is_explicit = False
	    node.child.join_info = []
	    node.child.join_info.append(self.where_condition)
	    print "joinnodelist", node.child.table_list
	    node.table_list = node.child.table_list
	    node.table_alias_dic = node.child.table_alias_dic'''
	    
	    node = JoinNodeList()
	    node.select_list = self.select_list
	    node.where_condition = self.where_condition
	    node.groupby_clause = self.groupby_clause
	    node.having_clause = self.having_clause
	    node.orderby_clause = self.orderby_clause
	    
	    tmp_list = []
	    for item in tmp_from_list:
		converted_item = self.toInitialPlanTree(item, False)
		tmp_list.append(converted_item)
		for key in converted_item.table_alias_dic.keys():
		    if key not in node.table_alias_dic.keys():
			node.table_alias_dic[key] = converted_item.table_alias[key]
		
		for table in converted_item.table_list:
		    if table not in node.table_list:
			node.table_list.append(table)
	    node.children_list = list(tmp_list)
	
	    node.is_explicit = False
	    node.join_info = []
	    node.join_info.append(self.where_condition)
	return node

    def convertFromList(self, input_list):
	item_dic = {}
	if len(input_list) < 3:
	    item_dic["type"] = "BaseTable"
	    item_dic["source"] = input_list
	    item_dic["content"] = input_list[0].content
	    if len(input_list) == 2:
		item_dic["alias"] = input_list[1].content
	    else:
		item_dic["alias"] = ""
	else:
	    item_dic["type"] = "SubQuery"
	    item_dic["source"] = input_list
	    sub_tree = None
	    id = None
	    for item in input_list:
		if isinstance(item, RootSelectNode):
		    sub_tree = item
		else:
		    if item.token_name == "ID":
			id = item.content
	    item_dic["content"] = sub_tree
	    item_dic["alias"] = id
	return item_dic
		
    def processFromList(self):
	final_from_list = []
	# partition by comma
	tmp_from_list = []
	for item in self.from_list[1:]:
	    tmp_from_list.append(item)
	    if isinstance(item, ast.ASTreeNode):
		if item.token_name == "COMMA":
		    final_from_list.append(tmp_from_list[:-1])
		    tmp_from_list = []
	    else:
		item.processFromList()
	final_from_list.append(tmp_from_list)
	
	# convert each item
	tmp_converted_from_list = []
	self.converted_from_list = tmp_converted_from_list
	for item in final_from_list:
	    item_dic = {}
	    if len(item) < 3:
		# item is a base table with alias or not
		item_dic["type"] = "BaseTable"
		item_dic["source"] = item
		item_dic["content"] = item[0].content
		if len(item) == 2:
		    item_dic["alias"] = item[1].content
		else:
		    item_dic["alias"] = ""
		tmp_converted_from_list.append(item_dic)
		continue
		
	    has_join = False
	    for t in item:
		if (not isinstance(t, RootSelectNode)) and t.content == "JOIN":
		    # item is a join clause
		    has_join = True
	    if not has_join:
		# item is a sub query
		item_dic["type"] = "SubQuery"
		item_dic["source"] = item
		sub_tree = None
		id = None
		for t in item:
		    if isinstance(t, RootSelectNode):
			sub_tree = t
		    else:
			if t.token_name == "ID":
			    id = t.content
			
		item_dic["content"] = sub_tree
		item_dic["alias"] = id
		tmp_converted_from_list.append(item_dic)
		continue
		
	    # item is a join clause
	    item_dic["type"] = "JoinClause"
	    item_dic["source"] = item
	    item_dic["content"] = []
	    item_dic["alias"] = []
	    item_dic["join_type"] = []
	    item_dic["on_condition"] = []
		
	    # TODO join clause
	    flag = False
	    tmp_list = []
	    for t in item:
		tmp_list.append(t)
		
		if isinstance(t, RootSelectNode):
		    continue
		if t.content == "JOIN":
		    input_list = tmp_list[:-3]
		    join_type = tmp_list[-3].content
		    if not flag:
			final_item = self.convertFromList(tmp_list)
			item_dic["content"].append(final_item)
		    else:
			parser = OnConditionParser(input_list)
			item_dic["on_condition"].append(parser)
			flag = False
		    item_dic["join_type"].append(join_type)
		    tmp_list = []
		    continue
		elif t.content == "ON":
		    input_list = tmp_list[:-1]
		    final_item = self.convertFromList(tmp_list)
		    item_dic["content"].append(final_item)
		    flag = True
		    tmp_list = []
	
	    parser = ast.OnConditionParser(tmp_list)
	    item_dic["on_condition"].append(parser)
	    tmp_converted_from_list.append(item_dic)
	
def planTreeToMRQ(node):
    mrq = node.toOp()
    if isinstance(node, JoinNode):
        if node.left_child is not None:
            lchild_op = planTreeToMRQ(node.left_child)
	    if lchild_op is not None:
		mrq.child_list.append(lchild_op)
		lchild_op.parent = mrq
	if node.right_child is not None:
	    rchild_op = planTreeToMRQ(node.right_child)
	    if rchild_op is not None:
		mrq.child_list.append(rchild_op)
		rchild_op.parent = mrq
    elif isinstance(node, GroupbyNode) or isinstance(node, OrderbyNode) or isinstance(node, SPNode):
	if node.child is not None:
	    child_op = planTreeToMRQ(node.child)
	    if child_op is not None:
		mrq.child_list.append(child_op)
		child_op.parent = mrq
    return mrq

'''def optimize(op):
    minset_map = {}
    minset = []
    minset_num = 0
    free_vertexes = []
    preOptimize(op, free_vertexes, minset_map, minset, minset_num)
    mergeMinset(op, minset_map, minset, minset_num)
    mergeFreeVertex(free_vertexex)
    
def mergeMinset(op, minset_map, minset, minset_num):
    if len(op.child_list) == 0:
	root_mrq = op.findRoot()
        root_mrq.__printAll__()
	return
    for child in op.child_list:
	mergeMinset(child, minset_map, minset, minset_num)
	if child.is_free_vertex or minset_map[op] != minset_map[child]:
	    continue
	new_op = None
        st_type = strategy.get_st(child, op)
        op1_child, op2_child = [], []
        new_op = Op.merge(child, self, i, op1_child, op2_child)
        mergeMinset(new_op, minset_map, minset, minset_num)

def mergeFreeVertex(free_vertexes):
    for fv in free_vertexes:
	p = getPrevNoneSPOp(fv)
	q = getPostNoneSPOp(fv)
	if fv.criticalFun() and fv.alpha * fv.beta <= 1:
	    Op.merge(p, fv, 4, [], [])
	    #fv to p.reduce_phase
	else if fv.criticalFun() and fv.alpha * fv.beta > 1:
	    Op.merge(fv, q, 3, [], [])
	    #fv to q.reduce_phase
	else if fv.criticalFun() is False and fv.alpha * fv.beta <= 1:
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

def preOptimize(op, free_vertexes, minset_map, minset, minset_num):
    node = op.node_list[0]
    if isinstance(node, SPNode) or isinstance(node, TableNode):
	return
    if node.parent is None:
	minset_map[op] = 0
	minset[0].append(op)
    prev = getPrevNoneSPOp(op)
    if op.pk_compare(op.pk_list, op.pk_list):
	if isinstance(node.parent, SPNode):
	    minset_map[op.parent] = minset_map[prev][0]
	    minset[minset_map[op.parent]].append(op.parent)
	minset_map[op] = minset_map[prev]
	minset[minset_map[prev]].append(op)
    else:
	if isinstance(node.parent, SPNode):
	    op.parent.is_free_vertex = True
	    free_vertexes.append(op.parent)
	minset_num = minset_num + 1
	minset_map[op] = minset_num
	minset[minset_num].append(op)
    for child in op.child_list:
	preOptimize(child, minset_map, minset, minset_num)

def getPrevNoneSPOp(op):
    if op.parent is None:
	return None
    if isinstance(op.parent.node_list[0], SPNode) is False:
	return op.parent
    return getPrevNoneSPNode(op.parent)

def getPostNoneSPOp(op):
    if len(op.child_list) > 1:
	return None
    if isinstance(op.child_list[0].node_list[0], SPNode) is False:
	return getPostNoneSPOp(op.child_list[0]
    return getPostNoneSPNode(op.child_list[0])
'''
