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

    def __init__(self):
        pass

    def genGroupby(self):
        # explicit groupby
	gb = GroupbyNode()
	gb.source = self.source
	gb.is_handler = self.is_handler
	gb.select_list = copy.deepcopy(self.select_list)
	self.groupby_clause = None
        if self.groupby_clause is not None:
	    print "111111"
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

    def getPartitionKey(self):
	pass

    def postProcess(self):
	self.genProjectList()
	#if self.checkSchema() is False:
	    # TODO error
	#    pass
	self.processSelectStar()
	self.genTableName()
	#self.predicatePushdown()
	self.columnFilter()
	self.genTableName()
	self.genColumnIndex() 

    def __getOriginalExp__(self, exp):
	pass

    def __print__(self):
	if self.select_list is not None:
	    self.select_list.__print__()
	if self.where_condition is not None:
	    self.where_condition.__print__()
	#print "table_list: ", self.table_list, "table_alias_dic: ", self.table_alias_dic
	
class SPNode(Node):
    child = None
    parent = None
    composite = None
    table_alias = None
    in_table_list = []
    in_table_alias_dic = {}

    def __init__(self):
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
	if self.select_list is None:
	    return
	select_list = self.select_list
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
	new_exp = None
	if self.where_condition is not None:
	    tmp_exp = copy.deepcopy(self.where_condition.where_condition_exp)
	    if tmp_exp is not None:
		col_list = []
		tmp_exp.getPara(col_list)
		select_dic = self.select_list.exp_alias_dic
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
		"""	
		child_exp = tmp_exp.booleanFilter(self, True)
		if child_exp is not None:
		    if self.child.where_condition is None:
			self.child.where_condition = WhereConditionParser(None)
			self.child.where_condition.where_condition_exp = copy.deepcopy(child_exp)
		    else:
			new_exp = copy.deepcopy(child_exp)
			para_list = []
			para_list.append(self.child.where_condition_exp)
			para_list.append(new_exp)
			self.child.where_condition.where_condition_exp = expression.Function("AND", para_list)"""
	#self.where_condition = None
	self.child.predicatePushdown()
	
    def columnFilter(self):
	new_select_dic = {}
	new_exp_list = []
	if self.select_list is None or self.child.select_list is None:
	    # TODO error
	    return
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
		    elif isinstance(child_exp, expression.Function) and child_exp_dic[child_exp] == exp.column_name and child_exp not in new_exp_List:
			new_exp_list.append(child_exp)
			new_select_dic[child_exp] = None
			break
	    elif isinstance(exp, expression.Function):
		col_list = []
		exp.getPara(col_list)
		for col in col_list:
		    col.table_name = ""
	    new_exp_list.append(exp)
	    new_select_dic[exp] = None
	
	self.child.select_list.exp_list = new_exp_list
	self.child.select_list.exp_alias_dic = new_select_dic
	self.child.columnFilter()
	
    def processSelectStar(self):
	super(SPNode, self).processSelectStar()
	self.child.processSelectStar()
	
    def genColumnIndex(self):
	self.__genSelectIndex__()
	self.__genWhereIndex__()
	self.child.genColumnIndex()

    def toOp(self):
	ret_op = op.SpjOp()
	ret_op.is_sp = True
	ret_op.map_phase.append(self)
	ret_op.pk_list = self.getPartitionKey()
	# DEBUG
	print "PK_LIST:", ret_op.pk_list
	return ret_op

    def getPartitionKey(self):
	return self.child.getPartitionKey()

    def __getOriginalExp__(self, exp):
	if not isinstance(exp, expression.Column):
	    return exp
	if self.child.select_list is None:
	    return self.child.__getOriginalExp__(exp)
	index = exp.column_name
        tmp_exp = self.child.select_list.exp_list[index] 
        new_exp = copy.deepcopy(self.__getOriginalExp__(tmp_exp))
        return new_exp

    def __print__(self):
	print "SPNode:"
	super(SPNode, self).__print__()
	print "in_table_list: ", self.in_table_list, "in_table_alias_dic: ", self.in_table_alias_dic
	if self.child is not None:
	    self.child.__print__()

class TableNode(Node):
    parent = None
    composite = None
    table_name = None
    table_alias = None

    def __init__(self):
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

    def getPartitionKey(self):
	return []

    def __getOriginalExp__(self, exp):
	if not isinstance(exp, expression.Column):
	    return exp
	new_exp = copy.deepcopy(exp)
        new_exp.table_name = self.table_name
        return new_exp

    def __print__(self):
	print "TableNode:"
	super(TableNode, self).__print__()
	print "table_name: ", self.table_name, "table_alias: ", self.table_alias

class GroupbyNode(Node):
    child = None
    parent = None
    composite = None

    def __init__(self):
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
	new_select_dic = {}
	new_exp_list = []
	if self.groupby_clause is None:
	    # TODO error
	    return
		
	for exp in self.groupby_clause.groupby_list:
	    if isinstance(exp, expression.Function):
		col_list = []
		exp.getPara(col_list)
		__addExpToChildSelectList__(exp, new_exp_list, new_select_dic)
	    elif isinstance(exp, expression.Column):
		__addExpToChildSelectList__(col, new_exp_list, new_select_dic)
	    else:
		new_exp = copy.deepcopy(exp)
		new_exp_list.append(new_exp)
		new_select_dic[new_exp] = None
	
	if self.where_condition is not None:
	    col_list = []
	    __getGroupbyList__(self.where_condition.where_condition_exp, col_list)
	    for col in col_list:
		__addExpToChildSelectList__(col, new_exp_list, new_select_dic)
	
	if self.select_list is not None:
	    for exp in self.select_list.exp_list:
		if isinstance(exp, expression.Column):
		    GroupbyNode.__addExpToChildSelectList__(exp, new_exp_list, new_select_dic)
		    new_select_dic[new_exp] = self.select_list.exp_alias_dic[exp]
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
	for exp in exp_list:
	    if isinstance(exp, expression.Column) and col.compare(exp) or col.column_name == select_dic[exp]:
		col.column_name = exp_list.index(exp)
		break

    def toOp(self):
	ret_op = op.SpjeOp()
	ret_op.map_phase.append(self)
	ret_op.reduce_phase.append(self)
	ret_op.pk_list = self.getPartitionKey()
	# DEBUG
	print "PK_LIST:", ret_op.pk_list
	return ret_op

    def getPartitionKey(self):
	ret_exp_list = []
        gb_exp_list = []
        for exp in self.groupby_clause.groupby_list:
            new_exp = self.__getOriginalExp__(exp)
            tmp = []
            tmp.append(new_exp)
            ret_exp_list.append(tmp)
            gb_exp_list.append(new_exp)
        
        ret_exp_list.append(gb_exp_list)
	# DEBUG
	print "ret_exp_list:", ret_exp_list
        return ret_exp_list

    def __getOriginalExp__(self, exp):
	if not isinstance(exp, expression.Column):
	    return exp
	index = exp.column_name
        tmp_exp = self.child.select_list.exp_list[index]
        new_exp = copy.deepcopy(self.child.__getOriginalExp__(tmp_exp))
        return new_exp

    def __print__(self):
	print "GroupbyNode:"
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

    def __init__(self):
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

    def toOp(self):
	ret_op = op.SpjOp()
	ret_op.map_phase.append(self)
	ret_op.reduce_phase.append(self)
	ret_op.pk_list = self.getPartitionKey()
	# DEBUG
	print "PK_LIST:", ret_op.pk_list
	return ret_op

    def getPartitionKey(self):
	return []

    def __getOriginalExp__(self, exp):
	if not isinstance(exp, expression.Column):
	    return exp
	return None

    def __print__(self):
	print "OrderbyNode:"
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

    def __init__(self):
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
	else:
	    self.join_condition.where_condition_exp.genTableName(self)
	self.left_child.genTableName()
	self.right_child.genTableName()
	
    def predicatePushdown(self):
	if self.where_condition is not None:
	    exp = self.where_condition.where_condition_exp
	    left_exp = exp.booleanFilter(self.left_child, True)
	    right_exp = exp.booleanFilter(self.right_child, True)
	    # DEBUG
	    #print exp.evaluate(), left_exp, right_exp, "PREDICATE_PUSHDOWN"
	    if left_exp is not None:
		if self.left_child.where_condition is None:
		    self.left_child.where_condition = WhereConditionParser(None)
		self.left_child.where_condition.where_condition_exp = copy.deepcopy(left_exp)
	    if right_exp is not None:
		if self.right_child.where_condition is None:
		    self.right_child.where_condition = WhereConditionParser(None)
		self.right_child.where_condition.where_condition_exp = copy.deepcopy(right_exp)
	    if self.is_explicit is False:
		join_exp = exp.genJoinKey()
		if join_exp is not None:
		    if self.join_condition is None:
			self.join_condition = WhereConditionParser(None)
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
		exp.selectListFilter(self.select_list, child, select_dic, exp_list)
	if self.where_condition is not None:
	    self.where_condition.where_condition_exp.addToSelectList(self, select_dic, exp_list)
	if self.is_explicit is True:
	    self.join_condition.on_condition_exp.addToSelectList(child, select_dic, exp_list)
	elif self.join_condition is not None:
	    self.join_condition.where_condition_exp.addToSelectList(child, select_dic, exp_list)
	
	if child.select_list is None:
	    child.select_list = ast.SelectListParser(None)
	if isinstance(child, SPNode):
	    new_list = []
	    new_dic = {}
	    table = util.searchTable(child.table_alias)
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
	    child.select_list.exp_alias_dic = new_dic
	    child.select_list.exp_list = new_list
	else:
	    child.select_list.exp_alias_dic = select_dic
	    child.select_list.exp_list = exp_list
	
    def processSelectList(self):
	super(JoinNode, self).processSelectStar()
	self.left_child.processSelectStar()
	self.right_child.processSelectStar()
	
    def __genJoinKey__(self):
	print "genJoinKey..."
	if self.is_explicit is False:
	    # DEBUG
	    print "genJoinKey"
	    join_exp = self.join_condition.where_condition_exp.genJoinKey()
	    if join_exp is not None:
		self.join_condition.where_condition_exp = copy.deepcopy(join_exp)

    def genColumnIndex(self):
	self.__genJoinKey__()
	self.__genSelectIndex__()
	self.__genWhereIndex__()
	#self.__genJoinIndex__()
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
	self.table_list = []
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
		if isinstance(child, SPNode) and isinstance(child.child, TableNode):					
		    if self.is_explicit is True:
			self.join_condition.on_condition_exp = self.join_condition.on_condition_exp.genIndex(child)
			self.join_condition.on_condition_exp.getPara(col_list)
		    elif self.join_condition is not None:
			self.join_condition.where_condition_exp = self.join_condition.where_condition_exp.genIndex(child)
			self.join_condition.where_condition_exp.getPara(col_list)
		    for col in col_list:
			if col.table_name in child.table_list:
			    col.table_name = child_name
			    # DEBUG
			    #print "col.table_name:", col.table_name
			    self.table_list.append(child_name)
		else:
		    join_exp.getPara(col_list)
		    for col in col_list:
			# DEBUG
			#print "join_exp, __genColumnIndex__: ", [col.table_name, col.column_name]
			#print select_dic
			self.__genColumnIndex__(col, exp_list, select_dic, self.table_list, child_name)
			#print "after:", [col.table_name, col.column_name]
		
		# generate the index of select list
	    for exp in self.parent.select_list.exp_list:
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

    def toOp(self):
	ret_op = op.SpjOp()
	ret_op.map_phase.append(self)
	ret_op.reduce_phase.append(self)
	ret_op.pk_list = self.getPartitionKey()
	# DEBUG
	print "PK_LIST:", ret_op.pk_list
	return ret_op

    def getPartitionKey(self):
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
	for i in range(0, len(tmp_list)):
	    new_exp = self.__getOriginalExp__(tmp_list[i])
	    if tmp_list[i].table_name == "LEFT":
		left_list.append(new_exp)
	    else:
		right_list.append(new_exp)
	
	ret_exp_list.append(left_list)
	ret_exp_list.append(right_list)
	return ret_exp_list

    def __getOriginalExp__(self, exp):
	if not isinstance(exp, expression.Column):
	    return exp
	index = exp.column_name
        table_name = exp.table_name
	# print [table_name, index]
        if table_name == "LEFT":
	    if self.left_child.select_list is None:
		new_exp = copy.deepcopy(self.left_child.__getOriginalExp__(exp))
	    else:            
		index = exp.column_name
		tmp_exp = self.left_child.select_list.exp_list[index]
		new_exp = copy.deepcopy(self.left_child.__getOriginalExp__(tmp_exp))
        else:
	    if self.right_child.select_list is None:
		new_exp = copy.deepcopy(self.right_child.__getOriginalExp__(exp))
            else:
		index = exp.column_name
		# print index
		# print self.right_child.select_list.exp_list
		tmp_exp = self.right_child.select_list.exp_list[index]
		new_exp = copy.deepcopy(self.right_child.__getOriginalExp__(tmp_exp))

        return new_exp

    def __print__(self):
	print "JoinNode:"
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

    def __init__(self):
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
	print "JoinNodeList:"
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
	    if node.is_handler:
	        node.select_list = self.select_list
                node.where_condition = self.where_condition
                node.groupby_clause = self.groupby_clause
                node.having_clause = self.having_clause
                node.orderby_clause = self.orderby_clause

	    node.child = TableNode()
	    node.child.table_name = input["content"]
	    node.child.table_alias = input["alias"]
	    if node.child.table_alias == "":
		node.child.table_list.append(node.child.table_name)
	    else:
		node.child.table_list.append(node.child.table_alias)
		node.child.table_alias_dic[node.child.table_alias] = node.child.table_name
			
	elif input["type"] == "SubQuery":
	    node = input["content"].toInitialQueryPlanTree()
	    node.table_alias = input["alias"]
	    if node.table_alias is not None and node.table_alias not in node.table_list:
		node.table_list.append(node.table_alias)
	    """node = SPNode()
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
	"""
	elif input["type"] == "JoinClause":
	    node = SPNode()
	    node.source = input
	    node.is_handler = is_handler
	    if is_handler:
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
	    node.child.join_info.append(input["jc_on_condition"])
	    node.child.join_info.append(input["jc_jointype_list"])
	
	return node
	
    def toInitialQueryPlanTree(self):
	node = None
	tmp_from_list = self.converted_from_list
	if len(tmp_from_list) == 1:
	    node = self.toInitialPlanTree(tmp_from_list[0], True)
	
	else:
	    node = SPNode()
	    node.select_list = self.select_list
	    node.where_condition = self.where_condition
	    node.groupby_clause = self.groupby_clause
	    node.having_clause = self.having_clause
	    node.orderby_clause = self.orderby_clause
	    
	    node.child = JoinNodeList()
	    node.child.parent = node
	    #node.child.select_list = self.select_list
	    node.child.where_condition = self.where_condition
	    tmp_list = []
	    for item in tmp_from_list:
		converted_item = self.toInitialPlanTree(item, False)
		tmp_list.append(converted_item)
		for key in converted_item.table_alias_dic.keys():
		    if key not in node.child.table_alias_dic.keys():
			node.child.table_alias_dic[key] = converted_item.table_alias
		
		for table in converted_item.table_list:
		    if table not in node.child.table_list:
			node.child.table_list.append(table)
	    node.child.children_list = list(tmp_list)
	
	    node.child.is_explicit = False
	    node.child.join_info = []
	    node.child.join_info.append(self.where_condition)
		
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
		if t.conntent == "JOIN":
		    input_list = tmp_list[:-3]
		    join_type = input_list[-1].content
		    if not flag:
			final_item = self.convertFromList(tmp_list)
			item_dic["content"].append(final_item)
		    else:
			parser = OnConditionParser(input_list)
			item_dic["on_condition"].append(parser)
			flag = False
		    item_dic["join_type"].append(join_type)
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

