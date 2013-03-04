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

    def __init__(self):
        pass

    def genGroupby(self):
        # explicit groupby
		gb = GroupbyNode()
		gb.source = self.source
		gb.select_list = copy.deepcopy(self.select_list)
		self.groupby_clause = None
        if self.groupby_clause is not None:
            gb.groupby_clause = self.groupby_clause
            if self.having_clause is not None:
                gb.having_clause = self.having_clause
                self.having_clause = None

        # implicit orderby, with avg function
        else:
            if self.select_list is None:
                return self
            select_dic = self.select_list.exp_alias_dic
			flag = False
			for key in select_dic.keys():
				flag |= key.hasGroupbyFunc()
			if flag is False:
				return self
			gb.groupby_clause = GroupbyParser(None)
			gb.groupby_clause.groupby_exp_list.append(Constant(1, "INTERGER"))
			
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
				if isinstance(exp, Column):
					if exp.column_name != "*":
						new_exp = copy.deepcopy(exp)
						new_exp_list.append(new_exp)
						new_select_dic[new_exp] = select_dic[exp]
					else:
						for item in self.table_list:
							table = searchTable(item)
							if table is not None:
								for col in table.column_list:
									tmp_exp = Column(item, col.column_name)
									new_exp_list.append(tmp_exp)
									new_select_dic[tmp_exp] = None
						for key in self.table_alias_dic.keys():
							table = searchTable(self.self.table_alias_dic[key])
							if table is not None:
								for col in table.column_list:
									tmp_exp = Column(item, col.column_name)
									new_exp_list.append(tmp_exp)
									new_select_dic[tmp_exp] = None
				elif isinstance(exp, Function):
					if exp.getGroupbyFuncName() == "COUNT"
						col_list = []
						exp.getPara(col_list)
						for col in col_list:
							if col.column_name == "*":
								func_obj = col.func_obj
								new_con = Constant(1, "INTERGER")
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
		
	def postProcess(self):
		self.genProjectList()
		if self.checkSchema() is False:
			# TODO error
		self.processSelectStar()
		self.genTableName()
		self.predicatePushdown()
		self.columnFilter()
		self.genTableName()
		self.genColumnIndex()
		
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
        super(SPNode, self).genGroupby()

    def genOrderby(self):
        self.child = self.child.genOrderby()
        super(SPNode, self).genOrderby()

    def toBinaryJoinTree(self):
        self.child = self.child.toBinaryJoinTree()
        self.child.parent = self
        return super(SPNode, self).toBinaryJoinTree()
		
	def genProjectList(self):
		genProjectList(self.child)
		
		for item in self.child.table_list:
			if item not in self.in_table_list:
				self.in_table_list.append(item)
		self.in_table_alias_dic = self.child.table_alias_dic
		
		project_list = []
		self.__genProjectList__(self.child.select_list, 
							self.in_table_list, 
							self.in_table_alias_dic, 
							project_list)
		tmp_name = self.table_alias
		if tmp_name in global_table_dic.keys():
			while tmp_name in global_table_dic.keys():
				tmp_name += "_1"
		tmp_schema = TableSchema(tmp_name, project_list)
		global_table_dic[tmp_name] = tmp_schema
		
	def __genProjectList__(self, project_list):
		select_list = self.child.select_list
		table_list = self.in_table_list
		table_alias_dic = self.in_table_alias_dic
		exp_dic = select_list.exp_alias_dic
		
		for exp in select_list.exp_list:
			col_type = None
			if isinstance(exp, Column):
				if exp.column_name == "*":
					__addTableList__(table_list, project_list)
					__addTableList__(table_alias_dic.values, project_list)
				else:
					col_list = searchColumn(exp.column_name)
					for col in col_list:
						if col.table_schema.table_name in table_list or 
							in table_alias_dic.values():
							col_type = col.table_schema.getColumnByName(exp.column_name).column_type
					if col_type is not None:
						if exp_dic[exp] is not None:
							tmp_col = ColumnSchema(exp_dic[exp], col_type)
						else:
							tmp_col = ColumnSchema(exp.column_name, col_type)
						project_list.append(tmp_col)
			elif isinstance(exp, Function):
				if exp_dic[exp] is not None:
					col_type = exp.getValueType()
					tmp_col = ColumnSchema(exp_dic[exp], col_type)
					project_list.append(tmp_col)
			else:
				# TODO error
	
	def __addTableList__(table_list, project_list):
		for table in table_list:
			tmp_table = searchTable(table)
			if tmp_table is not None:
				for col in tmp_table.column_list:
					tmp_col = copy.deepcopy(col)
					project_list.append(tmp_col)
	
	def checkSchema(self):
		schema_checker = SchemaChecker(self)
		if schema_checker.checkSelectList(self) and 
			schema_checker.checkWhere(self) and 
			schema_checker.checkGroupby(self) and 
			self.child.checkSchema():
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
				select_dic = self.child.select_list.exp_alias_dic
				for item in col_list:
					flag = False
					for key in select_dic.keys():
						if item.column_name == select_dic[key]:
							flag = True
							item.table_name = ""
							if isinstance(key, Column):
								item.column_name = key.column_name
								item.table_name = key.table_name
							else:
								tmp_func = item.func_obj
								new_para_list = []
								for para in tmp_func.parameter_list:
									if not isinstance(para, Column):
										new_para_list.append(para)
										continue
									if para.column_name == item.column_name:
										new_para = copy.deepcopy(key)
										new_para_list.append(new_para)
									else:
										new_para_list.append(para)
								func_obj.set_para_list(new_para_list)
							break
							
						if isinstance(key, Column):
							if item.column_name == key.column_name:
								flag = True
								item.table_name = key.table_name
								break
						
						elif isinstance(key, Function):
							if item.column_name == select_dic[key]
								flag = True
								item.table_name = ""
								break
					if flag is False:
						# TODO error
					
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
						self.child.where_condition.where_condition_exp = Function("AND", para_list)
		self.where_condition = None
		self.child.predicatePushdown()
		
	def columnFilter(self):
		new_select_dic = {}
		new_exp_list = []
		if self.select_list is None or self.child.select_dic is None:
			# TODO error
		child_exp_list = self.child.select_list.exp_list
		child_exp_dic = self.child.exp_alias_dic
		
		for exp in self.select_list.exp_list
			if isinstance(exp, Column):
				for child_exp in child_exp_list:
					if isinstance(child_exp, Column):
						if child_exp_dic[child_exp] == exp.column_name and 
							child_exp not in new_exp_list:
							new_exp_list.append(child_exp)
							new_select_dic[child_exp] = exp.column_name
							break
						if child_exp.column_name == exp.column_name and 
							child_exp not in new_exp_list:
							new_exp_list.append(child_exp)
							new_select_dic[child_exp] = None
							break
					elif isinstance(child_exp, Function) and 
						child_exp_dic[child_exp] == exp.column_name and 
						child_exp not in new_exp_List:
						new_exp_list.append(child_exp)
						new_select_dic[child_exp] = None
						break
			elif isinstance(exp, Function):
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
		
class TableNode(Node):
    parent = None
    composite = None
    table_name = None
    table_alias = None

    def __init__(self):
        super(TableNode, self).__init__()
		
	def checkSchema(self):
		schema_checker = SchemaChecker(self)
		if searchTable(self.table_name) is not None and 
			schema_checker.checkSelectList(self) and 
			schema_checker.checkWhere(self):
			return True
		return False
		
	def genColumnIndex(self):
		self.__genSelectIndex__()
		self.__genWhereIndex__()

class GroupbyNode(Node):
    child = None
    parent = None
    composite = None

    def __init__(self):
        super(GroupbyNode, self).__init__()

    def toBinaryJoinTree(self):
        self.child = self.child.toBinaryJoinTree()
        self.child.parent = self
        return super(GroupbyNode, self).bin_join_tree()
		
	def genProjectList(self):
		self.child.genProjectList()
	
	def checkSchema(self):
		schema_checker = SchemaChecker(self)
		if schema_checker.checkSelectList(self) and 
			schema_checker.checkWhere(self) and 
			schema_checker.checkGroupby(self) and 
			(self.having_clause is not None and schema_checker.checkHaving(self)) and 
			self.child.checkSchema():
			return True
		return False
		
	def genTableName(self):
		if self.groupby_clause is not None:
			for exp in self.groupby_clause.groupby_exp_list:
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
					new_exp = Function("AND", para_list)
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
				self.where_condition.where_condition_exp = Function("AND", para_list)
		self.child.predicatePushdown()
			
	def columnFilter(self):
		new_select_dic = {}
		new_exp_list = []
		if self.groupby_clause is None:
			# TODO error
			
		for exp in self.groupby_clause.groupby_exp_list:
			if isinstance(exp, Function):
				col_list = []
				exp.getPara(col_list)
				__addExpToChildSelectList__(exp, new_exp_list, new_select_dic)
			elif isinstance(exp, Column):
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
				if isinstance(exp, Column):
					__addExpToChildSelectList__(exp, new_exp_list, new_select_dic)
					new_select_dic[new_exp] = self.select_list.exp_alias_dic[exp]
				elif isinstance(exp, Function):
					col_list =  []
					exp.getPara(col_list)
					for col in col_list:
						__addExpToChildSelectList__(col, new_exp_list, new_select_dic)
		self.child.select_list.exp_alias_dic = new_select_dic
		self.child.select_list.exp_list = new_exp_list
		self.child.columnFilter()

	def __addExpToChildSelectList__(exp, new_exp_list, new_select_dic):
		flag = False
		for item in new_exp_list:
			if exp.campare(item) is True:
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
		if self.select_list is None or self.child_select_list is None:
			# TODO error
		select_dic = self.child.select_list.exp_alias_dic
		exp_list = self.child.select_list.exp_list
		if self.groupby_clause is not None:
			for exp in self.groupby_clause.groupby_exp_list:
				if isinstance(exp, Column):
					self.__genColumnIndex__(exp, exp_list, select_dic)
							
		if self.where_condition is not None:
			col_list = []
			self.where_condition.where_condition_exp.getPara(col_list)
			for col in col_list:
				self.__genColumnIndex__(col, exp_list, select_dic)
						
		for exp in self.select_list.exp_list:
			if isinstance(exp, Column):
				self.__genColumnIndex__(exp, exp_list, select_dic)
			elif isinstance(exp, Function):
				col_list = []
				exp.getPara(col_list)
				for col in col_list:
					self.__genColumnIndex__(col, exp_list, select_dic)
		
		self.child.genColumnIndex()
					
	def __genColumnIndex__(col, exp_list, select_dic):
		for exp in exp_list:
			if isinstance(exp, Column) and col.compare(exp) or 
				col.column_name == select_dic[exp]
				col.column_name = exp_list.index[exp]
				break

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
			if isinstance(exp, Column):
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
			fileter_name = "LEFT"
		else
			filter_name = "RIGHT"
		
		for x in self.select_list.tmp_exp_list:
			if isinstance(x, Column):
				if x.table_name != filter_name
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
	
	def getPrimaryKey(self):
		if self.join_condition is None:
			return None
		
		ret_exp_list = []
		tmp_list = []
		left_list = []
		right_list = []
		
		if self.is_explicit:
			self.join_condition.on_condition_exp.getPara(tmp_list)
		else
			self.join_condition.where_condition_exp.getPara(tmp_list)
		
		for i in range(0, len(tmp_list)):
			new_exp = __trace_to_leaf(self, tmp_list[i], True)
			if tmp_list[i].table_name == "LEFT":
				left_list.append(new_exp)
			else:
				right_list.append(new_exp)
		
		ret_exp_list.append(left_list)
		ret_exp_list.append(right_list)
		return ret_exp_list
		
	def setComposite(self, composite, node):
		if self.left_child == node:
			self.left_composite = composite
		else
			self.right_composite = composite
			
	def genProjectList(self):
		self.left_child.genProjectList()
		self.right_child.genProjectList()
		
	def checkSchema(self):
		schema_checker = SchemaChecker(self)
		if self.is_explicit and 
			schema_checker.checkJoin(self) and 
			schema_checker.checkWhere(self) and 
			schema_checker.checkSelectList(self) and 
			self.left_child.checkSchema() and 
			self.right_child.checkSchema():
				return True
		return False
		
	def genTableName(self):
		super(JoinNode, self).genTableName()
		if self.is_explicit is True:
			self.join_condition.on_condition_exp.genTableName(self)
		self.left_child.genTableName()
		sele.right_child.genTableName()
		
	def predicatePushdown(self):
		if self.where_condition is not None:
			exp = self.where_condition.where_condition_exp
			left_exp = exp.booleanFilter(self.left_child, True)
			right_exp = exp.booleanFilter(self.right_child, True)
			
			if left_exp is not None:
				if self.left_child.where_condition is None:
					self.left_child.where_condition = WhereConditionParser(None)
				self.left_child.where_condition.where_condition_exp = copy.deepcopy(left_exp)
			if right_exp is not None:
				if self.right_child.where_condition is None:
					self.right_child.where_condition = WhereConditionParser(None)
				self.right_child.where_condition.where_condition_exp = copy.deepcopy(right_exp)
			if self.is_explicit is False:
				join_exp = __genJoinKey__(exp, True)
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
		__childColumnFilter__(self, self.left_child)
		__childColumnFilter__(self, self.right_child)
		self.left_child.columnFilter()
		self.right_child.columnFilter()
		
	def __childColumnFilter__(self, child):
		select_dic = {}
		exp_list = []
		__selectListFilter__(self.select_list, 
							child.table_list, 
							child.table_alias_dic, 
							select_dic, 
							exp_list)# TODO
		if self.where_condition is not None:
			self.where_condition.where_condition_exp.addToSelectList(self, select_dic, exp_list)
		if self.is_explicit is True:
			self.join_condition.on_condition.addToSelectList(child, select_dic, exp_list)
		elif self.join_condition is not None:
			self.join_condition.where_condition_exp.addToSelectList(node, select_dic, exp_list)
		
		if child.select_list is None:
			child.select_list = SelectListParser(None)
		if isinstance(child, SPNode):
			new_list = []
			new_dic = {}
			table = searchTable(child.table_alias)
			if table is None:
				# TODO error
			for col in table.column_list:
				flag = False
				for exp in child_exp_list:
					if col.compare(exp) is True:
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
		
	def genColumnIndex(self):
		self.__genChildColumnIndex__(self.left_child, False)
		self.__genChildColumnIndex__(self.right_child, True)
		self.left_child.genColumnIndex()
		self.right_child.genColumnIndex()
			
	def __genChildColumnIndex__(self, child, type):
		left_select = None
		right_select = None
		self.table_list = []
		join_exp = None
		child_name = type ? "LEFT" : "RIGHT"
		
		if self.is_explicit is True:
			join_exp = self.join_condition.on_condition_exp
		elif self.join_condition is not None:
			join_exp = self.join_condition.where_condition_exp
			
		if self.child.select_list is not None:
			select_dic = self.child.select_list.exp_alias_dic
			exp_list = self.child.select_list.exp_list
		
			# generate the index of join key
			if join_exp is not None:
				col_list = []
				if isinstance(child, TableNode):					
					if self.is_explicit is True:
						self.join_condition.on_condition_exp = self.join_condition.on_condition_exp.genIndex(node)
						self.join_condition.on_condition_exp.genFuncPara(col_list)
					elif self.join_condition is not None:
						self.join_condition.where_condition_exp = self.join_condition.where_condition_exp.genIndex(node)
						self.join_condition.on_condition_exp.genFuncPara(col_list)
					for col in col_list:
						if col.table_name in child.table_list:
							col.table_name = child_name
							self.table_list.append(child_name)
				else:
					join_exp.genFuncPara(col_list)
					for col in col_list:
						self.__genColumnIndex__(col, exp_list, select_dic, self.table_list, child_name)
			
			# generate the index of select list
			for exp in self.select_list.exp_list:
				if isinstance(exp, Function):
					col_list = []
					exp.genFuncPara(col_list)
					for col in col_list:
						self.__genColumnIndex__(col, exp_list, select_dic, self.table_list, child_name)
				else:
					self.__genColumnIndex__(exp, exp_list, select_dic, self.table_list, child_name)
		
		if self.where_condition is not None:
			where_exp = self.where_condition.where_condition_exp
			col_list = []
			where_exp.genFuncPara(col_list)
			for col in col_list:
				self.__genColumnIndex__(col, exp_list, select_dic, self.table_list, child_name)
			
		
	def __genColumnIndex__(col, 
						exp_list, 
						select_dic, 
						table_list, 
						table_name):
		for exp in exp_list:
			if (isinstance(exp, Column) and col.compare(exp)) or 
				col.column_name == select_dic[exp]:
				col.table_name = table_name
				col.col_name = exp_list.index(exp)
				if table_name not in table_list:
					table_list.append(table_name)
					break
	
	def __getSelectIndex__(self):
		if self.select_list is None:
			return
		new_select_dic = {}
		new_exp_list = []
		exp_dic = self.select_list.exp_alias_dic
		exp_list = self.select_list.exp_list
		
		for exp in exp_list:
			new_exp = exp.genIndex(self)
			new_exp_list.append(new_exp)
			if isinstance(exp, Column):
				new_select_dic[new_exp] = self.select_list.exp_alias_dic[exp]
			else:
				new_select_dic[new_exp] = None
		
		self.select_list.exp_alias_dic = new_select_dic
		self.select_list.exp_list = new_exp_list
		
	def __genWhereIndex__(self):
		if self.where_condition is None:
			return
		if isinstance(self.where_condition.where_condition_exp, Function):
			self.where_condition.where_condition_exp = self.where_condition.where_condition_exp.genIndex(self)

class JoinNodeList(Node):
	is_explicit = None
	join_info = None
	children_list = None
	
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
		
class RootSelectNode(Node):
	from_list = None
	converted_from_list = None
	
	def __init__(self):
		self.from_list = []
		super(RootSelectNode, self).__init__()
	
	def toInitialPlanTree(self, input):
		node = None
		if input["type"] == "BaseTable":
			node = TableNode()
			node.source = input
			
			node.table_name = input["content"]
			node.table_alias = input["alias"]
			
			if node.table_alias == "":
				node.table_list.append(node.table_name)
			else:
				node.table_list.append(node.table_alias)
				node.table_alias_dic[node.table_alias] = node.table_name
				
		elif input["type"] == "SubQuery":
			node = SPNode()
			node.source = input
			
			node.child = input["content"].toInitialQueryPlanTree()
			node.table_alias = input["alias"]
			if node.table_alias is not None and node.table_alias not in table_list:
				node.table_list.append(node.table_alias)
		
		elif input["type"] == "JoinClause":
			node = JoinNodeList()
			node.source = input
			
			tmp_children_list = []
			for item in input["content"]:
				child = self.toInitialPlanTree(item)
				tmp_children_list.append(child)
			node.children_list = list(tmp_children_list)
			
			node.is_explicit = True
			node.join_info = []
			node.join_info.append(input["jc_on_condition"])
			node.join_info.append(input["jc_jointype_list"])
		
		return node
		
	def toInitialQueryPlanTree(self):
		node = None
		tmp_from_list = self.converted_from_list
		
		if len(tmp_from_list) == 1:
			node = self.toInitialPlanTree(tmp_from_list[0])
		
		else:
			node = JoinNodeList()
			node.select_list = self.select_list
			node.where_condition = self.where_condition
			node.groupby_clause = self.groupby_clause
			node.having_clause = self.having_clause
			node.orderby_clause = self.orderby_clause
			
			tmp_list = []
			for item in tmp_from_list:
				converted_item = self.toInitialPlanTree(item)
				tmp_list.append(converted_item)
				for key in converted_item.table_alias_dic.keys():
					if key not in node.table_alias_dic.keys():
						node.table_alias_dic[key] = converted_item.table_alias
				
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
					if item.token_name == "ID"
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
			if isinstance(item, ASTreeNode):
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
				if (not isinstance(t, ASTreeNode)) and t.content == "JOIN":
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
		
			parser = OnConditionParser(tmp_list)
			item_dic["on_condition"].append(parser)
			tmp_converted_from_list.append(item_dic)
		
