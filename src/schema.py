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
import copy
import ast

class ColumnSchema:
	column_name = None
	column_type = None
	column_others = None
	table_schema = None
	
	def __init__(self, column_name, column_type):
		self.column_name = column_name
		self.column_type = column_type
		
	def setOthers(self, others):
		self.column_others = others
		
class TableSchema:
	table_name = None
	column_list = None
	column_name_list = None
	
	def __init__(self, table_name, column_list):
		self.table_name = table_name
		self.column_list = column_list
		
		for item in column_list:
			item.table_schema = self
		
		self.column_name_list = []
		for item in column_list:
			self.column_name_list.append(item.column_name)
	
	def getColumnIndexByName(self, name):
		return self.column_name_list.index(name)
	
	def getColumnByName(self, name):
		index = self.getColumnIndexByName(name)
		if index != -1:
			return column_list[index]
		return None
			
class SchemaChecker:
	node = None
	
	def __init__(self, node):
		self.node = node
	
	def checkSelectList(self):
		# TODO
		
	def checkGroupby(self):
		# TODO
	
	def checkOrderby(self):
		# TODO
		
	def checkHaving(self):
		# TODO

	def checkWhere(self):
		# TODO

	def checkJoin(self):
		# TODO