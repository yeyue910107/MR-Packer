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
import copy

global_table_dic = {}
agg_func_list = ["SUM", "AVG", "COUNT", "MAX", "MIN", "COUNT_DISTINCT"]

def isColumnUniqueInTable(column_name, table_name):
    if table_name not in global_table_dic:
	return False

    tmp_list = filter(lambda x:x == column_name, global_table_dic[table_name].column_list)
    return (len(tmp_list) == 1)
	
def isColumnInTable(column_name, table_name):
    return (table_name in global_table_dic.keys()) and (column_name in [x.column_name for x in global_table_dic[table_name].column_list])

def searchTable(table_name):
    if table_name in global_table_dic.keys():
	return global_table_dic[table_name]
    return None

def searchColumn(column_name):
    ret_list = []
    for key in global_table_dic.keys():
	for column in global_table_dic[key].column_list:
	    if column_name == "*" or column_name == column.column_name:
		tmp = copy.deepcopy(column)
		ret_list.append(tmp)

    return ret_list

def printExpList(exp_list):
    for exp in exp_list:
	if exp is not None:
	    print exp.evaluate()

def printExpAliasDic(exp_alias_dic):
    for exp in exp_alias_dic.keys():
	if exp is not None:
	    print exp.evaluate() + " : " + str(exp_alias_dic[exp])
