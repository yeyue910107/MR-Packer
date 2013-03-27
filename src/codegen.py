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

math_func_dic = { "PLUS":" + ", "MINUS":" - ", "DIVIDE":" / ", "MULTIPLY":" * " }
agg_func_list = [ "SUM", "AVG", "COUNT", "MAX", "MIN", "COUNT_DISTINCT" ]
bool_func_dic = { "AND":" && ", "OR":" || " }
relation_func_dic = { "EQ":" == ", "GT":" > ", "LT":" < ", "NEQ":" != ", "GE":" >= ", "LE":" <= "}
value_type_list = ["INTEGER", "DECIMAL", "TEXT", "DATE"]

def genSelectFunctionCode(exp, buf_dic):
    ret_str = ""
    if not isinstance(exp, expression.Function):
	return
    if exp.func_name in math_func_dic.keys():
	para1, para2 = exp.para_list[0], exp.para_list[1]
	ret_str += "("
	ret_str += genParaCode(para1, buf_dic)
	ret_str += math_func_dic[exp.func_name]
	ret_str += genParaCode(para2, buf_dic)
	ret_str += ")"
    elif exp.func_name in agg_func_list:
	if len(exp.para_list) != 1:
	    # TODO ERROR
	    return
	para = exp.para_list[0]
	ret_str = genParaCode(para, buf_dic)
    else:
	# TODO ERROR
	return
    return ret_str

def genParaCode(para, buf_dic, flag=False, groupby_exp_list=None, hash_key=None):
    if isinstance(para, expression.Column):
	ret_str += __genParaCode__(para.column_type, para.column_name, buf_dic[para.table_name])
    elif isinstance(para, expression.Function):
	if flag:
	    ret_str += genGroupbyExpCode(para, buf_dic, True, groupby_exp_list, hash_key)
	else:
	    ret_str += genSelectFunctionCode(para, buf_dic)
    else:
	ret_str += para.cons_value
    return ret_str

def genGroupbyExpCode(exp, buf_dic, groupby_exp_list, hash_key):
    ret_str = ""
    if not instance(exp, expression.Function):
	# TODO ERROR
	return
    if exp.func_name in agg_func_list:
	for item in groupby_exp_list:
	    if item.compare(exp):
		ret_str += "("
		ret_str += buf_dic["AGG"] + "[" + str(groupby_exp_list.index(item)) + "]"
		if hash_key is not None:
		    ret_str += ".get(" + hash_key + ")"
		ret_str += ")"
		break
    elif exp.func_name in math_func_dic.keys():
	para1, para2 = exp.para_list[0], exp.para_list[1]
	ret_str += "("
	ret_str += genParaCode(para1, buf_dic, True, groupby_exp_list, hash_key)
	ret_str += genParaCode(para2, buf_dic, True, groupby_exp_list, hash_key)
	ret_str += ")"
    else:
	# TODO ERROR
	return
    return ret_str

def __genParaCode__(para_type, value, buf_name):
    ret_str = ""
    if buf_name is not None:
	if para_type == "INTERGER":
	    ret_str = "Integer.parseInt(" + buf_name + "[" + str(value) + "])"
	elif para_type == "DECIMAL":
	    ret_str = "Double.parseDouble(" + buf_name + "[" + str(value) + "])"
	elif para_type == "TEXT":
	    ret_str = buf_name + "[" + str(value) + "]"
	elif para_type == "DATE":
	    ret_str = buf_name + "[" + str(value) + "]"
	else:
	    if para_type in value_type_list:
		ret_str = str(value)
	    else:
		# TODO ERROR
		pass
    return ret_str

def genValueCode(value_type, value_name, value_list):
