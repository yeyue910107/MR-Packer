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
import copy

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
    ret_str = ""
    if value_name in relation_func_dic.keys():
	if value_type == "INTEGER" or value_type == "DECIMAL":
	    ret_str = value_list[0] + relation_func_dic[value_name] + value_list[1]
	else:
	    ret_str = value_list[0] + ".compareTo(" + value_list[1] + ")" + relation_func_dic[value_name] + "0"
    elif value_name in math_func_dic.keys():
	if value_type == "INTEGER" or value_type == "DECIMAL":
	    ret_str = value_list[0] + math_func_dic[value_name] + value_list[1]
	else:
	    # TODO ERROR
	    return
    elif value_name in bool_func_dic.keys():
	count = 0
	for value in value_list:
	    count = count + 1
	    if count != len(value_list):
		ret_str = ret_str + value + bool_func_dic[value_name]
	    else:
		ret_str = ret_str + value
    else:
	# TODO ERROR
	return
    return ret_str

def genWhereExpCode(exp, buf_dic):
    ret_str = ""
    if isinstance(exp, expression.Function):
	if exp.func_name in relation_func_dic.keys() or exp.func_name in math_func_dic.keys():
	    tmp_list = []
	    value_type = None
	    for tmp_exp in exp.para_list:
		if isinstance(tmp_exp, expression.Column):
		    if tmp_exp.table_name != "AGG":
			tmp_str = genParaCode(tmp_exp.column_type, tmp_exp.column_name, buf_dic[tmp_exp.table_name])
		    else:
			tmp_str = buf_dic[tmp_exp.table_name] + "[" + str(tmp_exp.column_name) + "]"
		    value_type = tmp_exp.column_type
		elif isinstance(tmp_exp, expression.Function):
		    tmp_str = genWhereExpCode(tmp_exp, buf_dic)
		    value_type = "DECIMAL"
		else:
		    tmp_str = tmp_exp.cons_value
		    value_type = tmp_exp.cons_type
		tmp_list.append(tmp_str)
	    if len(tmp_list) != 2:
		# TODO ERROR
		return
	    ret_str = genValueCode(value_type, exp.func_name, tmp_list)
	
	if exp.func_name in bool_func_dic.keys():
	    tmp_list = []
	    value_type = "BOOLEAN"
	    for tmp_exp in exp.para_list:
		tmp_str = genWhereExpCode(tmp_exp, buf_dic)
		tmp_list.append(tmp_str)
	    ret_str = genValueCode(value_type, exp.func_name, tmp_list)
	else:
	    ret_str += "("
	    if exp.func_name == "IS":
		[para1, para2] = exp.para_list
	    if isinstance(para1, expression.Column):
		ret_str += buf_dic[para1.table_name] + "[" + str(para1.column_name) + "]"
	    else:
		# TODO ERROR
		return
	    ret_str += ".compareTo(\""
	    if isinstance(para2, expression.Constant):
		ret_str += str(para2.cons_value)
		ret_str += "\") == 0"
	    else:
		# TODO ERROR
		return
	    ret_str += ")"
    elif isinstance(exp, expression.Constant):
	if exp.cons_type == "BOOLEAN":
	    return exp.cons_value

    return ret_str

sql_type_dic = {"INTEGER":"IntWritable", "DECIMAL":"DoubleWritable", "TEXT":"Text", "DATE":"Text"}
java_type_dic = {"IntWritable":"Integer", "DoubleWritable":"Double", "Text":"String", "Date":"String"}

def getKeyValueType(exp_list):
    res = ""
    if len(exp_list) != 1:
	res = sql_type_dic["TEXT"]
    else:
	exp = exp_list[0]
	if isinstance(exp, expression.Column):
	    res = sql_type_dic[exp.column_type]
	elif isinstance(exp, expression.Function):
	    res = sql_type_dic[exp.getValueType()]
	else:
	    res = sql_type_dic[exp.cons_type]
    return res

def genMRKeyValue(exp_list, value_type, buf_dic):
    res = ""
    if len(exp_list) == 0:
	res = "\"\""
	return res
    for exp in exp_list:
	if isinstance(exp, expression.Column):
	    res += genParaCode(exp.column_type, exp.column_name, buf_dic[exp.table_name])
	elif isinstance(exp, expression.Function):
	    res += genSelectFunctionCode(exp, buf_dic)
	else:
	    res += genParaCode(exp.cons_type, exp.cons_value, None)
	if value_type == "TEXT":
	    res += "+ \"\" +"
	else:
	    res += "+"
    res = res[:-1]
    return res

def getMaxIndex(exp_list):
    ret = -1
    for exp in exp_list:
	if isinstance(exp, expression.Column):
		ret = max(ret, int(exp.column_name))
	elif isinstance(exp, expression.Function):
	    col_list = []
	    exp.getPara(col_list)
	    ret = max(ret, max([int(col.col_name) for col in col_list]))
    return ret + 1

def getGroupbyExp(exp, tmp_list):
    if not isinstance(exp, expression.Function):
	return
    if exp.func_name in agg_func_list:
	tmp_list.append(exp)
    else:
	for para in exp.para_list:
	    getGroupbyExp(para, tmp_list)

def getGroupbyExpList(exp_list, gb_exp_list):
    for exp in exp_list:
	if isinstance(exp, expression.Function):
	    tmp_list = []
	    getGroupbyExp(exp, tmp_list)
	    for tmp in tmp_list:
		flag = False
		for gb_exp in gb_exp_list:
		    if tmp.compare(gb_exp):
			flag = True
			break
		if not flag:
		    gb_exp_list.append(tmp)

def genSPNodeCode(node, fo):
    line_buf = "line_buf"
    buf_dic = {}
    # TODO table_name
    buf_dic[node.table_name] = line_buf
    buf_dic[node.table_alias] = line_buf
    map_key_type = "NullWritable"
    map_value_type = genMRValueType(node.select_list.exp_list)
    map_value = genMRKeyValue(node.select_list.exp_list, map_value_type, buf_dic)
    exp_list = node.select_list.exp_list
    if node.where_condition is not None:
	exp_list.append(node.where_condition.where_condition_exp)
    max_index = getMaxIndex(exp_list)
    if node.where_condition is None:
	print >> fo, "\t\t\tNullWritable key_op = NullWritable.get();"
	tmp_output = "\t\t\tcontext.write(key_op, "
	tmp_output += "new " + max_value_type + "(" + map_value + ")"
	tmp_output += ");"
	print >> fo, tmp_output
    else:
	where_str = "\t\t\tif("
	where_str += genWhereExpCode(node.where_condition.where_condition_exp)
	where_str += "){\n"
	print >> fo, where_str
	
	print >> fo, "\t\t\tNullWritable key_op = NullWritable.get();"
	tmp_output = "\t\t\tcontext.write(key_op, "
	tmp_output += "new " + max_value_type + "(" + map_value + ")"
	tmp_output += ");"
	print >> fo, tmp_output
	print >> fo, "\t\t\t}"
    print >> fo, "\t\t}\n"
    print >> fo, "\t}\n"

def getJoinKey(exp, col_list, table):
    if exp is None or not isinstance(exp, expression.Function):
	return
    if len(exp.para_list) == 2:
	[para1, para2] = exp.para_list
	flag = True
	if not isinstance(para1, expression.Column) or not isinstance(para2, expression.Column):
	    flag = False
	if flag and para1.table_name != para2.table_name:
	    if para1.table_name == table:
		col_list.append(para1)
	    else:
		col_list.append(para2)
	    return
    for para in exp.para_list:
	if isinstance(para, expression.Function):
	    getJoinKey(para, col_list, table)

def genFuncExp(exp, table_name):
    if not isinstance(exp, expression.Function):
	return None
    new_list = []
    for para in exp.para_list:
	if isinstance(para, expression.Column):
	    if para.table_name != table_name:
		new_list.append(expression.Column("\"NULL\"", "TEXT"))
	    else:
		new_list.append(para)
	elif isinstance(para, expression.Function):
	    tmp_exp = genFuncExp(para, table_name)
	    if tmp_exp is None:
		# TODO ERROR
		return
	    new_list.append(tmp_exp)
	else:
	    new_list.append(para)
    ret_exp = expression.Function(exp.func_name, new_list)
    return ret_exp

def genJoinList(exp_list, new_list, table_name):
    count = 0
    tmp_exp = expression.Constant("\"NULL\"", "TEXT")
    for exp in exp_list:
	if isinstance(exp, expression.Column):
	    if exp.table_name != table_name:
		new_list.append(tmp_exp)
	    else:
		new_list.append(exp)
	elif isinstance(exp, expression.Function):
	    tmp_exp = genFuncExp(exp, table_name)
	    new_list.append(tmp_exp)

def genJoinWhere(exp, table_name):
    if not isinstance(exp, expression.Function):
	return None
    ret_exp = None
    if exp.func_name in bool_func_dic.keys():
	for para in exp.para_list:
	    if not isinstance(para, expression.Function):
		# TODO ERROR
		return
	    tmp_exp = genJoinWhere(para, table_name)
	    if ret_exp is None:
		ret_exp = tmp_exp
	    else:
		para_list = []
		para_list.append(ret_exp)
		para_list.append(tmp_exp)
		ret_exp = expression.Function(exp.func_name, para_list)
	return ret_exp
   else:
	flag = True
	para1 = exp.para_list[0]
	if isinstance(para1, expression.Column) and para1.table_name != table_name:
	    flag = False
	if exp.func_name != "IS":
	    para2 = exp.para_list[1]
	    if isinstance(para2, expression.Column) and para2.table_name != table_name:
		flag = False
	if flag:
	    ret_exp = copy.deepcopy(exp)
	else:
	    ret_exp = expression.Column("FALSE", "BOOLEAN")
    return ret_exp

def genJoinReduceCode(node, left_name, fo):
    line_buf = "line_buf"
    if node.select_list is None:
	# TODO ERROR
	return
    # TODO self join
    left_key_list = []
    right_key_list = []
    if node.is_explicit:
	getJoinKey(node.join_condition.on_condition_exp, left_key_list, "LEFT")
	getJoinKey(node.join_condition.on_condition_exp, right_key_list, "RIGHT")
    elif node.is_explicit:
	getJoinKey(node.join_condition.where_condition_exp, left_key_list, "LEFT")
	getJoinKey(node.join_condition.where_condition_exp, right_key_list, "RIGHT")
    if len(left_key_list) == 0:
	left_key_list.append(expression.Constant(1, "INTEGER"))
    if len(right_key_list) == 0:
	right_key_list.append(expression.Constant(1, "INTEGER"))
    left_key_type = getKeyValueType(left_key_list)
    right_key_type = getKeyValueType(right_key_list)
    if left_key_type != right_key_type:
	# TODO ERROR
	return
    map_key_type = left_key_type
    map_value_type = "Text"
    # TODO

def genOpCode(op, fo):
    line_buf = "line_buf"
    map_key_type = ""
    
    print >> fo,"\tpublic static class Map extends  Mapper<Object, Text, " + map_key_type + ", " + map_value_type + ">{\n"
    print >> fo, "\t\tprivate String filename;"
    print >> fo, "\t\tprivate int filetag = -1;"
    print >> fo, "\t\tpublic void setup(Context context) throws IOException, InterruptedException {\n"
    print >> fo, "\t\t\tint last_index = -1, start_index = -1;"
    print >> fo, "\t\t\tString path = ((FileSplit)context.getInputSplit()).getPath().toString();"
    print >> fo, "\t\t\tlast_index = path.lastIndexOf(\'/\');"
    print >> fo, "\t\t\tlast_index = last_index - 1;"
    print >> fo, "\t\t\tstart_index = path.lastIndexOf(\'/\', last_index);"
    print >> fo, "\t\t\tfilename = path.substring(start_index + 1, last_index + 1);"
    for table_name in tb_name_tag.keys():
        print >> fo, "\t\t\tif (filename.compareTo(\"" + table_name + "\") == 0){"
        print >> fo, "\t\t\t\tfiletag = " + str(tb_name_tag[tn]) + ";"
        print >> fo, "\t\t\t}"
    print >> fo, "\t\t}\n"

    print >> fo, "\t\tpublic void map(Object key, Text value, Context context) throws IOException, InterruptedException{\n"
    print >> fo, "\t\t\tString line = value.toString();"
    print >> fo, "\t\t\tString[] " + line_buf + "= line.split(\"\\\|\");"
    print >> fo, "\t\t\tBitSet dispatch = new BitSet(32);"

    for table_name in op.map_output.keys():
	map_key = genMRKeyValue(node.pk_dic[table_name][0], map_key_type, buf_dic)
	print >> fo, "\t\t\tif (filetag == )" + str(tb_name_tag[table_name]) + "){\n"
	map_value = genMRKeyValue(node.map_output[table_name], map_value_type, buf_dic)

	map_filter = {}
	for key in op.map_filter.keys():
	    where_exp = None
	    for value in op.map_filter[key]:
		if where_exp is None:
		    where_exp = value
		else:
		    para_list = []
		    para_list.append(where_exp)
		    para_list.append(value)
		    where_exp = expression.Function("OR", para_list)
	    map_filter[key] = where_exp
	if table_name in op.map_filter.keys():
	    print >> fo, "\t\t\t\tif (" + genWhereExpCode(map_filter[table_name], buf_dic) + "){\n"

	    for map_node in op.map_phase:
		if isinstance(map_node, node.GroupbyNode):
		    # TODO
		elif isinstance(map_node, node.JoinNode):
		    # TODO
		    if isinstance(map_node.left_child, node.SPNode):
			# TODO
		    if isinstance(map_node.right_child, node.SPNode):
			# TODO
