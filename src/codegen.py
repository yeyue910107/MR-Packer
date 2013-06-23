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

def genJoinCode(node, left_name, fo):
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

def isSelfJoin(node):
    #TODO
    return false

def genOpCode(op, fo):
    line_buf = "line_buf"
    map_key_type = ""

    # get op input tables
    tb_name_tag = {}
    index_name = {}

    filename = fo.name.split(".java")[0]
    [file_name, file_index] = filename.split("_")
   
    for child in op.child_list:
	index = int(file_index) + op.child_list.index(node) + 1
	tb_name = file_name + "_" + str(index)
	index_name[index] = tb_name
	op.pk_dic[tb_name] = copy.deepcopy(op.pk_dic[index])
	del op.pk_dic[index]
	for exp in op.pk_dic[tb_name][0]:
	    exp.table_name = tb_name
	exp_list = child.oc_list[-1].select_list.exp_list
	tmp_exp_list = []
	for exp in exp_list:
	    tmp_index = exp_list.index(exp)
	    new_exp = expression.Column(tb_name, tmp_index)
	    new_exp.table_name = tb_name
	    new_exp.column_name = int(new_exp.column_name)
	    new_exp.column_type = exp.column_type
	    tmp_exp_list.append(new_exp)

	op.map_output[tb_name] = tmp_exp_list

    # get map key value type
    buf_dic = {}
    i = 1
    for tb in op.map_output.keys():
	tb_name_tag[tb] = i
	buf_dic[tb] = line_buf
	map_key_type = getKeyValueType(op.pk_dic[tb][0])
	i += 1
    map_value_type = "Text"
    
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
		    if table_name in map_node.table_list and map_node.child.where_condition is not None:
			where_exp = map_node.child.where_condition.where_condition_exp
			print >> fo, "\t\t\t\t\tif (!(" + genWhereExpCode(where_exp, buf_dic) + "))"
			print >> fo, "\t\t\t\t\t\tdispatch.set(", op.map_phase.index(map_node),");"
		elif isinstance(map_node, node.JoinNode):
		    self_join_flag = isSelfJoin(map_node)
		    if isinstance(map_node.left_child, node.SPNode):
			if table_name == map_node.left_child.table_name and map_node.left_child.where_condition is not None:
			    where_exp = map_node.left_child.where_condition.where_condition_exp
			    print >> fo, "\t\t\t\t\tif (!(" + genWhereExpCode(where_exp, buf_dic) + "))"
			    if self_join_flag is False:
				print >> fo, "\t\t\t\t\t\tdispatch.set(" + op.map_phase.index(map_node) + ");"
			    else:
				print >> fo, "\t\t\t\t\t\tdispatch.set(", 16+op.map_phase.index(map_node), ");"
		    if isinstance(map_node.right_child, node.SPNode):
			if table_name == map_node.right_child.table_name and map_node.right_child.where_condition is not None:
			    where_exp = map_node.right_child.where_condition.where_condition_exp
			    print >> fo, "\t\t\t\t\tif (!(" + genWhereExpCode(where_exp, buf_dic) + "))"
			    if self_join_flag is False:
				print >> fo, "\t\t\t\t\t\tdispatch.set(" + op.map_phase.index(map_node) + ");"
			    else:
				print >> fo, "\t\t\t\t\t\tdispatch.set(", 16+op.map_phase.index(map_node), ");"

	    print >> fo, "\t\t\t\t\tif (dispatch.isEmpty())"
	    output = "\t\t\t\t\t\tcontext.write("
	    output += "new " + map_key_type + "(" + map_key + ")"
	    output += ", "
	    output += "new " + map_value_type + "(" + str(tb_name_tag[table_name]) + "+\"||\"+" + map_value + ")"
	    output += ");"
	    print >> fo, output

	    print >> fo, "\t\t\t\t\telse"
	    output = "\t\t\t\t\t\tcontext.write("
	    output += "new " + map_key_type + "(" + map_key + ")"
	    output += ", "
	    output += "new " + map_value_type + "(" + str(tb_name_tag[table_name]) + "+\"|\"+dispatch.toString()+\"|\"+" + map_value + ")"
	    output += ");"
	    print >> fo, output
	    print >> fo, "\t\t\t\t}"
	
	else:
	    output = "\t\t\t\tcontext.write("
	    output += "new " + map_key_type + "(" + map_key + ")"
	    output += ", "
	    output += "new " + map_value_type + "(" + str(tb_name_tag[table_name]) + "+\"||\"+" + map_value + ")"
	    output += ");"
	    print >> fo, output

	print >> fo, "\t\t\t}\n"

    print >> fo, "\t\t}\n"
    print >> fo, "\t}\n"

    reduce_key_type = "NullWritable"
    reduce_value_type = "Text"

    print >> fo, "\tpublic static class Reduce extends Reducer<" + map_key_type + ", " + map_value_type + ", " + reduce_key_type + ", " + reduce_value_type + "> {\n"
    print >> fo, "\t\tpublic void reduce(" + map_key_type + " key, Iterable<" + map_value_type + "> v, Context context) throws IOExceptiuon, InterruptedException {\n"
    
    print >> fo, "\t\t\tIterator values = v.iterator();"
    print >> fo, "\t\t\tArrayList[] tmp_output = new ArrayList[" + str(len(op.reduce_phase)) + "];"
    print >> fo, "\t\t\tfor (int i = 0; i < )" + str(len(op.reduce_phase)) + "; i++) {\n"
    print >> fo, "\t\t\t\ttmp_output[i] = new ArrayList();"
    print >> fo ,"\t\t\t}"

    agg_buf = "result"
    d_count_buf = "d_count_buf"
    line_counter = "al_line"
    left_array = "al_left"
    right_array = "al_right"

    print >> fo, "\t\t\tString tmp = \"\";"
    for reduce_node in op.reduce_phase:
	    reduce_node_index = str(op.reduce_phase.index(reduce_node))
	if isinstance(reduce_node, node.GroupbyNode):
	    gb_exp_list = []
	    getGroupbyExpList(reduce_node.select_list.exp_list, gb_exp_list)
	    tmp_agg_buf = agg_buf + "_" + reduce_node_index
	    tmp_count_buf = d_count_buf + "_" + reduce_node_index
	    tmp_line_counter = line_counter + "_" + reduce_node_index

	    print >> fo, "\t\t\tDouble[] " + tmp_agg_buf + " = new Double[" + str(len(gb_exp_list)) + "];"
	    print >> fo, "\t\t\tArrayList[] " + tmp_count_buf + " = new ArrayList[" + str(len(gb_exp_list)) + "];"
	    print >> fo, "\t\t\tint " + tmp_line_counter + " = 0;"
	    print >> fo, "\t\t\tfor (int i = 0; i < " + str(len(gb_exp_list)) + "; i++) {\n"
	    print >> fo, "\t\t\t\t" + tmp_agg_buf + "[i] = 0.0;"
	    print >> fo, "\t\t\t\t" + tmp_count_buf + "[i] = new ArrayList();"
	    print >> fo, "\t\t\t}\n"

	elif isinstance(reduce_node, node.JoinNode):
	    tmp_left_array = left_array + "_" + reduce_node_index
	    tmp_right_array = right_array + "_" + reduce_node_index
	    print >> fo, "\t\t\tArrayList " + tmp_left_array + " = new ArrayList();"
	    print >> fo, "\t\t\tArrayList " + tmp_right_array + " = new ArrayList();"

    # iterate each value
    print >> fo, "\t\t\twhile (values.hasNext()) {\n"
    print >> fo, "\t\t\t\tString line = values.next().toString();"
    print >> fo, "\t\t\t\tString dispatch = line.split(\"\\\|\")[1];"
    print >> fo, "\t\t\t\ttmp = line.substring(2+dispatch.length()+1);"
    print >> fo, "\t\t\t\tString[] " + line_buf + " = tmp.split(\"\\\|\");"

    for reduce_node in op.reduce_phase:
	if reduce_node is not in op.ic_list:
	    continue
	reduce_node_index = str(op.reduce_phase.index(reduce_node))
	if isinstance(reduce_node, node.GroupbyNode):
	    tmp_agg_buf = agg_buf + "_" + reduce_node_index
	    tmp_count_buf = d_count_buf + "_" + reduce_node_index
	    tmp_line_counter = line_counter + "_" + reduce_node_index
	    gb_exp_list = []
	    getGroupbyExpList(reduce_node.select_list.exp_list, gb_exp_list)
	    table_name = reduce_node.child.table_name
	    
	    print >> fo, "\t\t\t\tif (line.charAt(0) == '" + str(tb_name_tag[table_name]) + "' && (dispatch.length() == 0 || dispatch.indexOf('" + reduce_node_index + "') == -1)){\n"

	    for exp in gb_exp_list:
		exp_index = str(gb_exp_list.index(exp))
		if not isinstance(exp, expression.Function):
		    # TODO error
		    return
		select_func_output = genSelectFunctionCode(exp, buf_dic)
		agg_func = exp.getGroupbyFuncName()
		if agg_func == "SUM" or agg_func == "AVG":
		    print >> fo, "\t\t\t\t\t" + tmp_agg_buf + "[" + exp_index + "] += " + select_func_output + ";"
		elif agg_func == "COUNT_DISTINCT":
		    print >> fo, "\t\t\t\t\tif (" + tmp_count_buf + "[" + exp_index + "].contains(" + select_func_output + ") == false)"
		    print >> fo, "\t\t\t\t\t\t" + tmp_count_buf + "[" + exp_index + "].add(" + select_func_output + ");"
		elif agg_func == "MAX":
		    print >> fo, "\t\t\t\t\tif (" tmp_line_counter + " == 0)"
		    print >> fo, "\t\t\t\t\t\t" + tmp_agg_buf + "[" + exp_index + "] = (double) " + select_func_output + ";"
		    print >> fo, "\t\t\t\t\telse if (" + tmp_agg_buf + "[" + exp_index + "] > " + select_func_output + ")"
		    print >> fo, "\t\t\t\t\t\t" + tmp_agg_buf + "[" + exp_index + "] = (double) " + select_func_output + ";"
		elif agg_func == "MIN":
		    print >> fo, "\t\t\t\t\tif (" tmp_line_counter + " == 0)"
		    print >> fo, "\t\t\t\t\t\t" + tmp_agg_buf + "[" + exp_index + "] = (double) " + select_func_output + ";"
		    print >> fo, "\t\t\t\t\telse if (" + tmp_agg_buf + "[" + exp_index + "] < " + select_func_output + ")"
		    print >> fo, "\t\t\t\t\t\t" + tmp_agg_buf + "[" + exp_index + "] = (double) " + select_func_output + ";"

	    print >> fo, "\t\t\t\t\t" + tmp_line_counter + "++;"
	    print >> fo, "\t\t\t\t}"

	elif isinstance(reduce_node, node.JoinNode):
	    self_join_flag = isSelfJoin(reduce_node)
	    reduce_node_index = str(op.map_phase.index(reduce_node))
	    tmp_left_array = left_array + "_" + reduce_node_index
	    tmp_right_array = right_array + "_" + reduce_node_index
	    if isinstance(reduce_node.left_child, node.TableNode):
		left_table_name = reduce_node.left_child.table_name
	    else:
		if len(op.child_list) != 2:
		    # TODO error
		    return
		left_table_name = index_name[op.child_list[0]]
	    
	    if_stat = "\t\t\t\tif (line.charAt(0) == '" + str(tb_name_tag[left_table_name]) + "' && (dispatch.length() == 0 || "
	    if self_join_flag is False:
		if_stat += "dispatch.indexOf(\"" + reduce_node_index + "\") == -1"
	    else:
		if_stat += "dispatch.indexOf(\"" + str(int(reduce_node_index)+16) + "\") == -1"
	    if_stat += "))"
	    print >> fo, if_stat
	    print >> fo, "\t\t\t\t\t" + tmp_left_array + ".add(tmp);"

	
	    if isinstance(reduce_node.right_child, node.TableNode):
		right_table_name = reduce_node.right_child.table_name
	    else:
		if len(op.child_list) != 2:
		    # TODO error
		    return
		right_table_name = index_name[op.child_list[1]]
	    
	    if_stat = "\t\t\t\tif (line.charAt(0) == '" + str(tb_name_tag[right_table_name]) + "' && (dispatch.length() == 0 || "
	    if self_join_flag is False:
		if_stat += "dispatch.indexOf(\"" + reduce_node_index + "\") == -1"
	    else:
		if_stat += "dispatch.indexOf(\"" + str(int(reduce_node_index)+16) + "\") == -1"
	    if_stat += "))"
	    print >> fo, if_stat
	    print >> fo, "\t\t\t\t\t" + tmp_right_array + ".add(tmp);"

    print >> fo, "\t\t\t}"

    print >> fo, "\t\t\tString[] " + line_buf + " = tmp.split(\"\\\|\");"
    for reduce_node in op.reduce_phase:
	if reduce_node is not in ic_list:
	    continue
	reduce_node_index = str(op.reduce_phase.index(reduce_node))
	if isinstance(reduce_node, node.GroupbyNode):
	    tmp_agg_buf = agg_buf + "_" + reduce_node_index
	    tmp_count_buf = d_count_buf + "_" + reduce_node_index
	    tmp_line_counter = line_counter + "_" + reduce_node_index

	    gb_exp_list = []
	    getGroupbyExpList(reduce_node.select_list.exp_list, gb_exp_list)
	    for exp in gb_exp_list:
		exp_index = gb_exp_list.index(exp)
		if not isinstance(exp, expression.Function):
		    # TODO error
		    return
		agg_func = str(exp.getGroupbyFuncName())
		if agg_func == "AVG":
		    print >> fo, "\t\t\t" + tmp_agg_buf + "[" + exp_index + "] = " + tmp_agg_buf + "[" + exp_index + "] / " + tmp_line_counter + ";"
		elif agg_func == "COUNT":
		    print >> fo, "\t\t\t" + tmp_agg_buf + "[" + exp_index + "] = (double) " + tmp_line_counter + ";"
		elif agg_func == "COUNT_DISTINCT":
		    print >> fo, "\t\t\t", tmp_agg_buf + "[" + exp_index + "] = (double) " + tmp_count_buf + "[" + exp_index + "].size();"

 	    # TODO
	elif isinstance(reduce_node, node.JoinNode):
	    tmp_left_array = left_array + "_" + reduce_node_index
	    tmp_right_array = right_array + "_" + reduce_node_index
	    buf_dic = {}
	    tmp_left_buf = "left_buf_" + reduce_node_index
	    tmp_right_buf = "right_buf_" + reduce_node_index
	    reduce_value_type = "Text"

	    if reduce_node.is_explicit:
		join_type = reduce_node.join_type.upper()
		if join_type == "LEFT":
		    reduce_value = genMRKeyValue(reduce_node.select_list.exp_list, reduce_value_type, buf_dic)
		    print >> fo, "\t\t\tfor (int i = 0; i < " + tmp_left_array + ".size(); i++) {"
		    print >> fo, "\t\t\t\tString[] " + tmp_left_buf + " = ((String) " + tmp_left_array + ".get(i)).split(\"\\\|\");"
		    print >> fo, "\t\t\t\tif (" + tmp_right_array + ".size() > 0) {"
		    print >> fo, "\t\t\t\t\tfor (int j = 0; j < " + tmp_right_array + ".size(); j++) {"
		    print >> fo, "\t\t\t\t\t\tString[] " + tmp_right_buf + " = ((String) " + tmp_right_array + ".get(j)).split(\"\\\|\");"
		    if reduce_node.where_condition is not None:
			exp = reduce_node.where_condition.where_condition_exp
			print >> fo, "\t\t\t\t\t\tif (" + genWhereExpCode(exp, buf_dic) + "( {"
			print >> fo, "\t\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
			print >> fo, "\t\t\t\t\t\t}"
		    else:
			print >> fo, "\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
		    print >> fo, "\t\t\t\t\t}"

		    print >> fo, "\t\t\t\t} else {"
		    new_list = []
		    genJoinList(reduce_node.select_list.exp_list, new_list, "LEFT")
		    reduce_value = genMRKeyValue(new_list, reduce_value_type, buf_dic)
		    if reduce_node.where_condition is not None:
			new_where = genJoinWhere(reduce_node.where_condition.where_condition_exp, "LEFT")
			print >> fo, "\t\t\t\t\t\tif (" + genWhereExpCode(new_where, buf_dic) + ") {"
			print >> fo, "\t\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
			print >> fo, "\t\t\t\t\t\t}"
		    else:
			print >> fo, "\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"

		    print >> fo, "\t\t\t\t}"
		    print >> fo, "\t\t\t}"
  
		elif join_type == "RIGHT":
		    reduce_value = genMRKeyValue(reduce_node.select_list.exp_list, reduce_value_type, buf_dic)
		    print >> fo, "\t\t\tfor (int i = 0; i < " + tmp_right_array + ".size(); i++) {"
		    print >> fo, "\t\t\t\tString[] " + tmp_right_buf + " = ((String) " + tmp_right_array + ".get(i)).split(\"\\\|\");"
		    print >> fo, "\t\t\t\tif (" + tmp_left_array + ".size() > 0) {"
		    print >> fo, "\t\t\t\t\tfor (int j = 0; j < " + tmp_left_array + ".size(); j++) {"
		    print >> fo, "\t\t\t\t\t\tString[] " + tmp_left_buf + " = ((String) " + tmp_left_array + ".get(j)).split(\"\\\|\");"
		    if reduce_node.where_condition is not None:
			exp = reduce_node.where_condition.where_condition_exp
			print >> fo, "\t\t\t\t\t\tif (" + genWhereExpCode(exp, buf_dic) + "( {"
			print >> fo, "\t\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
			print >> fo, "\t\t\t\t\t\t}"
		    else:
			print >> fo, "\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
		    print >> fo, "\t\t\t\t\t}"

		    print >> fo, "\t\t\t\t} else {"
		    new_list = []
		    genJoinList(reduce_node.select_list.exp_list, new_list, "RIGHT")
		    reduce_value = genMRKeyValue(new_list, reduce_value_type, buf_dic)
		    if reduce_node.where_condition is not None:
			new_where = genJoinWhere(reduce_node.where_condition.where_condition_exp, "RIGHT")
			print >> fo, "\t\t\t\t\t\tif (" + genWhereExpCode(new_where, buf_dic) + ") {"
			print >> fo, "\t\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
			print >> fo, "\t\t\t\t\t\t}"
		    else:
			print >> fo, "\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"

		    print >> fo, "\t\t\t\t}"
		    print >> fo, "\t\t\t}"
		
		elif join_type == "FULL":
		    reduce_value = genMRKeyValue(reduce_node.select_list.exp_list, reduce_value_type, buf_dic)
		    print >> fo, "\t\t\tif (" + tmp_left_array + ".size() > 0 && " + tmp_right_array + (".size() > 0) {")
		    print >> fo, "\t\t\t\tfor (int i = 0; i < " + tmp_right_array + ".size(); i++) {"
		    print >> fo, "\t\t\t\t\tString[] " + tmp_right_buf + " = ((String) " + tmp_right_array + ".get(i)).split(\"\\\|\");"
		    print >> fo, "\t\t\t\t\tfor (int j = 0; j < " + tmp_left_array + ".size(); j++) {"
		    print >> fo, "\t\t\t\t\t\tString[] " + tmp_left_buf + " = ((String) " + tmp_left_array + ".get(j)).split(\"\\\|\");"
		    if reduce_node.where_condition is not None:
			exp = reduce_node.where_condition.where_condition_exp
			print >> fo, "\t\t\t\t\t\tif (" + genWhereExpCode(exp, buf_dic) + "( {"
			print >> fo, "\t\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
			print >> fo, "\t\t\t\t\t\t}"
		    else:
			print >> fo, "\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
		    print >> fo, "\t\t\t\t\t}"
		    print >> fo, "\t\t\t\t}"

		    print >> fo, "\t\t\t}  else if (" + tmp_left_array + ".size() > 0) {"
		    print >> fo, "\t\t\t\tfor (int i = 0; i < )" + tmp_left_array + ".size(); i++) {"
		    new_list = []
		    genJoinList(reduce_node.select_list.exp_list, new_list, "LEFT")
		    reduce_value = genMRKeyValue(new_list, reduce_value_type, buf_dic)
		    if reduce_node.where_condition is not None:
			new_where = genJoinWhere(reduce_node.where_condition.where_condition_exp, "LEFT")
			print >> fo, "\t\t\t\t\t\tif (" + genWhereExpCode(new_where, buf_dic) + ") {"
			print >> fo, "\t\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
			print >> fo, "\t\t\t\t\t\t}"
		    else:
			print >> fo, "\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"

		    print >> fo, "\t\t\t\t}"

		    print >> fo, "\t\t\t}  else if (" + tmp_right_array + ".size() > 0) {"
		    print >> fo, "\t\t\t\tfor (int i = 0; i < )" + tmp_right_array + ".size(); i++) {"
		    new_list = []
		    genJoinList(reduce_node.select_list.exp_list, new_list, "RIGHT")
		    reduce_value = genMRKeyValue(new_list, reduce_value_type, buf_dic)
		    if reduce_node.where_condition is not None:
			new_where = genJoinWhere(reduce_node.where_condition.where_condition_exp, "RIGHT")
			print >> fo, "\t\t\t\t\t\tif (" + genWhereExpCode(new_where, buf_dic) + ") {"
			print >> fo, "\t\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
			print >> fo, "\t\t\t\t\t\t}"
		    else:
			print >> fo, "\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"

		    print >> fo, "\t\t\t\t}"

		    print >> fo, "\t\t\t}"

	    else:
		print >> fo, "\t\t\tfor (int i = 0; i < " + tmp_left_array + ".size(); i++) {"
		print >> fo, "\t\t\t\tString[] " + tmp_left_buf + " = ((String) " + tmp_left_array + ".get(i)).split(\"\\\|\");"
		print >> fo, "\t\t\t\tfor (int j = 0; j < " + tmp_right_array + ".size(); j++) {"
		print >> fo, "\t\t\t\t\tString[] " + tmp_right_buf + " = ((String) " + tmp_right_array + ".get(j)).split(\"\\\|\");"
		reduce_value = genMRKeyValue(reduce_node.select_list.exp_list, reduce_value_type, buf_dic)
		if reduce_node.where_condition is not None:
		    exp = reduce_node.where_condition.where_condition_exp
		    print >> fo, "\t\t\t\t\tif (" + genWhereExpCode(exp, buf_dic) + ") {"
		    print >> fo, "\t\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"
		    print >> fo, "\t\t\t\t\t}"
		else:
		    print >> fo, "\t\t\t\t\ttmp_output[" + reduce_node_index + "].add(" + reduce_value + ");"

		print >> fo, "\t\t\t\t}"
		print >> fo, "\t\t\t}"

	# TODO
	for reduce_node in op.reduce_phase:
	    if reduce_node not in op.oc_list:
		continue
	    if isinstance(reduce_node, node.GroupbyNode):
		reduce_node_index = op.reduce_phase.index(reduce_node.child)
		tmp_gb_input = "tmp_output[" + str(reduce_node_index) + "]"
		gb_exp_list = []
		getGroupbyExpList(reduce_node.select_list.exp_list, gb_exp_list)
		
		key_len = len(reduce_node.groupby_clause.groupby_exp_list)
		tmp_output_len = len(gb_exp_List)
		tmp_gb_output = "tmp_gb_output_" + str(op.reduce_phase.index(reduce_node))
		tmp_dc_output = "tmp_dc_output_" + str(op.reduce_phase.index(reduce_node))
		tmp_count_output = "tmp_count_output_" + str(op.reduce_phase.index(reduce_node))
		print >> fo, "\t\t\tHashtable<String, Double>[] " + tmp_gb_output + " = new Hashtable<String, Double>[" + str(tmp_output_len) + "]();"
		print >> fo, "\t\t\tHashtable<String, ArrayList>[] " + tmp_dc_output + " = new Hashtable<String, ArrayList>[" + str(tmp_output_len) + "]();"
		print >> fo, "\t\t\tHashtable<String, Integer>[] " + tmp_count_output + " = new Hashtable<String, Integer>[" + str(tmp_output_len) + "]();"
		print >> fo, "\t\t\tfor (int i = 0; i < " + tmp_output_len + "; i++) {"
		print >> fo, "\t\t\t\t" + tmp_gb_output + "[i] = new Hashtable<String, Double>();"
		print >> fo, "\t\t\t\t" + tmp_dc_output + "[i] = new Hashtable<String, ArrayList>();"
		print >> fo, "\t\t\t\t" + tmp_count_output + "[i] = new Hashtable<String, Integer>();"
		print >> fo, "\t\t\t}"
		print >> fo, "\t\t\tfor (int i = 0; i < " + tmp_gb_input + ".size(); i++) {"
		print >> fo, "\t\t\t\tString[] tmp_buf = ((String)" + tmp_gb_input + ".get(i)).split(\"\\|\");"
		
		tmp_key = ""
		for i in range(0, key_len):
		    exp = gb_exp_list[i]
		    func_name = 

	

    types = { "map_key_type":map_key_type, "map_value_type":map_value_type, "reduce_key_type":reduce_key_type, "reduce_value_type":reduce_value_type }
    genMainCode(op, fo, types, True)

def genMainCode(op, fo, types, has_reduce):
    print >> fo, "\tpublic int run(String[] args) throws Exception {\n"
    job_name = fo.name.split(".java")[0]

    print >> fo, "\t\tConfiguration conf = new Configuration();"
    print >> fo, "\t\tJob job = new Job(conf, \"" + job_name + "\");"
    print >> fo, "\t\tjob.setJarByClass(" + job_name + ".class);"
    print >> fo, "\t\tjob.setMapOutputKeyClass(" + types["map_key_type"] + ".class);"
    print >> fo, "\t\tjob.setMapOutputValueClass(" + types["map_value_type"] + ".class);"
    print >> fo, "\t\tjob.setOutputKeyClass(" + types["reduce_key_type"] + ".class);"
    print >> fo, "\t\tjob.setOutputValueClass(" + types["reduce_value_type"] + ".class);"
    print >> fo, "setMapperClass(Map.class);"
    if has_reduce:
	print >> fo, "\t\tjob.setReduceClass(Reduce.class);"
    input_num = len(op.map_output.keys())
    for i in range(0, input_num):
	print >> fo, "\t\tFileOutputFormat.addInputPath(job, new Path(args[" + str[i] + "]));"
    print >> fo, "\t\tFileOutput.setOutputPath(job, new Path(args[" + str(input_num) + "]));"
    print >> fo, "\t\treturn (job.waitForCompletion) ? 0 : 1);"
    print >> fo, "\t}\n"

    print >> fo, "\tpublic static void main(String[] args) throws Exception {\n"
    print >> fo, "\t\tint res = ToolRunner.run(new Configuration(), new " + job_name + "(), args);"
    print >> fo, "\t\tSystem.exit(res);"
    print >> fo, "\t}\n"

