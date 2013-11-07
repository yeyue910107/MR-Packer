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
import ast
import node
import schema
import util
import op
import codegen
import sys
import os

if __name__ == "__main__":
    pwd = os.getcwd()
    _file = open("../test/17.xml", "r")
    schema = "../test/tpch.schema"
    pt = ast.astToQueryPlan(schema, _file)
    _file.close()
    mrq = node.planTreeToMRQ(pt)
    mrq.optimize()
    mrq.postProcess()
    '''_run = False
    if len(sys.argv) == 3 and sys.argv[2] == "r":
	_run = True
    mrq.getLowCostMRQ(sys.argv[1], _run)'''
    filename = "testquery"
    #op_list = codegen.genCode(mrq, filename)
    #id_list = [op.getID() for op in op_list]
    #print id_list
    codegen.run(mrq, filename)
    #_file.close()
