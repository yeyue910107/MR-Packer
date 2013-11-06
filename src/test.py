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

    '''codedir = "./code"
    jardir = "./jar"
    if os.path.exists(codedir) or os.path.exists(jardir):
        pass
    else:
        os.makedirs(codedir)
        os.makedirs(jardir)
    os.chdir(codedir)
    codegen.genCode(mrq, "testquery")
    os.chdir(pwd)
    os.chdir(jardir)
    fo = open(config.scriptname,'w')
    compile_class(codedir,fo)
    generate_jar(jardir, )
    os.chdir(pwd)
    #mrq.postProcess()
    #mrq.getLowCostMRQ(sys.argv[1])'''
    filename = "testquery"
    #codegen.genCode(mrq, filename)
    codegen.run(mrq, filename)
    #_file.close()
