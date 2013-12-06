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

code_dir = "./code/"
jar_dir = "./jar/"
script = "./testscript"

ratio = 0.4

hadoop_tmp_dir = "/tmp/sample/"
hadoop_core_dir = "/home/hadoop/hadoop/hadoop-core-*.jar"
data_dir = "/test/mrpacker/"

optimize = True
run = True
costmodel = True

history_dir = "/tmp/hadoop-yarn/staging/history/done_intermediate/hadoop23/"
log_dir = "./logs/"
tmp_log_dir = log_dir + "tmp_log"
fs_tmp_dir = "./tmp/"
