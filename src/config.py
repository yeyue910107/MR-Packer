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

# directories for result
code_dir = "./code/"
jar_dir = "./jar/"
script = "./testscript"

# hadoop configuration
hadoop_core_dir = "/home/hadoop/hadoop/hadoop-core-*.jar"
data_dir = "/test/mrpacker/"    # default input and output dir

# optimizer configuration
optimize = True    # If True, use optimized algorithm.
run = False        # If True, execute the generated jobs on Hadoop. If False, just generate script,codes and jars.
costmodel = False  # If True, use job analyzer with cost model.

# job analyzer configuration
ratio = 0.4    # sample ratio
hadoop_tmp_dir = "/tmp/sample/"    # hdfs tmp dir
history_dir = "/tmp/hadoop-yarn/staging/history/done_intermediate/hadoop23/"    # job history dir
log_dir = "./logs/"    # local dir for logs
tmp_log_dir = log_dir + "tmp_log"
fs_tmp_dir = "./tmp/"    # local tmp dir
