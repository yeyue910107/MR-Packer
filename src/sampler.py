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
import sys
import config
import os
import random
import pydoop.hdfs as hdfs
import pydoop.hdfs.path as hpath
import codegen

'''
{
"MAP_INPUT_BYTES":0,
"MAP_INPUT_RECORDS":0,
"REDUCE_INPUT_BYTES":0,
"REDUCE_INPUT_RECORDS":0,
"REDUCE_OUTPUT_BYTES":0,
"REDUCE_OUTPUT_RECORDS":0
}
'''

ratio = config.ratio
tmp_path = config.hadoop_tmp_dir

def doSample(jarfile, inputs, output, k):
    tmp_inputs = []
    for item in inputs:
        if hpath.isdir(item):
            item = item[:-1]
        name = hpath.basename(item)
        tmp_dir = tmp_path + name + "/"
        if not hpath.exists(tmp_dir):
            hdfs.mkdir(tmp_dir)
        tmp_inputs.append(tmp_dir)
        for f in hdfs.ls(item):
            if hpath.isfile(f):
                ff = tmp_dir + hpath.basename(f)
                if k > 0:
                    poolSample(f, ff, k)
                else:
                    commonSample(f, ff, ratio)
    if not hpath.exists(output):
        hdfs.mkdir(output)
    if hpath.isdir(output):
        output = output[:-1]
    name = hpath.basename(output)
    tmp_output = tmp_path + name + "/"
    #if not hpath.exists(tmp_output):
    #    hdfs.mkdir(tmp_output)
    codegen.executeJar(jarfile, tmp_inputs, tmp_output)

def commonSample(fip, fop, ratio):
    fi = hdfs.open(fip, "r")
    fo = hdfs.open(fop, "w")
    count = 0
    line = fi.readline()
    while line != "":
        count += 1
        line = fi.readline()
    k = int(count * ratio)
    print "count, ratio, k", count, ratio, k
    line_list = random.sample(range(0, count), k)
    line_list.sort()
    #print "line_list", line_list
    
    i = 0
    count = 0
    fi.seek(0)
    line = fi.readline()
    while line != "" and i < k:
        if count == line_list[i]:
            fo.write(line)
            i += 1
        count += 1
        line = fi.readline()

    fi.close()
    fo.close()

def poolSample(fip, fop, k):
    fi = hdfs.open(fip, "r")
    fo = hdfs.open(fop, "w")

    results = []
    i = 0
    line = fi.readline()
    while line != "":
        if i < k:
            results.append(line)
        else:
            r = random.randrange(0, i)
            if r < k:
                results[r] = line
        i += 1
        line = fi.readline()

    for line in results:
        fo.write(line)
    
    fi.close()
    fo.close()

def _sample(_file, k):
    f = open(_file, "r")
    tmp_file = open("tmp_file", "w")
    
    results = []
    i = 0
    for line in f:
	if i < k:
	    results.append(line)
        else:
            r = random.randrange(0, i)
	    if r < k:
		results[r] = line
        i += 1
    tmp_file.writelines(results)
    f.close()
    tmp_file.close()

if __name__ == "__main__":
    #sample(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    num = len(sys.argv)
    if num >= 5:
        jarfile = sys.argv[1]
        input_paths = []
        for i in range(2, num-2):
            input_paths.append(sys.argv[i])
        output_path = sys.argv[num-2]
        k = int(sys.argv[num-1])
        doSample(jarfile, input_paths, output_path, k)

