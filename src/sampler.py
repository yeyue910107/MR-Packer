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
import codegen
import hadoopy
import subprocess
import job

'''
{
"MAP_INPUT_BYTES":0,
"MAP_INPUT_RECORDS":0,
"MAP_OUTPUT_BYTES":0,
"MAP_OUTPUT_RECORDS":0,
"REDUCE_INPUT_BYTES":0,
"REDUCE_INPUT_RECORDS":0,
"REDUCE_OUTPUT_BYTES":0,
"REDUCE_OUTPUT_RECORDS":0
}
'''

ratio = config.ratio
tmp_path = config.hadoop_tmp_dir
fs_tmp_path = config.fs_tmp_dir
tmp_log_dir = config.tmp_log_dir
data_dir = config.data_dir

def doSample(jarfile, inputs, output, k):
    for item in inputs:
        if item[-1] == "/":
            name = (item[:-1]).split('/')[-1]
        else:
            name = item.split('/')[-1]
        print "item", item 
        #tmp_dir = tmp_path + name + "/"
        if hadoopy.exists(item):
            continue
        hadoopy.mkdir(item)
        #tmp_inputs.append(tmp_dir)
        real_input = data_dir + name + "/"
        for f in hadoopy.ls(real_input):
            if not hadoopy.isdir(f):
                #ff = tmp_dir + f.split('/')[-1]
                if k > 0:
                    poolSample(f, item, k)
                else:
                    commonSample(f, item, ratio)
    '''if not hadoopy.exists(output):
        hadoopy.mkdir(output)
    if hadoopy.isdir(output):
        output = output[:-1]
    if output[-1] == '/':
        output = output[:-1]
    name = output.split('/')[-1]
    tmp_output = tmp_path + name + "/"'''
    #if not hpath.exists(tmp_output):
    #    hdfs.mkdir(tmp_output)
    codegen.executeJar(jarfile, inputs, output)
    #jobid = job.getJobIDFromLog(tmp_log_dir)
    job_para = job.getJobPara()
    '''for item in tmp_inputs:
        os.system("hadoop fs -rmr " + item)
    os.system("hadoop fs -rmr " + tmp_output)'''
    return job_para

def commonSample(fip, fop, ratio):
    tmpfip = fs_tmp_path + fip.split('/')[-1]
    tmpfop = fs_tmp_path + "sample_" + (fop[:-1]).split('/')[-1]
    #print "hadoop fs -get " + fip + " " + tmpfip
    code = os.system("hadoop fs -get " + fip + " " + tmpfip)
    print "code", code
    #child = subprocess.Popen("hadoop fs -get " + fip + " " + tmpfip, stderr=subprocess.PIPE, shell=True)
    #child.wait()
    fi = open(tmpfip, "r")
    fo = open(tmpfop, "w")
    count = 0
    for f in fi:
        count += 1
    k = int(count * ratio)
    print "count, ratio, k", count, ratio, k
    line_list = random.sample(range(0, count), k)
    line_list.sort()
    #print "line_list", line_list
    fi.seek(0)
    i = 0
    count = 0
    results = []
    for f in fi:
        if i >= k:
            break
        if count == line_list[i]:
            results.append(f)
            i += 1
        count += 1
    fo.writelines(results)
    fi.close()
    fo.close()
    print "hadoop fs -put " + tmpfop + " " + fop
    code = os.system("hadoop fs -put " + tmpfop + " " + fop)
    #child = subprocess.Popen("hadoop fs -put " + tmpfop + " " + fop, stderr=subprocess.PIPE, shell=True)
    #child.wait()
    #os.system("rm " + tmpfip)
    #os.system("rm " + tmpfop)

def poolSample(fip, fop, k):
    tmpfip = fs_tmp_path + fip.split('/')[-1]
    tmpfip = fs_tmp_path + fip.split('/')[-1]
    tmpfop = fs_tmp_path + "sample_" + (fop[:-1]).split('/')[-1]
    #print "hadoop fs -get " + fip + " " + tmpfip
    code = os.system("hadoop fs -get " + fip + " " + tmpfip)
    print "code", code
    fi = open(tmpfip, "r")
    fo = open(tmpfop, "w")
    results = []
    i = 0
    for f in fi:
        if i < k:
            results.append(f)
        else:
            r = random.randrange(0, i)
            if r < k:
                results[r] = f
        i += 1

    fo.writelines(results)
    
    fi.close()
    fo.close()
    os.system("hadoop fs -put " + tmpfop + " " + fop)
    os.system("rm " + tmpfip)
    os.system("rm " + tmpfop)

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

