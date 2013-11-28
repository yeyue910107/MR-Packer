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
import json

mapCounterTitle='''FILE_BYTES_READ,FILE: Number of bytes read,FILE_BYTES_WRITTEN,FILE: Number of bytes written,FILE_READ_OPS,FILE: Number of read operations,FILE_LARGE_READ_OPS,FILE: Number of large read operations,FILE_WRITE_OPS,FILE: Number of write operations,HDFS_BYTES_READ,HDFS: Number of bytes read,HDFS_BYTES_WRITTEN,HDFS: Number of bytes written,HDFS_READ_OPS,HDFS: Number of read operations,HDFS_LARGE_READ_OPS,HDFS: Number of large read operations,HDFS_WRITE_OPS,HDFS: Number of write operations,MAP_INPUT_RECORDS,Map input records,MAP_OUTPUT_RECORDS,Map output records,MAP_OUTPUT_BYTES,Map output bytes,MAP_OUTPUT_MATERIALIZED_BYTES,Map output materialized bytes,SPLIT_RAW_BYTES,Input split bytes,COMBINE_INPUT_RECORDS,Combine input records,SPILLED_RECORDS,Spilled Records,FAILED_SHUFFLE,Failed Shuffles,MERGED_MAP_OUTPUTS,Merged Map outputs,GC_TIME_MILLIS,GC time elapsed (ms),CPU_MILLISECONDS,CPU time spent (ms),PHYSICAL_MEMORY_BYTES,Physical memory (bytes) snapshot,VIRTUAL_MEMORY_BYTES,Virtual memory (bytes) snapshot,COMMITTED_HEAP_BYTES,Total committed heap usage (bytes),BYTES_READ,Bytes Read,'''

redCounterTitle='''FILE_BYTES_READ,FILE: Number of bytes read,FILE_BYTES_WRITTEN,FILE: Number of bytes written,FILE_READ_OPS,FILE: Number of read operations,FILE_LARGE_READ_OPS,FILE: Number of large read operations,FILE_WRITE_OPS,FILE: Number of write operations,HDFS_BYTES_READ,HDFS: Number of bytes read,HDFS_BYTES_WRITTEN,HDFS: Number of bytes written,HDFS_READ_OPS,HDFS: Number of read operations,HDFS_LARGE_READ_OPS,HDFS: Number of large read operations,HDFS_WRITE_OPS,HDFS: Number of write operations,COMBINE_INPUT_RECORDS,Combine input records,COMBINE_OUTPUT_RECORDS,Combine output records,REDUCE_INPUT_GROUPS,Reduce input groups,REDUCE_SHUFFLE_BYTES,Reduce shuffle bytes,REDUCE_INPUT_RECORDS,Reduce input records,REDUCE_OUTPUT_RECORDS,Reduce output records,SPILLED_RECORDS,Spilled Records,SHUFFLED_MAPS,Shuffled Maps ,FAILED_SHUFFLE,Failed Shuffles,MERGED_MAP_OUTPUTS,Merged Map outputs,GC_TIME_MILLIS,GC time elapsed (ms),CPU_MILLISECONDS,CPU time spent (ms),PHYSICAL_MEMORY_BYTES,Physical memory (bytes) snapshot,VIRTUAL_MEMORY_BYTES,Virtual memory (bytes) snapshot,COMMITTED_HEAP_BYTES,Total committed heap usage (bytes),BAD_ID,BAD_ID,CONNECTION,CONNECTION,IO_ERROR,IO_ERROR,WRONG_LENGTH,WRONG_LENGTH,WRONG_MAP,WRONG_MAP,WRONG_REDUCE,WRONG_REDUCE,BYTES_WRITTEN,Bytes Written,'''

jobCounterTitle='''JobName,JobId,FILE_BYTES_READ,FILE_BYTES_WRITTEN,FILE_READ_OPS,\
FILE_LARGE_READ_OPS,FILE_WRITE_OPS,HDFS_BYTES_READ,HDFS_BYTES_WRITTEN,\
HDFS_READ_OPS,HDFS_LARGE_READ_OPS,HDFS_WRITE_OPS,TOTAL_LAUNCHED_MAPS,\
TOTAL_LAUNCHED_REDUCES,DATA_LOCAL_MAPS,SLOTS_MILLIS_MAPS,SLOTS_MILLIS_REDUCES,\
MAP_INPUT_RECORDS,MAP_OUTPUT_RECORDS,MAP_OUTPUT_BYTES,MAP_OUTPUT_MATERIALIZED_BYTES,\
SPLIT_RAW_BYTES,COMBINE_INPUT_RECORDS,COMBINE_OUTPUT_RECORDS,REDUCE_INPUT_GROUPS,\
REDUCE_SHUFFLE_BYTES,REDUCE_INPUT_RECORDS,REDUCE_OUTPUT_RECORDS,SPILLED_RECORDS,\
SHUFFLED_MAPS,FAILED_SHUFFLE,MERGED_MAP_OUTPUTS,GC_TIME_MILLIS,CPU_MILLISECONDS,\
PHYSICAL_MEMORY_BYTES,VIRTUAL_MEMORY_BYTES,COMMITTED_HEAP_BYTES,BAD_ID,CONNECTION,\
IO_ERROR,WRONG_LENGTH,WRONG_MAP,WRONG_REDUCE,BYTES_READ,BYTES_WRITTEN\n'''

jobCounterInfoTitle='''FILE_BYTES_READ,FILE: Number of bytes read,\
FILE_BYTES_WRITTEN,FILE: Number of bytes written,FILE_READ_OPS,\
FILE: Number of read operations,FILE_LARGE_READ_OPS,\
FILE: Number of large read operations,FILE_WRITE_OPS,\
FILE: Number of write operations,HDFS_BYTES_READ,\
HDFS: Number of bytes read,HDFS_BYTES_WRITTEN,\
HDFS: Number of bytes written,HDFS_READ_OPS,\
HDFS: Number of read operations,HDFS_LARGE_READ_OPS,\
HDFS: Number of large read operations,HDFS_WRITE_OPS,\
HDFS: Number of write operations,TOTAL_LAUNCHED_MAPS,\
Launched map tasks,TOTAL_LAUNCHED_REDUCES,Launched reduce tasks,\
DATA_LOCAL_MAPS,Data-local map tasks,SLOTS_MILLIS_MAPS,\
Total time spent by all maps in occupied slots (ms),\
SLOTS_MILLIS_REDUCES,Total time spent by all reduces in occupied slots (ms),\
MAP_INPUT_RECORDS,Map input records,MAP_OUTPUT_RECORDS,\
Map output records,MAP_OUTPUT_BYTES,Map output bytes,\
MAP_OUTPUT_MATERIALIZED_BYTES,Map output materialized bytes,\
SPLIT_RAW_BYTES,Input split bytes,COMBINE_INPUT_RECORDS,\
Combine input records,COMBINE_OUTPUT_RECORDS,Combine output records,\
REDUCE_INPUT_GROUPS,Reduce input groups,REDUCE_SHUFFLE_BYTES,\
Reduce shuffle bytes,REDUCE_INPUT_RECORDS,Reduce input records,\
REDUCE_OUTPUT_RECORDS,Reduce output records,SPILLED_RECORDS,\
Spilled Records,SHUFFLED_MAPS,Shuffled Maps ,FAILED_SHUFFLE,\
Failed Shuffles,MERGED_MAP_OUTPUTS,Merged Map outputs,GC_TIME_MILLIS,\
GC time elapsed (ms),CPU_MILLISECONDS,CPU time spent (ms),\
PHYSICAL_MEMORY_BYTES,Physical memory (bytes) snapshot,\
VIRTUAL_MEMORY_BYTES,Virtual memory (bytes) snapshot,\
COMMITTED_HEAP_BYTES,Total committed heap usage (bytes),\
BAD_ID,BAD_ID,CONNECTION,CONNECTION,IO_ERROR,IO_ERROR,\
WRONG_LENGTH,WRONG_LENGTH,WRONG_MAP,WRONG_MAP,WRONG_REDUCE,\
WRONG_REDUCE,BYTES_READ,Bytes Read,BYTES_WRITTEN,Bytes Written\n'''

class Job:
    def __init__(self,path):
        self.maps=[]
        self.reduces=[]
        #AM_STARTED
        #self.job_startTime=""
        #JOB_SUBMITTED
        self.js_jobName=""
        self.js_submitTime=0
        self.js_jobid=""
        #JOB_INITED
        self.ji_totalMaps=0
        self.ji_totalReduces=0
        self.ji_launchTime=0
        #JOB_INFO_CHANGED
        #self.jic_submitTime=""
        #self.jic_launchTime=""
        #self.jic_jobid=""
        #JOB_FINISHED
        self.jf_finishTime=""
        self.jf_finishedMaps=0
        self.jf_finishedReduces=0
        self.jf_failedMaps=0
        self.jf_failedReduces=0
        
        self.jf_totalConuters=[]

        self.try_to_analyze(path)

    def getMapNum(self):
        return self.jf_finishedMaps

    def getReduceNum(self):
        return self.jf_finishedReduces

    def output_as_csv(self):
        '''out="JobName,JobId,JobLaunch,JobFinish,\
        MapTaskStart,MapAttStart,MapAttFinish,MapFinish,MapTaskFinish,\
        RedTaskStart,RedAttStart,RedAttFinish,RedShuffleFinish,\
        RedSortFinish,RedTaskFinish\n"'''
        out=""
        
        lineNum=max(self.jf_finishedMaps,self.jf_finishedReduces)
        for i in range(lineNum):
            if i==0:
                out+="%s,%s,%d,%d,"%(self.js_jobName,\
                                     self.js_jobid,\
                                     self.ji_launchTime,\
                                     self.jf_finishTime)
            else:
                out+=" , , , ,"
            if i<len(self.maps):
                mp=self.maps[i]
                out+="%d,%d,%d,%d,%d,"%(mp.map_startTime,\
                                          mp.mapatt_startTime,\
                                          mp.mapatt_finishTime,\
                                          mp.mapatt_mapFinishTime,\
                                          mp.map_finishTime)
            else:
                out+=" , , , , ,"
            if i<len(self.reduces):
                rd=self.reduces[i]
                out+="%d,%d,%d,%d,%d,%d,"%(rd.red_startTime,\
                                        rd.redatt_startTime,\
                                        rd.redatt_finishTime,\
                                        rd.redatt_shuffleFinishTime,\
                                        rd.redatt_sortFinishTime,\
                                        rd.red_finishTime)
            else:
                out+=" , , , , , ,"
            out+="\n" #new line
        return out
    
    def try_to_analyze(self,path):
        a=file(os.getcwd()+os.sep+"done"+os.sep+path)
        lines=a.readlines()[2:]
        for each in lines:
            eachone=json.loads(each)  #loads for string, and load for file!
            #here, eachone is {tpye:xxx,event:xxx}
            self.analyze_each_type(eachone)
        a.close()

    def analyze_each_type(self,one_type):
        a=one_type['type']
        b=one_type['event']
        if a=='JOB_SUBMITTED':
            self.js(b["org.apache.hadoop.mapreduce.jobhistory.JobSubmitted"])
        elif a=="JOB_INITED":
            self.ji(b["org.apache.hadoop.mapreduce.jobhistory.JobInited"])
        elif a=="TASK_STARTED":
            self.ts(b["org.apache.hadoop.mapreduce.jobhistory.TaskStarted"])
        elif a=="TASK_FINISHED":
            self.tf(b["org.apache.hadoop.mapreduce.jobhistory.TaskFinished"])
        elif a=="MAP_ATTEMPT_STARTED":
            self.mas(b["org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStarted"])
        elif a=="REDUCE_ATTEMPT_STARTED":
            self.ras(b["org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStarted"])
        elif a=="MAP_ATTEMPT_FINISHED":
            self.maf(b["org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinished"])
        elif a=="REDUCE_ATTEMPT_FINISHED":
            self.raf(b["org.apache.hadoop.mapreduce.jobhistory.ReduceAttemptFinished"])
        elif a=="JOB_FINISHED":
            self.jf(b["org.apache.hadoop.mapreduce.jobhistory.JobFinished"])
        else:
            print "omitted", a
        
    def js(self,data):
        self.js_jobName    = data['jobName']
        self.js_submitTime = data['submitTime']
        self.js_jobid      = data['jobid']

    def ji(self,data):
        self.ji_totalMaps   = data['totalMaps']
        self.ji_totalReduces= data['totalReduces']
        self.ji_launchTime  = data['launchTime']

    def ts(self,data):
        if data['taskType']=="MAP":
            mp=MapTask()
            self.maps.append(mp)
            self.ts_map(mp,data)
            
        elif data['taskType']=="REDUCE":
            rd=ReduceTask()
            self.reduces.append(rd)
            self.ts_red(rd,data)

    def ts_map(self,a_map,data):
        a_map.map_taskid=data['taskid']
        a_map.map_startTime=data['startTime']
       # a_map.map_splitLocaitions=data['splitLocaitions'] #

    def ts_red(self,a_red,data):
        a_red.red_taskid=data['taskid']
        a_red.red_startTime=data['startTime']
    
    def tf(self,data):
        if data['taskType']=="MAP":
            self.tf_map(data)
        elif data['taskType']=="REDUCE":
            self.tf_red(data)

    def tf_map(self,data):
        taskid=data['taskid']
        for each in self.maps:
            if each.map_taskid==taskid:
                each.map_finishTime = data['finishTime']
                each.map_counter    = data['counters']['groups']
                #each.map_counters
                break

    def tf_red(self,data):
        taskid=data['taskid']
        for each in self.reduces:
            if each.red_taskid==taskid:
                each.red_finishTime = data['finishTime']
                each.red_counter    = data['counters']['groups']
                break
        
    def mas(self,data):
        taskid=data['taskid']
        for each in self.maps:
            if each.map_taskid==taskid:
                each.mapatt_startTime   = data['startTime']
                each.mapatt_trackerName = data['trackerName']
                each.mapatt_attemptId   = data['attemptId']
                each.mapatt_trackerName = data['trackerName']#
                break
        '''for each in self.maps:
            print each.map_taskid'''
    def maf(self,data):
        taskid=data['taskid']
        for each in self.maps:
            if each.map_taskid==taskid:
                each.mapatt_mapFinishTime = data['mapFinishTime']
                each.mapatt_finishTime    = data['finishTime']
                each.mapatt_hostname      = data['hostname'] #
                break
        
        
    def ras(self,data):
        taskid=data['taskid']
        for each in self.reduces:
            if each.red_taskid==taskid:
                each.redatt_startTime   = data['startTime']
                each.redatt_trackerName = data['trackerName']
                each.redatt_attemptId   = data['attemptId']
                break

    def raf(self,data):
        taskid=data['taskid']
        for each in self.reduces:
            if each.red_taskid==taskid:
                each.redatt_shuffleFinishTime = data['shuffleFinishTime']
                each.redatt_sortFinishTime    = data['sortFinishTime']
                each.redatt_finishTime        = data['finishTime']
                break

    def jf(self,data):
        self.jf_finishTime       = data['finishTime']
        self.jf_finishedMaps     = data['finishedMaps']
        self.jf_finishedReduces  = data['finishedReduces']
        self.jf_failedMaps       = data['failedMaps']
        self.jf_failedReduces    = data['failedReduces']

        self.jf_totalCounters    = data['totalCounters']['groups']
        
    def print_jf_totalCountersInfo(self):
        out=""
        out+=self.js_jobName+","+self.js_jobid+","

        debugInfo=""
                
        for each in self.jf_totalCounters:

            for infos in each['counts']:
                out+=infos['name']+","+str(infos['value'])+","
                debugInfo+=infos['name']+","+infos['displayName']+","

        return out+"\n"

    def print_tf_mf_counters(self):
        out=""
        out+=self.js_jobName+","+self.js_jobid+"\n"
        
        for eachmap in self.maps:#each map
            out+=eachmap.map_taskid+", ,"
            debugInfo=""
            for eachGroup in eachmap.map_counter:
                for infos in eachGroup['counts']:
                    out+=infos['name']+","+str(infos['value'])+","
                    debugInfo+=infos['name']+","+str(infos['displayName'])+","
            out+="\n"
        #print debugInfo
        #return out+"\n"
        for eachmap in self.reduces:#each map
            out+=eachmap.red_taskid+", ,"
            debugInfo=""
            for eachGroup in eachmap.red_counter:
                for infos in eachGroup['counts']:
                    out+=infos['name']+","+str(infos['value'])+","
                    debugInfo+=infos['name']+","+str(infos['displayName'])+","
            out+="\n"
        #print debugInfo

        return out+"\n"
        
class MapTask:  #task : succ-att== 1:1
    def __init__(self):
        #TASK_STARTED
        self.map_taskid=""
        self.map_startTime=0
        self.map_splitLocaitions=""  #
        
        #self.map_splitLocations=""
        #MAP_ATTEMPT_STARTED
        self.mapatt_startTime=0
        self.mapatt_trackerName="" #
        self.mapatt_attemptId=""

        #MAP_ATTEMPT_FINISHED
        self.mapatt_mapFinishTime=0
        self.mapatt_finishTime=0
        self.mapatt_hostname=""  #

        #TASK_FINISHED
        self.map_finishTime=0
        self.map_counter=[]
        
class ReduceTask:
    def __init__(self):
        #TASK_STARTED
        self.red_taskid=""
        self.red_startTime=0
        
        #RED_ATTEMPT_STARTED
        self.redatt_startTime=0
        self.redatt_trackerName=""
        self.redatt_attemptId=""

        #RED_ATTEMPT_FINISHED
        self.redatt_shuffleFinishTime=0
        self.redatt_sortFinishTime=0
        self.redatt_finishTime=0

        #TASK_FINISHED
        self.red_finishTime=0
        self.red_counter=[]

prefix="org.apache.hadoop.mapreduce.jobhistory."

def getJobCounters(jobs):
    tableTitle=Titles.jobCounterTitle
    tableTitle1='''JobName,JobId,FILE_BYTES_READ,FILE_BYTES_WRITTEN,\
HDFS_BYTES_READ,HDFS_BYTES_WRITTEN\n'''
    additionInfo=Titles.jobCounterInfoTitle
    mapCT=Titles.mapCounterTitle
    redCT=Titles.redCounterTitle
    
    out=file("23jobCounters.csv","w")

    #out.write(tableTitle)
    out.write("some,info,"+additionInfo)
    for each in jobs:  
        out.write(each.print_jf_totalCountersInfo())
    
    #out.close()
    out.write("\n\n\n")
    out.write("mapinfo, ,"+mapCT+"\n redinfo, ,"+redCT+"\n")

    for each in jobs:
        #print each.print_tf_mf_counters()
        out.write(each.print_tf_mf_counters())

    out.close()

def ana():
    jobs=[]
    out=file("23out.csv","w")
    for each in os.listdir(os.getcwd()+os.sep+"done"):
        if each.split(".")[1]=="jhist":
            newjob=Job(each)
            jobs.append(newjob)
            #break  #debug
    tableTitle="JobName,JobId,JobLaunch,JobFinish,\
        MapTaskStart,MapAttStart,MapAttFinish,MapFinish,MapTaskFinish,\
        RedTaskStart,RedAttStart,RedAttFinish,RedShuffleFinish,\
        RedSortFinish,RedTaskFinish\n"
    out.write(tableTitle)
    for each in jobs:  
        out.write(each.output_as_csv())
        
    out.close()
    getJobCounters(jobs)

ana()

