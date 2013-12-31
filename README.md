MRPacker
=========

A Rule&Cost Based SQL-to-MapReduce Translator ([https://github.com/yeyue910107/MR-Packer](https://github.com/yeyue910107/MR-Packer))

## Overview ##

MRPacker is an SQL-to-MapReduce translator that translates an SQL command to Java codes for Hadoop. MR-Pakcer consists of three parts.

- AstGenerator convert the SQL statement in a file to an XML file that represents the abstract syntax tree of the SQL command. 
- Packer generate the execution plan and optimize it based on rules and cost.
- CodeGenerator translate the execution plan to Java codes.

MRPacker significantly improves the efficiency of MapReduce tasks compared with Hive, by using a set of transformation rules to reduce the number of MapReduce jobs and merging MapReduce jobs in a more reasonable way.

MRPacker is not a full functional database system. It only supports a subset features of SQL SELECT queries like "SELECT, POJECT, JOIN, GROUP BY, ORDER BY". We use TPC-H query ([http://www.tpc.org/tpch/](http://www.tpc.org/tpch/)) as our test cases.
 
## System Requirements ##
 
- Linux 32-bit or 64-bit 
- Python2.6+ (not Python3)
- Java and GCC (for ANTLR)
- If you want to let MRPacker compile and execute translated Java codes, Hadoop 0.2x or 1.x must be installed and HADOOP_HOME must be set.

## Setup and Usage ##

First install AstGenerator in astgen by the following steps:

1.Decompress ANTLR: 

    tar xzvf antlr-3.3.tar.gz

2.Generate Lexer and Parser: 

    java -jar antlr-3.3/lib/antlr-3.3-complete.jar MRPacker.g

3.Install C runtime:

    cd antlr-3.3/runtime/C/dist
    tar xzvf libantlr3c-3.1.4-SNAPSHOT.tar.gz
    cd libantlr3c-3.1.4-SNAPSHOT
    ./configure --prefix=/tmp/antrl_runtime_install_dir
    make install
    cd ../../../../../

4.Compile and Link:

    gcc -m32 -o MRPackerFront.exe MRPackerMain.c MRPackerLexer.c MRPackerParser.c /tmp/antrl_runtime_install_dir/lib/libantlr3c.a -I . -I /tmp/antrl_runtime_install_dir/include/

5.Eventually, we can get MRPacker.exe, which converts an SQL file to an XML file. 

- The command is "./MRPackerFront.exe inputsqlfile > outputxmlfile". 
- The input SQL file must contain an SQL SELECT command ended by ";". 
- The output XML file represents the abstract grammar tree of the input SQL SELECT command. 
- The XML file will be used as an input for Optimizer and CodeGenerator.

Then translate the SQL file by running command: 

    python translate.py $1 $2 $3 $4 $5

- $1: a file that contains the input SQL command.
- $2: a schema file that describes the structures of the tables in the input SQL command. 
- $3: an optional query name (default is "testquery").
- $4: an optional HDFS path that contains table data (default is config.py/data_dir).
- $5: an optional HDFS path that contains query output data (default is config.py/data_dir).

After translation, results will be created which contains:

- testscript: a shell script which specifies how to compile the generated code and how to execute the code on Hadoop.
- code: all the source code files. The source code file is named with the pattern "queryname + number". Each source code file corresponds to one Hadoop job.
- jar: jar file to run on the hadoop clusters.

Some config parameters could be configured in config.py for different usages. See detailed information in config.py.
