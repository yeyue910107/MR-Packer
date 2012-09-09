MR-Packer
=========

A Cost-based SQL-to-MapReduce Translator
MR-Packer is an SQL-to-MapReduce translator that translates an SQL command to Java codes for Hadoop. MR-Pakcer consists of two parts. The first part (SQL-to-XML), which is implemented in C, is to convert the SQL statement in a file to an XML file that represents the abstract syntax tree of the SQL command. The second part (XML-to-MapReduce), which is implemented in Python, is to translate the XML file to Java codes. The two parts can be used individually.

 As its name shows, MR-Packer is only a translator from SQL to MapReduce. So, its inputs are an SQL file and a data schema file, and its outputs are only generated Java codes that are used to execute the SQL command on Hadoop. But, unlike Hive or Pig, it is not MR-Packer's responsibility to compile and execute those codes, although MR-Pakcer can also be configured to do that. Therefore, MR-Pakcer can be used in a machine where even no Hadoop is installed. 

 The main advantage of MR-Packer over Hive or Pig is that it can efficiently translate complex queries that have intra-query correlations.
