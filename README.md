# GE_multiple_Input_POC


snowflake_DS.py : 
  The snowflake data connection file with connection details and creation of datasource using the credentials.


DataConnection.py : 
  Abstract class for multiple input data connection functions.


snowflakeConnection.py : 
  Creating batch request for snowflake data with the created datasource.


csvConnection.py : 
  Creating batch request for csv data with the created datasource.


profiling_userbased_test.py : 
  Based on the input arguments(type of input data),creating expectation suite , validator and required expectations. 
  Currently we are using 2 input arguments. 
  1. "snowflake" -> for snowflake datasource 
  2. "csv" -> for csv datasource

  Command to run the script -> python3 profiling_userbased_test.py snowflake


testCases.py : Sample test cases to test the connection.



Future developments which can be performed :

1.Create separate yaml file
2.Can store all the credentials in a conf file and access these through .sh file and can run the script using the same .sh file
3.Create profiler,checkpoints etc
4.Create datadocs and attach for better presentable way.

