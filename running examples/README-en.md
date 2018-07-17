<font size=5><div align="right"><a href="https://github.com/daseECNU/Touchstone/blob/master/running%20examples/README.md">中文 版本</a></div>
</font>

### Running example

**Copy the folder "running examples" to your machine (any directory) and run Touchstone directly by executing the following command.**

**Command: java -jar Touchstone.jar XXX.conf**

**Very simple!**


### The description of the configuration file

In the configuration file ("XXX.conf" in the execution command), the user needs to configure the cluster information for deploying Touchstone, the controller and data generators of Touchstone, the target database schema, the data characteristics of columns, the cardinality constraints on test queries (workload characteristics), as well as some algorithm parameters and so on. The below is an example configuration file.


#### Sample configuration file:

\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-

\#\# configurations of servers

IPs of servers: 10.11.1.190; 10.11.1.191; 10.11.1.192  
password of root user: w@ngl5i  
user names of servers: touchstone; touchstone; touchstone  
passwords of servers: 123456; 123456; 123456  

\#\# The above is the configuration of the cluster for deploying Touchstone (currently only supporting Linux system & Java 8+). There are three nodes configured here, the IPs are 10.11.1.190, 10.11.1.191 and 10.11.1.192. The root password of all nodes is 'w@ngl5i' (using it to clear the cache of operating system). Currently, Touchstone clears the operating system cache of all nodes by default before running, ensuring that the JVM GC does not be triggered due to insufficient memory. If you can't get the root password, you can ignore it. And Touchstone has a austere memory consumption which usually less than 5GB. Note that the command "java -jar Touchstone.jar XXX.conf" can be run on a Windows or Linux system, but the system (node) must be network connected to the cluster configured above.


\#\# configurations of controller

IP of controller: 10.11.1.190  
port of controller: 32100  
running directory of controller: ~//icde_test  

\#\# The above is the configuration of Touchstone controller. And the controller is automatically deployed by the program. The network port of controller is 32100. Controller and data generators use Netty network framework for communication (sending data generation task and join information table).


\#\# input files

database schema: .//input//tpch_schema_sf_1.txt  
cardinality constraints: .//input//tpch_cardinality_constraints_sf_1.txt  

\#\# The above is the configuration of input files of database schema (including data characteristics) and cardinality constraints. For the specific format of the input data, please refer to the sample inputs in the "input" folder. And we also give a detailed description of the input data format in README of "input" folder.


\#\# configuration of log4j

path of log4j.properties: .//lib//log4j.properties  


\#\# configuration of Mathematica

path of JLink: C://Program Files//Wolfram Research//Mathematica//10.0//SystemFiles//Links//JLink  

\#\# For complicated predicates in non-equi-filters and non-equi-joins, Touchstone needs to take advantage of the numerical integration provided by Mathematica (corresponding to the random sampling algorithm in the paper) for probability evaluation during query instantiation. If the test queries contain only simple filtering predicates (such as "col > para") and equivalent join predicates (such as "T1.pk = T2.fk"), there is no need to configure Mathematica here.


\#\# configurations of data generators

IPs of data generators: 10.11.1.191; 10.11.1.191; 10.11.1.191; 10.11.1.192; 10.11.1.192; 10.11.1.192  
ports of data generators: 32101; 32102; 32103; 32101; 32102; 32103  
thread numbers of data generators: 2; 2; 2; 2; 2; 2  
running directories of data generators: ~//icde_test//dg1; ~//icde_test//dg2; ~//icde_test//dg3; ~//icde_test//dg1; ~//icde_test//dg2; ~//icde_test//dg3  
data output path: .//data  


\#\# The above is the configuration of Touchstone data generators. And all data generators are automatically deployed by the program. Since the performance of starting multiple data generation threads in one JVM is often lower than starting the same number of data generation threads in multiple JVMs, it is recommended to start multiple JVMs on a node based on the CPU core number. The above example configuration launches 3 JVMs on each node and 2 data generation threads in each JVM. All run directories are created automatically by the program and do not need to be created manually.


\#\# running parameters

thread numbers of query instantiation: 2  
maximum iterations of query instantiation: 20  
global relative error of query instantiation: 0.0001  
maximum iterations of parameter instantiation: 20  
relative error of parameter instantiation: 0.0001  
maximum number of shuffle: 1000  
maximum size of PKVs: 10000  

\#\# The above is the configuration of parameters used in algorithms of Touchstone, and the details (related meanings) can be viewed in the paper.

\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-


### Additional notes

(1) Do not rename the folder "input" & "lib", their names are hard-coded in the currently packaged jar. If you really want to rename the file names, please update the corresponding source code and repackage it.

(2) The instantiated query parameters are written in the log of the Touchstone controller. You can search "Final instantiated parameters" in the log file for locating instantiated parameters. The order of the instantiated parameters here is the same as the order of the variable parameters in the input cardinality constraints.
