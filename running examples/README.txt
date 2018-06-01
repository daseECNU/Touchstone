Touchstone运行示例：

将该文件夹"running examples"拷贝到你的机器（任一目录），可通过执行如下命令直接运行Touchstone。
运行命令：java -jar Touchstone.jar XXX.conf

在配置文件（上面命令中的"XXX.conf"）中，用户需要对部署的集群信息、Touchstone的controller、Touchstone的data generators、待生成数据库schema和目标测试Query的基数约束、以及运行时的一些算法参数等信息进行配置。下面通过一个示例配置文件进行说明。

示例配置文件：
---------------------------------------------------------------
## configurations of servers
IPs of servers: 10.11.1.190; 10.11.1.191; 10.11.1.192
password of root user: w@ngl5i
user names of servers: touchstone; touchstone; touchstone
passwords of servers: 123456; 123456; 123456

## 上面是对Touchstone部署的集群信息进行了配置（目前仅支持Linux系统 & Java 8+）。这里配置了三个节点，IP分别是10.11.1.190，10.11.1.191，10.11.1.192。所有节点的root密码是w@ngl5i（为了清空操作系统缓存），目前程序在运行前一般默认会清空所有节点的操作系统缓存，确保程序在运行过程中不会因为内存不足而出现JVM GC。Touchstone对内存要求并不高，实际占用内存一般不超过1GB。注意：运行命令"java -jar Touchstone.jar XXX.conf"可在Windows或Linux系统中运行，但所在节点必须与上面配置的节点网络连通。

## configurations of controller
IP of controller: 10.11.1.190
port of controller: 32100
running directory of controller: ~//icde_test

## 上面配置了Touchstone controller的部署节点（自动部署），controller端口号为32100，controller与data generators利用netty网络框架进行通信（发送数据生成任务和Join Information Table）。

## input files
database schema: .//input//tpch_schema_sf_1.txt
cardinality constraints: .//input//tpch_cardinality_constraints_sf_1.txt

## 上面配置了待生成数据库的schema和目标测试Query的基数约束。

## configuration of log4j
path of log4j.properties: .//lib//log4j.properties

## configuration of Mathematica
path of JLink: C://Program Files//Wolfram Research//Mathematica//10.0//SystemFiles//Links//JLink

## 针对复杂的过滤谓词以及非等值连接谓词，Touchstone需要利用Mathematica提供的数值积分运算功能（对应论文中的random sampling算法），但是如果输入负载中仅包含简单的过滤谓词（如col > para）和等值连接谓词，这里无需配置。

## configurations of data generators
IPs of data generators: 10.11.1.191; 10.11.1.191; 10.11.1.191; 10.11.1.192; 10.11.1.192; 10.11.1.192
ports of data generators: 32101; 32102; 32103; 32101; 32102; 32103
thread numbers of data generators: 2; 2; 2; 2; 2; 2
running directories of data generators: ~//icde_test//dg1; ~//icde_test//dg2; ~//icde_test//dg3; ~//icde_test//dg1; ~//icde_test//dg2; ~//icde_test//dg3
data output path: .//data

## 上面配置了Touchstone data generators的部署节点（自动部署）。由于一个JVM中启动多个数据生成线程的性能往往没有多个JVM中启动相同数量数据生成线程的性能好，所以建议在一个节点上根据CPU物理核数启动多个JVM。上面的示例配置在每个物理节点上启动了3个JVM，每个JVM中启动了2个数据生成线程。所有运行目录会自动创建，无需人工手动创建。

## running parameters
thread numbers of query instantiation: 2
maximum iterations of query instantiation: 20
global relative error of query instantiation: 0.0001
maximum iterations of parameter instantiation: 20
relative error of parameter instantiation: 0.0001
maximum number of shuffle: 1000
maximum size of PKVs: 10000

## 上面配置了Touchstone运行过程中所需的一些参数，详细含义可查看论文。
---------------------------------------------------------------

其他注意事项：
1. 不要对running examples文件夹下的input & lib文件夹进行重命名！当前打包jar中这两个文件夹名称是写死的，你要是真想改，请稍微改动下我们的源码并重新打包。
2. 如果你拿不到root密码，那么请在我们的程序中注释掉清操作系统缓存的那行代码。
3. 放心，我们的root密码已经不是w@ngl5i了，我不会那么没有保密意识的。
4. ...... 还没想到，等想到了再加。

待生成数据库的schema和目标测试Query的基数约束：
针对TPC-H和Star Schema Benchmark，我们给出了论文实验中的部分输入（当前目录下的input文件夹中），具体请参照示例文件。

约束链中三类约束的基本数据结构如下：
Filter node: [0, exp1@op1#exp2@op2 ... #and|or, probability]
FKJoin node: [2, fk1#fk2 ..., probability, pk1#pk2 ..., num1, num2]
PKJoin node: [1, pk1#pk2 ..., num1, num2, ...]
// num1 is the identifier that can join, num2 is the identifier that can not join