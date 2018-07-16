
<font size=5><div align="right"><a href="https://github.com/daseECNU/Touchstone/blob/master/README.md">中文 版本</a></div>
</font>

### Introduction to Touchstone

**Touchstone is a query-aware (or an application-oriented) data generator developed by the School of Data Science and Engineering at East China Normal University (DaSE@ECNU). This work has been accepted by USENIX Annual Technical Conference 2018 (USENIX ATC'2018).**

**Paper info: Yuming Li, Rong Zhang, Xiaoyan Yang, Zhenjie Zhang, Aoying Zhou, "Touchstone: Generating Enormous query-aware Test Databases", USENIX ATC 2018.**

Please refer to "research paper - touchstone.pdf" for the conference paper, and "technical report - touchstone.pdf" for the technical report. We give a more detailed description and analysis of the involved algorithms and add more experiments in the technical report.

In order to make it easier for everyone to use Touchstone and repeat our work, we open the source code of Touchstone here, and give the executable files as well as a lot of example inputs (see "running examples" folder).


### Application scenarios and related works

The application scenarios of Touchstone include: database management system (DBMS) testing, database application stress testing, and application-driven database performance evaluation (or application-driven benchmarking).

(1)	DBMS testing: Testing over the synthetic database is one of the essential steps in DBMS development, aiming to verify the correctness and efficiency of the system implementation. However, it’s difficult to simulate the expected workload characteristics on target test queries (such as the size of the intermediate query result) if we generate the test database randomly or based on the specified data characteristics.

(2)	Database application stress testing: The test database may not be available to current application stress testing, when 1) a new application is under testing before it goes online; 2) the users want to test the extreme performance of the system, even before the real world application reaches the target scale; 3) the users are unwilling to share the internal database in production with the tester due to the privacy considerations. So we need to generate the test database with required data scale and workload characteristics on target application's queries.

(3)	Application-driven benchmarking: When evaluating the performance among different DBMSs, it is preferred to use the standard benchmarks, such as TPC series. However, existing benchmarks are not completely suitable for all applications because each application have its own data and workloads with specific characteristics. Generating tailored test database of an application for benchmarking multiple solutions is a good complement to standard benchmarks.

Currently, a bulk of existing data generators generate test databases independent of the test queries, which only consider the data distribution of inter- and intra-attribute. They fail to guarantee the similar workload characteristics of the test queries, therefore it’s difficult to match the overheads of the query execution engine for real world workloads.

A number of other studies, attempt to build query-aware data generators. But the performance of the state-of-the-art solutions still remains far from satisfactory, due to the lack of parallelization, scalability and memory usage control, as well as the narrow support of non-equi-join workload. In order to generate the enormous (TB, PB, ...) query-aware test databases, we design and implement Touchstone, our new query-aware data generator.


### Input and output

The input of Touchstone includes: database schema, data characteristics of each column and workload characteristics of each test query (output cardinality constraints on the intermediate query results).

The output of Touchstone includes: a test database instance and instantiated test queries (instantiating the variable parameters in test queries with concrete values).

Note that 1) the data in the tables strictly follows the specified data characteristics; 2) the executions of the instantiated queries on the generated database instance produce the expected output cardinality specified in workload characteristics on each operator.


### The main features of Touchstone

(1) Support the most common workloads in real world applications. Touchstone currently supports the first 16 queries of TPC-H and all queries of Star Schema Benchmark (SSB). To the best of our knowledge, Touchstone provides the widest support to TPC-H and SSB workloads, among all the existing studies.

(2) Highly Efficient data generation. Touchstone supports fully parallel data generation on multiple nodes. The data generation throughput of Touchstone is at least 3 orders of magnitude higher than that of state-of-the-art work MyBenchmark. Intuitive data: It takes about 25 minutes to generate a 1TB (SF=1000) TPC-H database with the input of the first TPC-H 16 queries on a cluster with 8 nodes (CPU: 2 * E5-2620, MEM consumption: less than 5GB).

(3) Linear scalability. Touchstone is linearly scalable to generation database size and multiple nodes.

(4) High data fidelity. The test database generated by Touchstone is highly similar with the original (real) database both in relative error on cardinality constraints and performance deviation on query latencies.


### Contact

R & D team: CEDAR project team, School of Data Science and Engineering, East China Normal University.

Key members: Professor Rong Zhang, Yuming Li, Ph.D candidate.

Cooperators: Xiaoyan Yang, Zhenjie Zhang (Singapore R&D, Yitu Technology Ltd.).

Address: No. 3663, North Zhongshan Road, Putuo District, Shanghai.

Zip code: 200062.

Email: liyuming@stu.ecnu.edu.cn.

For any question about Touchstone, you can contact us by email, and we will give you feedback.

