
### Touchstone简介

Touchstone是华东师范大学数据科学与工程学院（DaSE@ECNU）研发的面向应用负载的数据生成器。该工作已被USENIX Annual Technical Conference 2018 (ATC'2018) 录取。

论文题目：Touchstone: Generating Enormous Query-Aware Test Databases。
会议论文请参照"research paper - touchstone.pdf"，技术报告请参照"technical report - touchstone.pdf"。

为了使大家能够更方便地使用Touchstone和重复我们的工作，我们给出了大量的运行示例，见"running examples"文件夹。

### Touchstone主要特征

1. 支持常见的复杂查询负载。Touchstone目前可支持TPC-H的前16个Query和Star Schema Benchmark（SSB）的所有Query，是目前（2018-06-01）针对TPC-H和SSB负载支持最广泛的面向查询的数据生成器。

2. 高效的数据生成。Touchstone的数据生成速度是目前state-of-the-art工作MyBenchmark的1000倍左右。直观上的数据：8个节点（CPU：2 * E5-2620，MEM：消耗量小于5GB），针对TPC-H的前16个Query，生成1TB（SF=1000）的测试数据库需要25min左右。

3. 线性的可扩展性。Touchstone针对物理节点数和生成数据集大小具有线性的可扩展性。

4. 高保真的生成数据。Touchstone生成的测试数据库无论是在Query中间结果的基数约束上，还是在查询性能上都与原始数据库具有高度的相似性。


### 联系我们
 
研发团队：华东师范大学数据科学与工程学院 CEDAR项目组。

该工作的主要成员：张蓉 教授，李宇明 在读博士。

地址：上海市普陀区中山北路3663号。

邮政编码：200062。

联系邮箱：liyuming@stu.ecnu.edu.cn。

对于文档内容和实现源码有任何疑问，可通过邮件与我们联系，我们收到后将尽快给您反馈。