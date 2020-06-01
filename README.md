
<font size=5><div align="right"><a href="https://github.com/daseECNU/Touchstone/blob/master/README-en.md">English Version</a></div>
</font>

### Touchstone简介

**Touchstone是华东师范大学数据科学与工程学院（DaSE@ECNU）研发的面向应用负载的数据生成器。该工作已被USENIX Annual Technical Conference 2018 (USENIX ATC'2018) 录取。**

**论文信息：Yuming Li, Rong Zhang, Xiaoyan Yang, Zhenjie Zhang, Aoying Zhou, "Touchstone: Generating Enormous Query-Aware Test Databases", USENIX ATC 2018.**

会议论文请参照"research paper - touchstone.pdf"，技术报告请参照"technical report - touchstone.pdf"。我们在技术报告中给出了论文算法的详细描述和分析，并补充了更多的实验内容。

为了使大家能够更方便地使用Touchstone和重复我们的工作，我们在此开源了实现源码，并给出了打包好的可执行文件以及大量的运行示例（见"running examples"文件夹）。


### 应用背景与相关工作

Touchstone的应用场景主要有：数据库管理系统测试，数据库应用压力测试和应用驱动的数据库性能评测。

（1）数据库管理系统测试：在数据库开发的过程中，为了验证系统实现的正确性和高效性，基于模拟数据库进行测试是保证系统质量的必要途径。而简单地随机生成测试数据库，或者基于数据特征来生成测试数据库，往往难以模拟我们针对测试Query期望的负载特征（如中间结果集的大小）。

（2）数据库应用压力测试：对于一个还未上线或者新上线的数据库应用来说，当前已有的数据规模（甚至没有数据）还无法支撑有效的数据库应用压力测试，所以需要根据当前应用负载构建所需数据规模以及满足特定负载特征的测试数据库。

（3）应用驱动的数据库性能评测：标准Benchmark一般比较通用，而每一个应用的数据和负载往往都有自己的特征，因此应用开发者在数据库系统选择，以及数据库开发者在面向客户应用的性能调优的过程中，生成面向应用负载的测试数据库是至关重要的（生产上的真实数据由于隐私安全问题往往难以共享）。

当前有很多数据生成工作仅根据数据特征来生成模拟数据，而没有将目标测试负载（一组Query）考虑进来，所以生成的数据难以模拟目标测试Query上的负载特征。而面向负载的数据生成器目前还难以满足实际应用需求，主要的缺陷有：数据生成难以并行化，内存消耗过大，可扩展性不足，不支持非等值连接负载等。Touchstone正是在这种背景下，为生成满足复杂实际应用负载特征和工业级规模（TB、PB级）的测试数据库而诞生。


### 输入和输出

Touchstone的输入包含：待生成数据库的Schema，所有属性的数据特征和测试负载（一组Query）的负载特征（指定所有Query Tree中Operator的输入和输出数据集大小，即记录数）；Touchstone的输出包含：实例化的测试负载（将输入Query中的符号参数实例化成具体数值）和一个测试数据库实例。生成的测试数据库满足所有属性上指定的数据特征，实例化的查询在生成的测试数据库上执行时的中间结果集大小与指定的负载特征一致。


### Touchstone主要特征

（1）支持常见的复杂查询负载。Touchstone目前可支持TPC-H的前16个Query和Star Schema Benchmark（SSB）的所有Query，是目前（2018-06-01）针对TPC-H和SSB负载支持最广泛的查询敏感的数据生成器。

（2）高效的数据生成。Touchstone可以实现完全并行化的数据生成，数据生成速度是目前state-of-the-art工作MyBenchmark的1000倍以上。直观上的数据：8个节点（CPU：2 * E5-2620，MEM：消耗量小于5GB），针对TPC-H的前16个Query，生成1TB（SF=1000）的测试数据库需要25min左右。

（3）线性的可扩展性。Touchstone针对物理节点数和生成数据集大小具有线性的可扩展性。

（4）高保真的生成数据。Touchstone生成的测试数据库无论是在Query中间结果的基数约束上，还是在查询性能上都与原始数据库具有高度的相似性。


### 联系我们

研发团队：华东师范大学数据科学与工程学院 **DBHammer**项目组。

该工作的主要成员：张蓉`教授`，王清帅`博士生`，游舒泓`硕士生`，李宇明`已毕业博士`。

合作者：Xiaoyan Yang, Zhenjie Zhang (Singapore R&D, Yitu Technology Ltd.).

地址：上海市普陀区中山北路3663号。

邮政编码：200062。

联系邮箱：qswang@stu.ecnu.edu.cn

对于文档内容和实现源码有任何疑问，可通过邮件与我们联系，我们收到后将尽快给您反馈。
