# 作业5  多个文档的有序合并及去重

211275007  康钊源

### 一、设计思路

该任务要求将A、B输入文件合并为C文件，观察样例可知

日期-股票键值对优先以日期排序，相同日期内以值的字母序排序

同时要去掉相同日期的相同股票

因为mapreduce不方便操作txt，故先将输入文件都转为了txt格式

### 二、程序实现

用maven组织项目

主要程序代码为main,map,reduce三个分开的java文件

main(MergeDriver.java)如下

主要进行了任务分配，设置输出键值对格式，设置输入输出路径

![image-20231026225325455](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20231026225325455.png)

map(MergeMapper.java)如下：

主要是对输入文件每行分成了一对键值对，并写入context

![image-20231026230634999](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20231026230634999.png)

reduce(MergeReducer.java)如下：

主要通过set这个数据结构对每个日期下的value进行排序与去重

![image-20231026231843169](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20231026231843169.png)

### 三、其它讨论

![image-20231026235001740](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20231026235001740.png)

输出结果如上图，符合预期

接下来查看WEB端yarn调度各nodes的情况

![image-20231027100653400](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20231027100653400.png)

![image-20231027100935083](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20231027100935083.png)

可见yarn也工作正常

### 四、心得体会

本次作业虽然体量不大，但却还是耗费了不少心力

首先是对java以及maven管理的学习

接着又去理解了M/R工作中Map和Reduce类各自的工作模式，以及如何继承这些类进行重载来编程

期间还有很长时间耗时在用poi对excel文件进行读写上，但乱码的问题始终没有解决，故选择采用先将xlsx变成txt再mapreduce的做法

接着还处理了一些历史遗留问题：、

![image-20231027101308856](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20231027101308856.png)

之前磁盘空间分配不够，但在实验1这个问题还没有反应出来，在本次作业却体现得淋漓尽致

因为空间不够，节点一直处于unhealthy状态，导致一直无法正常提交任务，在对磁盘扩容、重新分区后这个问题才得到解决，可见以前欠下的债还是要还的啊TAT