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