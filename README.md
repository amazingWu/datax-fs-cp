# DataX-fs-cp

Datax-fs-cp基于Datax改造，目的是为了解决不同文件系统的文件同步，尤其是文件系统之间存在网络隔离的情况下通过中转机实现不同文件系统的文件同步。目前已经实现了本地文件系统、HDFS等之间的文件同步。  


# Features

Datax-fs-cp本身作为文件同步框架，将不同的文件系统之间的同步抽象为源头文件流读取Reader插件，以及向目标端文件系统写入数据的Writer插件，
理论上DataX-fs-cp框架可以支持任意文件系统之间的文件同步。
同时DataX插件体系作为一套生态系统, 每接入一套新文件系统新加入的文件系统即可实现和现有的文件系统互通。  
在面对不同的文件系统之间存在网络隔离时是更好的选择


# DataX详细介绍

##### 请参考：[DataX-Introduction](./introduction.md)



# Quick Start

Datax-fs-cp作为Datax的改造版，基础的配置（除了Job中的Writer和Reader插件内容不同）和Datax保持一致，可参阅：

##### 请点击：[Quick Start](./userGuid.md)


# Support Data Channels 

DataX-fs-cp目前已经有了比较全面的插件体系，目前支持数据如下图:

 数据源        | Reader(读) | Writer(写) |文档|
---------- | :-------: | :-------: |:-------: |
本地文件系统 |     √     |     √     |[读](./local-fs-reader/doc/local-fs-reader.md) 、[写](./local-fs-writer/doc/local-fs-writer.md)|
HDFS |     √     |     √     |[读](./hdfs-fs-reader/doc/hdfs-fs-reader.md) 、[写](./hdfs-fs-writer/doc/hdfs-fs-writer.md)|

# 我要开发新的插件
请点击：[DataX插件开发宝典](./dataxPluginDev.md)