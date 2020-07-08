# 阿里云开源离线同步工具DataX-fs-cp介绍

## 一. DataX-fs-cp概览

​	DataX-fs-cp 基于Datax3.0改造，是一个异构数据源离线同步工具，致力于实现包括本地文件系统、HDFS等各种异构文件系统数据源之间稳定高效的数据同步功能。

- #### 设计理念

  为了解决异构数据源同步问题，DataX将复杂的网状的同步链路变成了星型数据链路，DataX作为中间传输载体负责连接各种数据源。当需要接入一个新的数据源的时候，只需要将此数据源对接到DataX，便能跟已有的数据源做到无缝数据同步。

## 二、DataX-fs-cp框架设计	

DataX本身作为离线数据同步框架，采用Framework + plugin架构构建。将数据源读取和写入抽象成为Reader/Writer插件，纳入到整个同步框架中。

- Reader：Reader为数据采集模块，负责采集数据源的数据，将数据发送给Framework。
- Writer： Writer为数据写入模块，负责不断向Framework取数据，并将数据写入到目的端。
- Framework：Framework用于连接reader和writer，作为两者的数据传输通道，并处理缓冲，流控，并发，数据转换等核心技术问题。

## 三. DataX-fs-cp插件体系

DataX-fs-cp目前已经有了比较全面的插件体系，目前支持数据如下图:

 数据源        | Reader(读) | Writer(写) |文档|
---------- | :-------: | :-------: |:-------: |
本地文件系统 |     √     |     √     |[读](./local-fs-reader/doc/local-fs-reader.md) 、[写](./local-fs-writer/doc/local-fs-writer.md)|
HDFS |     √     |     √     |[读](./hdfs-fs-reader/doc/hdfs-fs-reader.md) 、[写](./hdfs-fs-writer/doc/hdfs-fs-writer.md)|

DataX-fs-cp Framework提供了简单的接口与插件交互，提供简单的插件接入机制，只需要任意加上一种插件，就能无缝对接其他数据源。

## 四、DataX-fs-cp核心架构

DataX-fs-cp 开源版本支持单机多线程模式完成同步作业运行，本小节按一个DataX-fs-cp作业生命周期的时序图，从整体架构设计非常简要说明DataX-fs-cp各个模块相互关系。

![datax_arch](https://cloud.githubusercontent.com/assets/1067175/17850849/aa6c95a8-6891-11e6-94b7-39f0ab5af3b4.png)

#### 核心模块介绍：

1. DataX完成单个数据同步的作业，我们称之为Job，DataX接受到一个Job之后，将启动一个进程来完成整个作业同步过程。DataX Job模块是单个作业的中枢管理节点，承担了数据清理、子任务切分(将单一作业计算转化为多个子Task)、TaskGroup管理等功能。
2. DataXJob启动后，会根据不同的源端切分策略，将Job切分成多个小的Task(子任务)，以便于并发执行。Task便是DataX作业的最小单元，每一个Task都会负责一部分数据的同步工作。
3. 切分多个Task之后，DataX Job会调用Scheduler模块，根据配置的并发数据量，将拆分成的Task重新组合，组装成TaskGroup(任务组)。每一个TaskGroup负责以一定的并发运行完毕分配好的所有Task，默认单个任务组的并发数量为5。
4. 每一个Task都由TaskGroup负责启动，Task启动后，会固定启动Reader—>Channel—>Writer的线程来完成任务同步工作。
5. DataX作业运行起来之后， Job监控并等待多个TaskGroup模块任务完成，等待所有TaskGroup任务完成后Job成功退出。否则，异常退出，进程退出值非0

#### DataX调度流程：

举例来说，用户提交了一个DataX作业，并且配置了20个并发，目的是将一个100张分表的mysql数据同步到odps里面。	DataX的调度决策思路是：

1. DataXJob根据分库分表切分成了100个Task。
2. 根据20个并发，DataX计算共需要分配4个TaskGroup。
3. 4个TaskGroup平分切分好的100个Task，每一个TaskGroup负责以5个并发共计运行25个Task。

## 五、DataX 3.0六大核心优势

- #### 可靠的数据质量监控

  - 提供作业全链路的流量、数据量运行时监控

     DataX-fs-cp运行过程中可以将作业本身状态、数据流量、数据速度、执行进度等信息进行全面的展示，让用户可以实时了解作业状态。并可在作业执行过程中智能判断源端和目的端的速度对比情况，给予用户更多性能排查信息。

  - 提供脏数据探测

     在大量数据的传输过程中，必定会由于各种原因导致很多数据传输报错(比如类型转换错误)，这种数据DataX认为就是脏数据。DataX目前可以实现脏数据精确过滤、识别、采集、展示，为用户提供多种的脏数据处理模式，让用户准确把控数据质量大关！

- #### 精准的速度控制

  还在为同步过程对在线存储压力影响而担心吗？新版本DataX-fs-cp提供了包括通道(并发)、记录流、字节流三种流控模式，可以随意控制你的作业速度，让你的作业在库可以承受的范围内达到最佳的同步速度。

  ```json
  "speed": {
     "channel": 5,
     "byte": 1048576,
     "record": 10000
  }
  ```

- #### 强劲的同步性能

  DataX-fs-cp每一种读插件都有一种或多种切分策略，都能将作业合理切分成多个Task并行执行，单机多线程执行模型可以让DataX速度随并发成线性增长。在源端和目的端性能都足够的情况下，单个作业一定可以打满网卡。另外，DataX团队对所有的已经接入的插件都做了极致的性能优化，并且做了完整的性能测试。性能测试相关详情可以参照每单个数据源的详细介绍：[DataX数据源指南](https://github.com/alibaba/DataX/wiki/DataX-all-data-channels)

- #### 健壮的容错机制

  DataX作业是极易受外部因素的干扰，网络闪断、数据源不稳定等因素很容易让同步到一半的作业报错停止。因此稳定性是DataX的基本要求，在DataX 3.0的设计中，重点完善了框架和插件的稳定性。目前DataX-fs-cp可以做到线程级别、进程级别(暂时未开放)、作业级别多层次局部/全局的重试，保证用户的作业稳定运行。

  - 线程内部重试

     DataX的核心插件都经过团队的全盘review，不同的网络交互方式都有不同的重试策略。

  - 线程级别重试

     目前DataX已经可以实现TaskFailover，针对于中间失败的Task，DataX框架可以做到整个Task级别的重新调度。

- #### 极简的使用体验

  - 易用

     下载即可用，支持linux和windows，只需要短短几步骤就可以完成数据的传输。请点击：[Quick Start](https://github.com/alibaba/DataX/wiki/Quick-Start)

  - 详细

     DataX在运行日志中打印了大量信息，其中包括传输速度，Reader、Writer性能，进程CPU，JVM和GC情况等等。

    - 传输过程中打印传输速度、进度等	

      ![datax_run_speed](https://cloud.githubusercontent.com/assets/1067175/17850877/d1612c0a-6891-11e6-9970-d6693c15ef24.png)

    - 传输过程中会打印进程相关的CPU、JVM等

      ![datax_run_cpu](https://cloud.githubusercontent.com/assets/1067175/17850903/ee63c2fe-6891-11e6-9056-97d7e3d13d8d.png)

    - 在任务结束之后，打印总体运行情况

      ![datax_end_info](https://cloud.githubusercontent.com/assets/1067175/17850930/0484d3ac-6892-11e6-9c1d-b102ad210a32.png)

## 六、DataX-fs-cp默认参数 

在原有的dataX上注入了一些常用的默认参数，如下：

${dataX.current.date} (程序运行时的当天，格式：yyyy-MM-dd)

${dataX.1.days.ago} (程序运行时的前一天，格式：yyyy-MM-dd)

${dataX.2.days.ago} (程序运行时的前两天，格式：yyyy-MM-dd)

${dataX.3.days.ago} (程序运行时的前三天，格式：yyyy-MM-dd)

${dataX.7.days.ago} (程序运行时的前七天，格式：yyyy-MM-dd)

${dataX.30.days.ago} (程序运行时的前三十天，格式：yyyy-MM-dd)