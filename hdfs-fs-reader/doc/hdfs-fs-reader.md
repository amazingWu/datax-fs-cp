# DataX-fs-cp hdfs-fs-reader 插件文档


------------

## 1 快速介绍

hdfs-fs-reader提供了读取分布式文件系统数据存储的能力。在底层实现上，hdfs-fs-reader获取分布式文件系统上文件的数据，并转换为DataX传输协议传递给Writer。


## 2 功能与限制

hdfs-fs-reader实现了从Hadoop分布式文件系统Hdfs中读取文件数据并转为DataX协议的功能。

1. 支持任意类型的数据格式

1. 支持递归读取、支持正则表达式（"*"和"?"）。

1. 多个File可以支持并发读取。

1. 目前插件中Hive版本为1.1.1，Hadoop版本为2.7.1（Apache［为适配JDK1.7］,在Hadoop 2.5.0, Hadoop 2.6.0 和Hive 1.2.0测试环境中写入正常；其它版本需后期进一步测试； 

1. 支持kerberos认证（注意：如果用户需要进行kerberos认证，那么用户使用的Hadoop集群版本需要和hdfsreader的Hadoop版本保持一致，如果高于hdfsreader的Hadoop版本，不保证kerberos认证有效）

我们暂时不能做到：

1. 单个File支持多线程并发读取。

2. 目前还不支持hdfs HA;

## 3 功能说明


### 3.1 配置样例

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "hdfs-file-reader",
          "parameter": {
            "defaultFS": "hdfs://hadoop001:9000",
            "path": [
              "/user/test/test1"
            ]
          }
        },
        "writer": {
          "name": "local-file-writer",
          "parameter": {
            "path": "src/test/resources/hdfs-2-local/to",
            "writeMode": "overwrite"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "2"
      },
      "errorLimit": {
        "record": 0
      }
    }
  }
}
```

### 3.2 参数说明（各个配置项值前后不允许有空格）

* **path**

	* 描述：要读取的文件路径，如果要读取多个文件，可以使用正则表达式"*"，注意这里可以支持填写多个路径。。 <br />

		当指定单个Hdfs文件，hdfs-fs-reader暂时只能使用单线程进行数据抽取。

		当指定多个Hdfs文件，hdfs-fs-reader支持使用多线程进行数据抽取。线程并发数通过通道数指定。

		当指定通配符，hdfs-fs-reader尝试遍历出多个文件信息。例如: 指定/*代表读取/目录下所有的文件，指定/bazhen/\*代表读取bazhen目录下游所有的文件。hdfs-fs-reader目前只支持"*"和"?"作为文件通配符。

	* 必选：是 <br />

	* 默认值：无 <br />

* **defaultFS**

	* 描述：Hadoop hdfs文件系统namenode节点地址。 <br />

		**目前hdfs-fs-reader已经支持Kerberos认证，如果需要权限认证，则需要用户配置kerberos参数，见下面**

	* 必选：是 <br />

	* 默认值：无 <br />


* **haveKerberos**

	* 描述：是否有Kerberos认证，默认false<br />
 
		 例如如果用户配置true，则配置项kerberosKeytabFilePath，kerberosPrincipal为必填。

 	* 必选：haveKerberos 为true必选 <br />
 
 	* 默认值：false <br />

* **kerberosKeytabFilePath**

	* 描述：Kerberos认证 keytab文件路径，绝对路径<br />

 	* 必选：否 <br />
 
 	* 默认值：无 <br />

* **kerberosPrincipal**

	* 描述：Kerberos认证Principal名，如xxxx/hadoopclient@xxx.xxx <br />

 	* 必选：haveKerberos 为true必选 <br />
 
 	* 默认值：无 <br />

	
* **hadoopConfig**

	* 描述：hadoopConfig里可以配置与Hadoop相关的一些高级参数，比如HA的配置。<br />

		```json
		"hadoopConfig":{
		        "dfs.nameservices": "testDfs",
		        "dfs.ha.namenodes.testDfs": "namenode1,namenode2",
		        "dfs.namenode.rpc-address.aliDfs.namenode1": "",
		        "dfs.namenode.rpc-address.aliDfs.namenode2": "",
		        "dfs.client.failover.proxy.provider.testDfs": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
		}
		```

	* 必选：否 <br />
 
 	* 默认值：无 <br />



### 3.4 按分区读取

Hive在建表的时候，可以指定分区partition，例如创建分区partition(day="20150820",hour="09")，对应的hdfs文件系统中，相应的表的目录下则会多出/20150820和/09两个目录，且/20150820是/09的父目录。了解了分区都会列成相应的目录结构，在按照某个分区读取某个表所有数据时，则只需配置好json中path的值即可。

比如需要读取表名叫mytable01下分区day为20150820这一天的所有数据，则配置如下：

```json
"path": "/user/hive/warehouse/mytable01/20150820/"
```


## 5 约束限制

略

## 6 FAQ

略

