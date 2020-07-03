# DataX hdfs-fs-writer 插件文档


------------

## 1 快速介绍

hdfs-fs-writer提供向HDFS文件系统指定路径中写入文件的能力


## 2 功能与限制
实现原理：   
根据reader端读取的文件层级结构，按相应的结构写入，保持文件的层级信息，因此支持Hive的动态分区信息。  
写入时会在目标路径下生成临时文件夹，该文件夹以__fscp__开头，会先把文件拷贝到该路径下，结束后会将临时目录下的文件移动到目标路径。  
写入时如果是覆盖式写入，会直接覆盖目标文件，如果是更新方式，会根据文件长度信息来判断是否需要进行更新，如果长度一致则跳过更新。    

功能：
* 支持按文件层级结构写入
* 支持任意文件格式
* 支持Kerberos

限制：
* 目前插件中Hive版本为1.1.1，Hadoop版本为2.7.1（Apache［为适配JDK1.7］,在Hadoop 2.5.0, Hadoop 2.6.0 和Hive 1.2.0测试环境中写入正常；其它版本需后期进一步测试；
* 目前hdfs-fs-writer支持Kerberos认证（注意：如果用户需要进行kerberos认证，那么用户使用的Hadoop集群版本需要和hdfsreader的Hadoop版本保持一致，如果高于hdfsreader的Hadoop版本，不保证kerberos认证有效）

## 3 功能说明


### 3.1 配置样例

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "local-file-reader",
          "parameter": {
            "path": [
              "E:\\test"
            ]
          }
        },
        "writer": {
          "name": "hdfs-file-writer",
          "parameter": {
            "defaultFS": "hdfs://hadoop001:9000",
            "path": "/user/test/test1",
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

### 3.2 参数说明

* **defaultFS**

	* 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9000<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **path**

	* 描述：存储到Hadoop hdfs文件系统的路径信息，hdfs-fs-writer会根据并发配置在Path目录下写入多个文件。为与hive表关联，请填写hive表在hdfs上的存储路径。例：Hive上设置的数据仓库的存储路径为：/user/hive/warehouse/ ，已建立数据库：test，表：hello；则对应的存储路径为：/user/hive/warehouse/test.db/hello  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **writeMode**

 	* 描述：hdfswriter写入前数据清理处理模式： <br />

		* overwrite，直接覆盖目标文件
		* update，如果长度一致则跳过拷贝，否则进行覆盖式写入。
		* deleteIfExist，如果目标路径path已经存在，则先删除整个目录

	* 必选：是 <br />

	* 默认值：overwrite <br />

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


## 4 配置步骤
略

## 5 约束限制

略

## 6 FAQ

略
