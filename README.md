# mars/火星实时物联数据库
High performance realtime database.Our goal is to store the second level data of 3-10 million sensors on a single server.

以记录物联网时代各种传感器数据为目的的，高性能实时传感器数据库。相较于传统工业系统中的实时库，该库提供了基于互联网的各种特性，相较于时序库该库提供了以单个传感器数据（变量）为记录单位的记录和管理数据的方式较时序库仍然以表为记录单位方式的不同。该项目的目标：在单台服务器上达到**300~1000万**个传感器数据按照秒级变化的数据的历史存储，分布式版本可以达到**上亿以及十亿级别**。

目前测试的结果是，在如下配置的商务台式电脑上可以达到**100**万个传感器数据按照秒级变化的历史存储。

* CPU： I7 4790,主频：3.6, 4核8线程
* 内存：16G
* 磁盘: 机械磁盘 512G,7200转

在如下配置的服务器上，实现了**300-万**个传感器数据按照秒级变化的历史存储。
* CPU： E5-2650 V4,主频：2.2, 双12核24线程
* 内存：64G
* 磁盘: 机械磁盘 2T,7200转


## 功能
1. 实时数据服务。提供数据的实时值的查询、修改服务。
2. 历史数据存储。提供对实时数据按照秒级进行存储，存储类型包括定时、值变化存储2种模式。历史数据的压缩提供了：**无压缩**、**无损压缩**、**死区压缩**、**斜率死区压缩（旋转门算法）** 4种压缩方式。
3. 外部访问接口。基于**Web API**、**Grpc**、**OPC-UA**等各种上、下访问接口。
4. 数据类型：byte,short,ushort,int,uint,long,ulong,double,float,string,IntPoint(int,int),LongPoint(long,long)。通过IntPoint,LongPoint可以实现对Gis位置信息的高效的压缩存储，从而实现一个**地理信息数据库**。

## 程序集
1. DbInRun: 提供数据库的实时、历史数据服务。
2. DbInStudio,DbInStudioServer:提供分布式、多客户端数据变量的开发配置。
3. HisDataTools:提供对已经记录的历史数据的查询、导入、导出和数据分析功能。
4. DBHightApi,DBWebApi: 提供数据库的实时数据的访问API服务。
5. DbHisDataServer: 提供数据库历史数据的访问API服务。

## 接口
1. 基于消费端的上接口
2. 基于数据采集端的下接口 [Spider ](https://github.com/cdy816/Spider) ([Gitee地址](https://gitee.com/chongdaoyang/Spider))

## 运行环境
系统采用.net core 3.1 平台开发,依赖于.net core 的跨平台性，可部署在window、Linux等操作系统中,也可以部署在Docker中。 

## 沟通交流
QQ 群:950906131

## 帮助文档、接口开发文档
[文档](https://github.com/cdy816/mars/wiki)([Gitee地址](https://gitee.com/chongdaoyang/mars/wikis/Home))

## 版本
尚未发布。

## 未来计划
1. **2020年10月发布1.0版本**，完成单机版功能，同时性能尽量达到单机100万点左右。
2. **2021年1月份发布1.1版本**，完成单机性能在300~500万点左右的目标。
3. 2021年2月份开始分布式版本的开发，预计**2023年10月份能够推出分布式版本的2.0**。分布式版本通过计算机集群、多级协作的方式，来实现更大规模、变化更加快速的传感器数据的接入。

## 应用
1. [Mar数据库应用结构](https://my.oschina.net/u/3520380/blog/4288058)
