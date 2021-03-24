# mars/火星实时物联数据库

以记录物联网时代各种传感器数据为目的的，高性能实时传感器数据库。物联网项目用作数据存储的，主要有关系型数据库、实时库、时序库三种类型。关系型数据库、时序库都只提供数据存储功能，而实时数据服务，历史数据断电续传、数据冗余等项目种的特有功能是没有的,需要客户自己处理;传统工业系统中使用的实时库，在灵活性、新式接口、分布式的支持等方面略显不足。该项目就是想兼顾各自的优缺点，开发出一款适应这个时代的工业互联网实时数据库（**实时物联数据库**）。该项目的目标：在单台服务器上达到**300~500万**个传感器数据按照秒级变化的数据的历史存储，分布式版本可以达到**上千万、亿级别**。

## 功能

  Mars 数据库整个分成开发、运行2个部分，两者之间相互独立。数据库的开发用于管理数据库支持的变量、安全信息、接口信息等；数据库的运行负责数据的采集、存储、对外提供实时、历史数据服务。
  
  数据库开发支持分布式、多客户端、多数据库开发；开发客户端目前仅支持在Windows下运行的桌面客户端；服务器则是跨平台的，对外提供GRPC、Web API接口，方便第三方进行扩展接入。
  
  数据库运行支持跨平台、多种类型数据访问接口。支持有Web API、GRPC、自定义私有协议接口、OPC UA。数据库运行时采用数据库核心服务和API接口独立分开的多进程设计，API 服务和数据库核心功能可以在一台机器上也可以分别在不同的机器上；同时运行有多中类型的API接口、同一个种多个API接口同时运行，以支持多种、多个类型的客户端同时访问。
  
  数据库运行时不支持直接添加、删除、修改变量，但支持数据库的热启动（即在不退出重启的情况下，可以动态加载生效数据库库开发时新增、修改的变量，不支持删除的变量动态生效）。
  
  历史数据存储支持**无压缩**、**无损压缩**、**死区压缩**、**斜率死区压缩（旋转门算法）** 4种压缩方式，同时支持**数据补录**功能（配合设备驱动实现当网络中断、又恢复后历史数据补录的功能）。
  
  历史数据支持定时导出功能，由于采用独立的文件存储设计，历史数据的导入，只要将相应的文件拷贝到历史存储目录即可。
  
  数据库支持双机冗余热备功能，历史数据路径存储到第三方的物理磁盘上，实时数据支持实时数据同步。

## 接口
1. 基于消费端的上接口。基于**Web API**、**Grpc**、**OPC-UA**、**私有协议高速接口**等。
2. 基于数据采集端的下接口 [Spider ](https://github.com/cdy816/Spider) ([Gitee地址](https://gitee.com/chongdaoyang/Spider))

## 运行环境
系统采用.net 5 平台开发,依赖于.net 5 的**跨平台**性，可部署在window、Linux等操作系统中,也可以部署在Docker中。 

## 沟通交流
QQ 群:950906131

## 帮助文档、接口开发文档
[文档](https://github.com/cdy816/mars/tree/master/Doc)([Gitee地址](https://gitee.com/chongdaoyang/mars/tree/master/Doc))

## 版本
1. [0.35 版本](https://github.com/cdy816/mars/releases/tag/V0.35)

## 未来计划
1. **2021年3月发布1.0版本**，完成单机版功能，同时性能尽量达到单机100万点左右。
2. **2022年1月份发布1.1版本**，完成单机性能在300~500万点左右的目标。
3. 2022年2月份开始分布式版本的开发，预计**2025年10月份能够推出分布式版本的2.0**。分布式版本通过计算机集群、多级协作的方式，来实现更大规模、变化更加快速的传感器数据的接入。

## 合作伙伴

  **lin**

## 应用
1. [Mar数据库应用结构](https://my.oschina.net/u/3520380/blog/4288058)
