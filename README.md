[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![Apache License][license-shield]][license-url]

[![star](https://gitee.com/chongdaoyang/mars/badge/star.svg?theme=white)](https://gitee.com/chongdaoyang/mars/stargazers)
[![fork](https://gitee.com/chongdaoyang/mars/badge/fork.svg?theme=white)](https://gitee.com/chongdaoyang/mars/members)

 **物联网三板斧：[Mars 实时库](https://github.com/cdy816/mars) 、设备采集平台[Spider](https://github.com/cdy816/Spider) 、跨平台界面解决方案[Chameleon](https://github.com/cdy816/Chameleon)**
 
<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://github.com/cdy816/mars">
    <img src="/Doc/Images/mrdbr.png" alt="Logo" width="128" height="80">
  </a>

  <h2 align="center">Mars 实时库</h2>
 
  <p align="center">
    高性能、跨平台实时库!        
    <br />
    <br />
    <a href="https://github.com/cdy816/mars/tree/master/Doc">帮助文档</a>
    ·
    <a href="https://github.com/cdy816/mars/issues">Bug 提交</a>
    ·
    <a href="https://github.com/cdy816/mars/issues">功能申请</a>
  </p>
</p>

# mars/火星实时物联数据库

 在一个物联网、大数据的时代，需要完成对海量的、各种传感器数据的采集、存储。传感器数据的特点是：时序的、海量的。目前主要有关系型数据库、时序库、实时库三种类型数据库可用于对数据记录存储。区别于结构化的数据，这类数据具有时间的特性.
   
   如果使用关系型数据库，则需要自己设计数据库表的结构使其能够高效的存储数据，由于海量数据的存在，对数据的压缩显得尤为重要，而这些是传统的关系型数据库所不具备的，需要自己实现。
   
   时序数据库设计的初衷，就是为了弥补关系型数据库在存储时序性数据方面的缺点，但是它自身的定位还是在一种存储软件上；而现实中的应用除了需要将数据存储下来之外还有传感器的实时值服务、传感器的采集等工作是其不曾涉及的；同样这些需要使用者自己开发功能，而对于一个有着几十万、上百万、千万级别的系统来说，设计一套传感器实时数据的采集、实时数据的管理、服务等功能，同样需要较高的要求；这一点在传统工业领域使用的实时库，能够较好的弥补时序数据的不足。
   
   而工业系统中使用的实时库，在灵活性、新式接口、分布式的支持等方面略显不足。该项目就是想兼顾各自的优缺点，开发出一款适应这个时代的工业互联网实时数据库（**实时物联数据库**）。该项目的目标：在单台服务器上达到**300~500万**个传感器数据按照秒级变化的数据的历史存储，分布式版本可以达到**上千万、亿级别**。
   
  

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

## 开始使用

### 安装

 1. 安装.net 运行环境 [参考微软官方文档](https://dotnet.microsoft.com/download/dotnet/5.0)
 2. 数据库安装。下载Mars 发部版本，将软件包解压到特定目录。

### 手动构建

 1. 安装 [VS 2019 开发环境](https://visualstudio.microsoft.com/zh-hans/vs/)
 2. Clone [Mars 工程](https://github.com/cdy816/mars)


### 使用流程

 1. 使用DbInStudio 进行开发配置
 2. 使用DbInRun 运行配置结果
 3. 运行 XXXAPI 对外提供不同类型的服务
 4. 通过 InSpiderDevelopWindow 进行数据采集驱动配置
 5. 运行 InSpiderRun.exe 进行传感器设备采集


## 帮助文档、接口开发文档
1. [文档](https://github.com/cdy816/mars/tree/master/Doc)([Gitee地址](https://gitee.com/chongdaoyang/mars/tree/master/Doc))
2. [Mar数据库应用结构](https://my.oschina.net/u/3520380/blog/4288058)

## 版本
[0.35 版本](https://github.com/cdy816/mars/releases/tag/V0.35)

## 路线图
项目分成2个阶段：单机、分布式。

1. 2021年完成第一阶段，同时性能尽量达到单机300万点左右。
2. 2022年2月份开始分布式版本的开发，预计2025年10月份能够推出分布式版本的2.0。分布式版本通过计算机集群、多级协作的方式，来实现更大规模、变化更加快速的传感器数据的接入。

## 沟通交流

1. QQ 群:950906131
2. Email:cdy816@hotmail.com

## 合作伙伴

非常欢迎你的加入！提一个 [Issue](https://github.com/cdy816/mars/issues)  或者提交一个 Pull Request。

感谢以下人员的参与：

  **lin**

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/cdy816/mars.svg?style=for-the-badge
[contributors-url]: https://github.com/cdy816/mars/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/cdy816/mars.svg?style=for-the-badge
[forks-url]:https://github.com/cdy816/mars/network/members
[stars-shield]: https://img.shields.io/github/stars/cdy816/mars.svg?style=for-the-badge
[stars-url]:https://github.com/cdy816/mars/stargazers
[issues-shield]: https://img.shields.io/github/issues/cdy816/mars.svg?style=for-the-badge
[issues-url]:https://github.com/cdy816/mars/issues
[license-shield]: https://img.shields.io/github/license/cdy816/mars.svg?style=for-the-badge
[license-url]: https://github.com/cdy816/mars/blob/master/LICENSE.txt
[product-screenshot]:https://github.com/cdy816/mars/blob/master/Doc/Images/DbInStudio.png
