# SparkETL
主要运用spark SQL实现数据仓库etl。从extract、transform到导出到其他数据库，基本是写sql方式实现。
实现从数据库抽取数据，在spark上实现etl主逻辑，将数据仓库加工后的数据再导入到RDBMS中供后续使用。sql满足不了的再需要写程序实现。
## 环境
* CentOS 7.7
* OpenJDK 1.8.0_242
* CDH6.3.2
### DB
* Oracle 19c
* MySQL 8.0.19
* SQLServer 2019 Developer # TODO
* PostgreSQL  #TODO 
* DB2 # TODO
* MongoDB  
* Elasticsearch # TODO 

## 数据
https://blog.csdn.net/wuda0112/article/details/88387735

## 调用
spark-submit --master yarn --class etl.App --driver-memory 512m --executor-memory 512m /dp/bin/etl.jar
### idea远程调试
spark-submit --master yarn --class etl.App --driver-memory 512m --executor-memory 512m --driver-java-options "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005" /dp/bin/etl.jar
