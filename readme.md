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
* SQLServer 2019 Developer
* PostgreSQL  
* DB2 
* MongoDB  # TODO
* Elasticsearch # TODO 

## 数据
https://github.com/wuda0112/mysql-tester
java -jar mysql-tester-1.0.1.jar --mysql-username=test --mysql-password=Test123! --user-count=100000 --max-item-per-user=100 --thread=10 --mysql-url=jdbc:mysql://127.0.0.1:3306/?serverTimezone=UTC&characterEncoding=UTF-8 

## 调用
spark-submit --master yarn --class etl.App --driver-memory 512m --executor-memory 512m /dp/bin/etl.jar prod 1 2020-03-23
### idea远程调试
spark-submit --master yarn --class etl.App --driver-memory 512m --executor-memory 512m --driver-java-options "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005" /dp/bin/etl.jar  prod 1 2020-03-23

## 具体使用方式见
[SparkETL 用Spark SQL实现ETL](https://blog.csdn.net/dazheng/article/details/105370358)
