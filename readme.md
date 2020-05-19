# SparkETL
主要运用spark SQL实现数据仓库etl。从extract、transform到导出到其他数据库，基本是写sql方式实现。
实现从数据库抽取数据，在spark上实现etl主逻辑，将数据仓库加工后的数据再导入到RdbMS中供后续使用。sql满足不了的再需要调用spark接口实现。
## 公共约定
* 数据库、文件、程序数据处理等需要指定字符集的都是UTF-8
* 事实表以时间分区的,分区键都是time_type,time_id

## 环境
* CentOS 7.7
* OpenJDK 1.8.0_242
* CDH6.3.2
### DB
* Oracle 19c
* MySQL 8.0.19
* SQLServer 2019 Developer
* PostgreSQL 12.2  
* DB2 11.5 # TODO
* MongoDB 4.2.5  # TODO
* Elasticsearch 7.6.2 # 用hive表映射方式，参考： https://www.elastic.co/guide/en/elasticsearch/hadoop/current/hive.html
* Redis 5.0.8 # TODO
* Apache Kudu 1.10.0 # TODO

## 数据
* github：https://github.com/wuda0112/mysql-tester
* 生成数据：java -jar mysql-tester-1.0.1.jar --mysql-username=test --mysql-password=Test123$ --user-count=1000 --max-item-per-user=100 --thread=10 --mysql-url=jdbc:mysql://127.0.0.1:3306/?serverTimezone=UTC&characterEncoding=UTF-8 

## 调用
spark-submit --master yarn --class etl.App --driver-memory 512m --executor-memory 512m /dp/bin/etl.jar prod 1 2020-03-23
### idea远程调试
spark-submit --master yarn --class etl.App --driver-memory 512m --executor-memory 512m --driver-java-options "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005" /dp/bin/etl.jar  prod 1 2020-03-23

## 记录应用日志
/etc/spark/conf/log4j.properties 增加
```
log4j.logger.etl=DEBUG,rollingFile
log4j.appender.rollingFile=org.apache.log4j.RollingFileAppender
log4j.appender.rollingFile.Threshold=DEBUG
log4j.appender.rollingFile.ImmediateFlush=true
log4j.appender.rollingFile.Append=true
log4j.appender.rollingFile.File=/dp/log/etl.log
log4j.appender.rollingFile.MaxFileSize=50MB
log4j.appender.rollingFile.MaxBackupIndex=5
log4j.appender.rollingFile.layout=org.apache.log4j.PatternLayout
log4j.appender.rollingFile.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} [%p] %m%n
```
## 具体使用方式见
[SparkETL 用Spark SQL实现ETL](https://blog.csdn.net/dazheng/article/details/105370358)


