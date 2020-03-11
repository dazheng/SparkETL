## 改写自python版的etl
## 调用
spark-submit --master yarn --class etl.App --driver-memory 512m --executor-memory 512m /dp/bin/etl-0.1-jar-with-dependencies.jar
## idea远程调试
spark-submit --master yarn --class etl.App --driver-memory 512m --executor-memory 512m --driver-java-options "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005" /dp/bin/etl-0.1-jar-with-dependencies.jar
