package etl.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public interface DB {

    /**
     * 释放数据库及其他申请的资源
     *
     * @throws Exception
     */
    void release() throws Exception;

    /**
     * spark接口方式读取数据
     *
     * @param spark SparkSession
     * @param sql   查询SQL语句
     */
    void read(SparkSession spark, String sql);

    /**
     * spark接口方式写入数据
     *
     * @param df    Dataset
     * @param table 表名
     */
    void write(Dataset<Row> df, String table);

    /**
     * 获取导出方式
     *
     * @return export实现类
     * @throws Exception
     */
    BiConsumer<String, String> getExport() throws Exception;

    /**
     * 获取加载方式
     *
     * @return load实现类
     * @throws Exception
     */
    BiConsumer<String, List<String>> getLoad() throws Exception;

    /**
     * 执行SQL语句。除支持jdbc的数据库外，其他只支持简单的删除
     *
     * @param sql
     * @throws Exception
     */
    void exeSQL(String sql) throws Exception;

    /**
     * 获取表的列
     *
     * @param table 表名
     * @return 列字符串
     * @throws Exception
     */
    String getTableColumns(String table) throws Exception;

    /**
     * 获取要导入表的文件名
     *
     * @param table    表名
     * @param timeType 时间类型
     * @return 文件名列表
     * @throws IOException
     */
    default List<String> getLoadFiles(String table, String timeType) throws IOException {
        String dir = Public.getTableDataDirectory(table, timeType);
        return Files.list(Paths.get(dir)).map(Path::toFile)
            .filter(file -> file.isFile() && !file.isHidden() && file.length() > 0).map(File::getAbsolutePath)
            .collect(Collectors.toList());
    }
}
