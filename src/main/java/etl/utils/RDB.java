package etl.utils;

import com.moandjiezana.toml.Toml;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;


/**
 * 对rdbms的处理
 */
public class RDB {
    private final Logger logger = LoggerFactory.getLogger(RDB.class);
    private final String driverClass;
    private final String url;
    private final String user;
    private final String password;
    private final String dbType;
    private Connection conn;

    RDB(Toml db) throws SQLException, ClassNotFoundException {
        this.driverClass = db.getString("driver_class");
        this.url = db.getString("url");
        this.user = db.getString("user");
        this.password = db.getString("password");
        this.dbType = new Public.JdbcUrlSplitter(this.url).driverName;
        this.conn = connection();
    }

    protected String getDbType() {
        return this.dbType;
    }

    /**
     * 释放申请的资源
     *
     * @throws SQLException
     */
    protected void release() throws SQLException {
        close(this.conn);
    }

    private void close(Connection conn) throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    private void close(Statement stmt) throws SQLException {
        if (stmt != null) {
            stmt.close();
        }
    }


    private void close(ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
    }


    @NotNull
    private Connection getConnection() {
        return this.conn;
    }

    /**
     * 连接数据库
     *
     * @return 数据库连接
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @NotNull
    private Connection connection() throws ClassNotFoundException, SQLException {
        Class.forName(this.driverClass);
        Connection conn = DriverManager.getConnection(this.url, this.user, this.password);
        conn.setAutoCommit(true);
        return conn;
    }

    /**
     * 执行插入的SQL
     *
     * @param sql
     * @throws SQLException
     */
    void exeSQL(String sql) throws SQLException {
        LocalDateTime start = LocalDateTime.now();
        this.logger.info(Public.getMinusSep());
        this.logger.info(sql);
        PreparedStatement stmt = getConnection().prepareStatement(sql);
        stmt.executeUpdate();
        close(stmt);
        Public.printDuration(start, LocalDateTime.now());
    }


    /**
     * 获取表的字段名
     *
     * @param table
     * @return 以逗号分隔的表字段字符串
     * @throws SQLException
     */
    String getTableColumns(String table) throws Exception {

        String sql = "";
        switch (this.dbType) {
            case "mysql":
                sql = "select column_name from information_schema.columns where table_schema = ? and table_name = ? order by ordinal_position";
                break;
            case "oracle":  // TODO: 排序
                sql = "select column_name from user_tab_columns where table_name = ? order by column_id";
                break;
            case "sqlserver":
                sql = "select column_name from information_schema.columns where table_catalog = ? and table_name = ? order by ordinal_position";
                break;
            case "postgresql":
                sql = "select column_name from information_schema.columns where table_catalog = ? and table_name = ? order by ordinal_position";
                break;
            case "db2":
                sql = "SELECT colname column_name from syscat.columns where tabschema = ? and tabname = ? order by colno";
            default:
                this.logger.error("not support {}", this.dbType);
                throw new Exception("not support " + this.dbType);
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String s = "";

        try {
            String schema = new Public.JdbcUrlSplitter(this.url).database;
            stmt = getConnection().prepareStatement(sql);
            if ("oracle".equals(this.dbType)) {
                stmt.setString(1, table.toUpperCase());
            } else {
                stmt.setString(1, schema);
                stmt.setString(2, table);
            }
            rs = stmt.executeQuery();
            StringBuilder cs = new StringBuilder();
            while (rs.next()) {
                cs.append(rs.getString("column_name"));
                cs.append(",");
            }

            s = cs.toString();
            if (s.length() > 0) {
                s = s.substring(0, s.length() - 1);
            }
        } finally {
            close(rs);
            close(stmt);
        }
        return s;
    }

    /**
     * 获取要导入表的文件名
     *
     * @param table    表名
     * @param timeType 时间类型
     * @return 文件名列表
     * @throws IOException
     */
    protected List<String> getLoadFiles(String table, String timeType) throws IOException {
        String dir = Public.getTableDataDirectory(table, timeType);
        return Files.list(Paths.get(dir)).map(Path::toFile)
            .filter(file -> file.isFile() && !file.isHidden() && file.length() > 0).map(File::getAbsolutePath)
            .collect(Collectors.toList());
    }

    protected void MySQLLoad(String table, List<String> files) throws SQLException {
        for (String file : files) {
            String sql = String.format(
                "load data local infile '%s' replace into table %s Fields Terminated By %s Lines Terminated By '\\n' ",
                file, table, Public.getColumnDelimiterRDB());
            exeSQL(sql);
        }
    }

    /**
     * 调用oracle sqlldr load数据
     * jdbc url需要配置成thin service方式
     *
     * @param table 表名
     * @param files 文件名
     * @throws IOException
     * @throws InterruptedException
     */
    // .ctl文件需要提前定义好,客户端安装sqlldr工具, 要求db与实例名字一致
    protected void OracleLoad(String table, List<String> files) throws IOException, InterruptedException {
        String db = new Public.JdbcUrlSplitter(url).database;
        String host = new Public.JdbcUrlSplitter(url).host;
        String port = new Public.JdbcUrlSplitter(url).port;
        for (String file : files) {
            String ctlFile = Public.getConfDirectory() + table + ".ctl";
            String logDir = Public.getLogDirectory() + table + "/";
            Files.createDirectories(Paths.get(logDir));
            String logFile = logDir + table + ".log";
            String badFile = logDir + table + ".bad";
            String discardFile = Public.getLogDirectory() + table + ".dcd";
            String cmd = String.format(
                "sqlldr %s/%s@%s:%s/%s control=%s data=%s log=%s bad=%s discard=%s direct=true", this.user,
                this.password, host, port, db, ctlFile, file, logFile, badFile, discardFile
            );
            Public.exeCmd(cmd);
        }
    }

    /**
     * microsoft sql server导入数据
     * 参考：https://docs.microsoft.com/zh-cn/sql/relational-databases/import-export/bulk-import-and-export-of-data-sql-server?view=sql-server-ver15
     * bulk insert方式文件必须在SQL server服务器上，或者通过指定通用命名约定 (UNC) 名称
     * 需要先生成格式文件，然后再导入导出。如bcp individual_user format nul -dtest -S192.168.1.5 -Usa -PTest123$ -c -t0x01 -f/dp/conf/stg.s_user.fmt
     * 导入：bcp individual_user in /dp/data/hive_exchange/stg.s_user.txt -dtest -S192.168.1.5 -Usa -PTest123$ -f /dp/conf/stg.s_user.fmt -e/dp/log/stg.s_user.bad
     * 导出： bcp "select * from individual_user" queryout /dp/data/hive_exchange/b.txt -dtest -S192.168.1.5 -Usa -PTest123$ -f /dp/conf/stg.s_user.fmt -e/dp/log/stg.s_user.bad
     *
     * @param table 表名
     * @param files 文件列表
     * @throws SQLException
     */
    protected void SQLServerLoad(String table, List<String> files) throws IOException, InterruptedException {
        String db = new Public.JdbcUrlSplitter(this.url).database;
        String host = new Public.JdbcUrlSplitter(this.url).host;
        String ctlFile = Public.getConfDirectory() + table + ".fmt";
        for (String file : files) {
//            String sql = String.format(
//                "bulk insert %s from '%s' WITH(FIELDTERMINATOR＝%s, ROWTERMINATOR＝'\\n', batchsize=100000)",
//                table, file, Public.getColumnDelimiterRDB());
//            exeSQL(sql);
            String cmd = String.format(
                "bcp %s in %s -d%s -S%s -U%s -P%s -f%s",
                table, file, db, host, this.user, this.password, ctlFile
            );
            Public.exeCmd(cmd);
        }
    }


    /**
     * 没有采用db2 load方式是因为：1、数据量一般没有那么大；2、load用不好会导致表不可用
     *
     * @param table 表名称
     * @param files 要加载的文件
     * @throws IOException
     * @throws InterruptedException
     */
    protected void DB2Load(String table, List<String> files) throws SQLException {
        for (String file : files) {
            String sql = String.format(
                "import from %s of del modified by coldel%s insert into %s",
                file, Public.getColumnDelimiterRDB(), table);
            exeSQL(sql);
        }
    }

    /**
     * 参考： https://www.postgresql.org/docs/12/sql-copy.html
     * copy支持服务器端，客户端不支持。pg_bulkload也是只支持服务器端
     * TODO: 客户端load方式
     *
     * @param table
     * @param files
     * @throws IOException
     * @throws InterruptedException
     */
    protected void PostgreSQLLoad(String table, List<String> files) throws IOException, InterruptedException {
        String db = new Public.JdbcUrlSplitter(this.url).database;
        String host = new Public.JdbcUrlSplitter(this.url).host;
        String logDir = Public.getLogDirectory() + table + "/";
        Files.createDirectories(Paths.get(logDir));
        String logFile = logDir + table + ".log";
        String badFile = logDir + table + ".bad";

//        pg_bulkload --dbname lottu --username lottu --password --writer=PARALLEL --input /home/postgres/tbl_lottu_output.txt --output tbl_lottu --logfile /home/postgres/tbl_lottu_output.log --parse-badfile=/home/postgres/tbl_lottu_bad.bad  --option "TYPE=CSV" --option "DELIMITER=|" -- 依赖pg server程序
//        copy shop from '/dp/data/stg.s_shop.txt' delimiter E'\x01' ; -- 服务器端导入
        for (String file : files) {
            String cmd = String.format("pg_bulkload --host %s --dbname %s --username %s --password %s --writer=PARALLEL --input %s --output %s --logfile %s --parse-badfile=%s --option \"TYPE=CSV\" --option \"DELIMITER=%s\"",
                host, db, this.user, this.password, file, table, logFile, badFile, Public.getColumnDelimiterRDB());
            Public.exeCmd(cmd);
        }
    }


    protected void OracleExport(String query, String fileName) throws Exception {
        jdbcToFile(query, fileName);
    }

    //   mysql 导出 select * from f_pc_user_cndt_20101128 into outfile 'd:/f.txt' Fields Terminated By ',' Lines Terminated By '\n' -- 只适用于服务器端
    //        mysql -A service_db -h your_host -utest -ptest -ss --default-character-set=utf8mb4 -e "SELECT * from t_apps limit 300;" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > apps.csv -- 性能待测试
    protected void MysqlExport(String query, String fileName) throws Exception {
        jdbcToFile(query, fileName);
    }

    /**
     * 导出查询成文件
     *
     * @param query    查询语句
     * @param fileName 文件名
     * @param table    表名
     * @throws Exception
     */
    //    sqlcmd -S"127.0.0.1"  -U"sa" -P"sa" -d"run" -Q"SELECT * FROM [kbss].[d].[list]" -o d:\aaa.txt # 适用于windows，不适合linux
    protected void SQLServerExport(String query, String fileName, String table) throws Exception {
        jdbcToFile(query, fileName);

        // TODO：测试没有通过
//        String db = new Public.JdbcUrlSplitter(this.jdbcUrl).database;
//        String host = new Public.JdbcUrlSplitter(this.jdbcUrl).host;
//        String ctlFile = Public.getConfDirectory() + table + ".fmt";
//        String cmd = String.format(
//            "bcp \"%s\" queryout %s -d%s -S%s -U%s -P%s -f%s -c",
//            query, fileName, db, host, this.user, this.password, ctlFile
//        );
//        Public.exeCmd(cmd);
    }

    //    psql --dbname=my_db_name --host=db_host_ip --username=my_username -c "COPY (select id as COL_ID, name as COL_NAME from my_tab order by id) TO STDOUT with csv header" > D:/client_exp_dir/file_name.csv
    protected void PostgreSQLExport(String query, String fileName) throws Exception {
        jdbcToFile(query, fileName);
    }

    // TODO: export
    protected void DB2Export(String query, String fileName) throws Exception {
//        jdbcToFile(query, fileName);
        String sql = String.format(
            "export to %s of del modified by coldel%s %s",
            fileName, Public.getColumnDelimiterRDB(), query);
        exeSQL(sql);
    }

    private boolean deleteDirectoryData(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        Files.deleteIfExists(path); // 删除数据
        Files.createDirectories(path.getParent());
        Files.createFile(path);
        return true;
    }

    /**
     * jdbc方式从数据库读取数据，然后写成文件
     *
     * @param query    查询SQL
     * @param fileName 文件名
     * @throws IOException
     * @throws SQLException
     */
    private void jdbcToFile(String query, String fileName) throws IOException, SQLException {
        BufferedWriter writer = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            this.logger.info(Public.getMinusSep());
            this.logger.info(query);
            if (!deleteDirectoryData(fileName)) {
                return;
            }
            Path path = Paths.get(fileName);
            writer = Files.newBufferedWriter(path);
            stmt = getConnection().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Math.toIntExact(Public.getParameters().getTable("base").getLong("jdbc_batch_size", (long) 1)));

            rs = stmt.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int cnt = meta.getColumnCount();
            String[] cols = new String[cnt];
            while (rs.next()) {
                for (int i = 0; i < cnt; i++) { // 下标从1开始
                    cols[i] = rs.getString(i + 1);
                }
                writer.write(StringUtils.join(cols, Public.getColumnDelimiter()) + "\n");
            }
        } finally {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
            close(rs);
            close(stmt);
        }
    }

    /**
     * spark jdbc接口方式获取SQL执行结果
     *
     * @param spark SparkSession
     * @param sql   SQL语句
     * @return Dataset
     */
    protected void read(@NotNull SparkSession spark, String sql) {
        LocalDateTime start = LocalDateTime.now();
        logger.info(Public.getMinusSep());
        logger.info(sql);

        String table = "";
        Dataset<Row> df = spark.read().format("jdbc").option("driver", this.driverClass).option("url", this.url)
            .option("user", this.user).option("password", this.password).option("dbtable", table) // spark 支持pushDownPredicate，可以在连接指定表名，然后用spark sql操作
            .option("fetchsize", Public.getJdbcFetchSize()).load();
        df.createOrReplaceTempView(table);
        spark.sql(sql);
        Public.printDuration(start, LocalDateTime.now());
    }

    /**
     * spark jdbc接口方式将df结果保存到rdb表中
     * 在psotgresql中，原表与目标表的字段类型需要一致。已返现hive字段tinyint，postgresql smallint报 Unsupported type in postgresql: ByteType。
     *
     * @param df    Dataset
     * @param table 表名
     */
    protected void write(@NotNull Dataset<Row> df, String table) {
        LocalDateTime start = LocalDateTime.now();
        df.write().mode(SaveMode.Append).format("jdbc").option("driver", this.driverClass).option("url", this.url)
            .option("user", this.user).option("password", this.password).option("dbtable", table)
            .option("batchsize", Public.getJdbcBatchSize()).save();
        Public.printDuration(start, LocalDateTime.now());
    }

}
