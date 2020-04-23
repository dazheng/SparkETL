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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;


/**
 * 对Rdbms的处理
 */
public class Rdb implements DB {
    private final Logger logger = LoggerFactory.getLogger(Rdb.class);
    private final String driverClass;
    private final String url;
    private final String user;
    private final String password;
    private final String dbType;
    private Connection conn;
    private Map<String, BiConsumer> dbLoads;
    private Map<String, BiConsumer> dbExports;

    Rdb(Toml db) throws Exception {
        this.driverClass = db.getString("driver_class");
        this.url = db.getString("url");
        this.user = db.getString("user");
        this.password = db.getString("password");
        this.dbType = new Public.JdbcUrlSplitter(this.url).driverName;
        this.conn = connection();
        RdbLoads();
        RdbExports();
    }


    /**
     * 释放申请的资源
     *
     * @throws SQLException
     */
    @Override
    public void release() throws SQLException {
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
    @Override
    public void exeSQL(String sql) throws SQLException {
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
    @Override
    public String getTableColumns(String table) throws Exception {
        String sql = "";
        switch (this.dbType) {
            case Public.DB_MYSQL:
                sql = "select column_name from information_schema.columns where table_schema = ? and table_name = ? order by ordinal_position";
                break;
            case Public.DB_ORACLE:
                sql = "select column_name from user_tab_columns where table_name = ? order by column_id";
                break;
            case Public.DB_SQLSERVER:
                sql = "select column_name from information_schema.columns where table_catalog = ? and table_name = ? order by ordinal_position";
                break;
            case Public.DB_POSTGRESQL:
                sql = "select column_name from information_schema.columns where table_catalog = ? and table_name = ? order by ordinal_position";
                break;
            case Public.DB_DB2:
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

    private boolean deleteDirectoryData(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        Files.deleteIfExists(path); // 删除数据
        Files.createDirectories(path.getParent());
        Files.createFile(path);
        return true;
    }

    /**
     * spark jdbc接口方式获取SQL执行结果
     *
     * @param spark SparkSession
     * @param sql   SQL语句
     * @return Dataset
     */
    @Override
    public void read(@NotNull SparkSession spark, String sql) {
        LocalDateTime start = LocalDateTime.now();
        logger.info(Public.getMinusSep());
        logger.info(sql);

        String table = Public.getTableFromSelectSQL(sql);
        if ("".equals(table)) {
            return;
        }
        Dataset<Row> df = spark.read().format("jdbc").option("driver", this.driverClass).option("url", this.url)
            .option("user", this.user).option("password", this.password).option("dbtable", table) // spark 支持pushDownPredicate，可以在连接指定表名，然后用spark sql操作
            .option("fetchsize", Public.getJdbcFetchSize()).load();
        df.createOrReplaceTempView(table);
        spark.sql(sql);
        Public.printDuration(start, LocalDateTime.now());
    }

    /**
     * spark jdbc接口方式将df结果保存到Rdb表中
     * 在psotgresql中，原表与目标表的字段类型需要一致。已返现hive字段tinyint，postgresql smallint报 Unsupported type in postgresql: ByteType。
     *
     * @param df    Dataset
     * @param table 表名
     */
    @Override
    public void write(@NotNull Dataset<Row> df, String table) {
        LocalDateTime start = LocalDateTime.now();
        df.write().mode(SaveMode.Append).format("jdbc").option("driver", this.driverClass).option("url", this.url)
            .option("user", this.user).option("password", this.password).option("dbtable", table)
            .option("batchsize", Public.getJdbcBatchSize()).save();
        Public.printDuration(start, LocalDateTime.now());
    }

    private void RdbLoads() throws Exception {
        this.dbLoads = new HashMap<>();
        this.dbLoads.put(Public.DB_MYSQL, Public.rethrowBiConsumer(this::MySQLLoad));
        this.dbLoads.put(Public.DB_ORACLE, Public.rethrowBiConsumer(this::OracleLoad));
        this.dbLoads.put(Public.DB_SQLSERVER, Public.rethrowBiConsumer(this::SQLServerLoad));
//        this.dbLoads.put(Public.DB_POSTGRESQL, Public.rethrowBiConsumer(this::PostgreSQLLoad));
        this.dbLoads.put(Public.DB_DB2, Public.rethrowBiConsumer(this::DB2Load));
    }

    @Override
    public BiConsumer getLoad() throws Exception {
        return this.dbLoads.getOrDefault(this.dbType, Public.rethrowBiConsumer(this::fileToRdbByJdbc));
    }

    private void RdbExports() throws Exception {
        this.dbExports = new HashMap<>();
        this.dbExports.put(Public.DB_DB2, Public.rethrowBiConsumer(this::DB2Export));
    }

    @Override
    public BiConsumer getExport() throws Exception {
        return this.dbExports.getOrDefault(this.dbType, Public.rethrowBiConsumer(this::jdbcToFile));
    }


    /**
     * jdbc方式从数据库读取数据，然后写成文件
     *
     * @param table 表名
     * @param files 多个文件
     * @throws IOException
     * @throws SQLException
     */
    private void fileToRdbByJdbc(String table, List<String> files) throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        BufferedReader in = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            String[] cols;
            String tableCols = getTableColumns(table);
            if ("".equals(tableCols)) {
                throw new Exception("not get " + table + " columns");
            }
            int cnt = tableCols.split(",").length;
            StringBuilder sql = new StringBuilder("insert into ");
            sql.append(table);
            sql.append("(");
            String[] paras = new String[cnt];
            for (int i = 0; i < cnt; i++) {
                paras[i] = "?";
            }
            sql.append(tableCols);
            sql.append(") values (");
            sql.append(String.join(",", paras));
            sql.append(")");
            stmt = conn.prepareStatement(sql.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            long rowCnt = 0;
            String line;
            for (String file : files) {
                in = new BufferedReader(new FileReader(file));
                while ((line = in.readLine()) != null) {
                    cols = line.split(Public.getColumnDelimiter());
                    if (cols.length != cnt) {
                        continue;
                    }
                    rowCnt++;
                    for (int i = 0; i < cnt; i++) {
                        stmt.setObject(i + 1, cols[i]);
                    }
                    stmt.addBatch();
                    if (rowCnt % Public.getJdbcBatchSize() == 0) {
                        stmt.executeBatch();
                    }
                }
                stmt.executeBatch();
            }
            conn.commit();
        } finally {
            if (in != null) {
                in.close();
            }
            if (conn != null) {
                conn.rollback();
            }
            close(stmt);
            close(conn);
        }
    }


    protected void MySQLLoad(String table, List<String> files) throws SQLException {
        for (String file : files) {
            String sql = String.format(
                "load data local infile '%s' replace into table %s Fields Terminated By %s Lines Terminated By '\\n' ",
                file, table, Public.getColumnDelimiterRdb());
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
        String ctlFile = Public.getConfDirectory() + table + ".ctl";
        String logDir = Public.getLogDirectory() + table + "/";
        String logFile = logDir + table + ".log";
        String badFile = logDir + table + ".bad";
        String discardFile = Public.getLogDirectory() + table + ".dcd";
        Files.createDirectories(Paths.get(logDir));
        for (String file : files) {
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
//                table, file, Public.getColumnDelimiterRdb());
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
                file, Public.getColumnDelimiterRdb(), table);
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
                host, db, this.user, this.password, file, table, logFile, badFile, Public.getColumnDelimiterRdb());
            Public.exeCmd(cmd);
        }
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
            stmt.setFetchSize(Math.toIntExact(Public.getJdbcFetchSize()));

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
     * 导出查询成文件
     *
     * @param query    查询语句
     * @param fileName 文件名
     * @throws Exception
     */
    //    sqlcmd -S"127.0.0.1"  -U"sa" -P"sa" -d"run" -Q"SELECT * FROM [kbss].[d].[list]" -o d:\aaa.txt # 适用于windows，不适合linux
    protected void SQLServerExport(String query, String fileName) throws Exception {
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

    // TODO: export
    protected void DB2Export(String query, String fileName) throws Exception {
//        jdbcToFile(query, fileName);
        String sql = String.format(
            "export to %s of del modified by coldel%s %s",
            fileName, Public.getColumnDelimiterRdb(), query);
        exeSQL(sql);
    }


}
