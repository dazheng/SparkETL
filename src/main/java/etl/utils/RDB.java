package etl.utils;

import com.moandjiezana.toml.Toml;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;


public class RDB {
    private final Logger logger = LoggerFactory.getLogger(RDB.class);
    private final String id;
    private final String driverClass;
    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String dbType;
    private Connection conn;

    RDB(String ID) {
        this.id = ID;
        Toml db = getRDB();
        assert db != null;
        this.driverClass = db.getString("driver_class");
        this.jdbcUrl = db.getString("url");
        this.user = db.getString("user");
        this.password = db.getString("password");
        this.dbType = db.getString("db_type");
        this.conn = connection();
    }

    protected String getDbType() {
        return this.dbType;
    }

    void release() {
        close(this.conn);
    }

    private Toml getRDB() {
        Toml toml = Public.getParameters();
        List<Toml> dbs = toml.getTables("db");
        for (Toml db : dbs) {
            if (db.getString("id").equals(this.id)) {
                return db;
            }
        }
        return null;
    }

    private void close(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            this.logger.error(e.toString(), e);
        }
    }

    private void close(Statement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            this.logger.error(e.toString(), e);
        }
    }


    private void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            this.logger.error(e.toString(), e);
        }
    }


    @NotNull
    private Connection getConnection() {
        return this.conn;
    }

    @NotNull
    private Connection connection() {
        // 1.注册驱动
        try {
            Class.forName(this.driverClass);
        } catch (ClassNotFoundException e) {
            this.logger.error(e.toString(), e);
        }

        // 2.创建Connection(数据库连接对象)
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(this.jdbcUrl, this.user, this.password);
            conn.setAutoCommit(true);
            return conn;
        } catch (SQLException e) {
            this.logger.error(e.toString(), e);
        }
        return null;
    }

    void exeSQL(String sql) {
        PreparedStatement stmt = null;
        LocalDateTime start = LocalDateTime.now();
        try {
            System.out.println(sql);
            this.logger.info(Public.getMinusSep());
            this.logger.info(sql);
            stmt = getConnection().prepareStatement(sql);
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            this.logger.error(e.toString());
        } finally {
            close(stmt);
            Public.printDuration(start, LocalDateTime.now());
        }
    }

    String getTableColumns(String table) {
        StringBuilder cs = new StringBuilder();
        String sql = "";
        switch (this.dbType) {
            case "mysql":
                sql = "select column_name from information_schema.columns where table_schema = ? and table_name = ? order by ordinal_position";
                break;
            case "oracle":
                sql = "select column_name from user_tab_columns where schema_name = ? and table_name = ? order by column_id;";
                break;
            default:
                logger.error("not support {}", this.dbType);
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String s = "";
        try {
            String[] urls = this.jdbcUrl.split("/");
            String schema = urls[urls.length - 1];
            stmt = getConnection().prepareStatement(sql);
            stmt.setString(1, schema);
            stmt.setString(2, table);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                cs.append(rs.getString("column_name"));
                cs.append(",");
            }

            s = cs.toString();
            if (s.length() > 0) {
                s = s.substring(0, s.length() - 1);
            }
        } catch (SQLException e) {
            this.logger.error(e.toString(), e);
        } finally {
            close(rs);
            close(stmt);
        }
        return s;
    }

    protected List<String> getLoadFiles(String table, String timeType) {
        String dir = Public.getDataDirectory() + table + "/" + timeType + "/";
        List<String> files = null;
        try {
            Files.newDirectoryStream(Paths.get(dir), path -> path.toFile().isFile() && !path.toFile().isHidden() && path.toFile().length() > 0)
                .forEach(f -> {
                    files.add(f.toString());
                });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return files;
    }

    protected void MySQLLoad(String table, String timeType) {
        List<String> files = getLoadFiles(table, timeType);
        if (files.isEmpty()) {
            return;
        }
        for (String file : files) {
            String sql = String.format(
                "load data local infile '%s' replace into table %s Fields Terminated By '%s' Lines Terminated By '\\n' ",
                file, table, Public.getColumnDelimiter());
            exeSQL(sql);
        }
    }

    protected void OracleLoad(String table, String timeType) { // .ctl文件需要提前定义好
        List<String> files = getLoadFiles(table, timeType);
        if (files.isEmpty()) {
            return;
        }
        try {
            for (String file : files) {
                String ctlFile = Public.getConfDirectory() + table + ".ctl";
                String logDir = Public.getLogDirectory() + table + "/" + timeType + "/";
                Files.createDirectories(Paths.get(logDir));
                String logFile = logDir + table + ".log";
                String badFile = logDir + table + ".bad";
                String discardFile = Public.getLogDirectory() + table + ".dcd";
                String sql = String.format(
                    "sqlldr %s/%s@%s control=%s data=%s log=%s bad=%s discard=%s direct=true parallel=true", this.user,
                    this.password, this.jdbcUrl, ctlFile, file, logFile, badFile, discardFile
                );
                Public.exeCmd(sql);
            }
        } catch (IOException e) {
            logger.error(e.toString(), e);
        }
    }
//sqlserver 导入 BULK INSERT
//[表名]
//    FROM [csv文件地址]
//    WITH(FIELDTERMINATOR＝',',ROWTERMINATOR＝'\n');
//
//    sqlcmd -S"127.0.0.1"  -U"sa" -P"sa" -d"run" -Q"SELECT * FROM [kbss].[d].[list]" -o d:\aaa.txt # 适用于windows，不适合linux

//    postgresql export
//    psql --dbname=my_db_name --host=db_host_ip --username=my_username -c "COPY (select id as COL_ID, name as COL_NAME from my_tab order by id) TO STDOUT with csv header" > D:/client_exp_dir/file_name.csv

//   mysql 导出 select * from f_pc_user_cndt_20101128 into outfile 'd:/f.txt' Fields Terminated By ',' Lines Terminated By '\n'


    private String jdbcToFile(String table, String timeType) {
        String fileName = null;
        Path path = Paths.get(fileName);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write("d");
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
        return fileName;
    }

    private Dataset<Row> jdbcRead(@NotNull SparkSession spark, String sql) {
        return spark.read().format("jdbc").option("url", this.jdbcUrl).option("query", sql)
            .option("user", this.user).option("password", this.password).load();
    }

    void jdbcWrite(@NotNull Dataset<Row> df, String table) {
        LocalDateTime start = LocalDateTime.now();
        df.write().mode("append").format("jdbc").option("url", this.jdbcUrl).option("dbtable", table)
            .option("user", this.user).option("password", this.password).save();
        Public.printDuration(start, LocalDateTime.now());
    }

    Dataset<Row> sqlToDF(SparkSession spark, String sql) {
        LocalDateTime start = LocalDateTime.now();
        logger.info(Public.getMinusSep());
        logger.info(sql);
        Dataset<Row> df = jdbcRead(spark, sql);
        Public.printDuration(start, LocalDateTime.now());
        return df;
    }

    void sqlToView(SparkSession spark, String sql) {
        Dataset<Row> df = sqlToDF(spark, sql);
        df.createOrReplaceTempView("v_tmp");
    }

    void sqlToSpecialView(SparkSession spark, String table, String sql) {
        Dataset<Row> df = sqlToDF(spark, sql);
        df.createOrReplaceTempView("v_" + table);
    }
}
