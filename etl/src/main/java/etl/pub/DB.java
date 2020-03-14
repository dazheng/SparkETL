package etl.pub;

import com.moandjiezana.toml.Toml;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;


public class DB {
    private final Logger logger = LoggerFactory.getLogger(DB.class);
    private String id;
    private String driverClass;
    private String jdbcUrl;
    private String user;
    private String password;
    private Connection conn;

    DB(String ID) {
        this.id = ID;
        Toml db = getDB();
        assert db != null;
        this.driverClass = db.getString("driver_class");
        this.jdbcUrl = db.getString("url");
        this.user = db.getString("user");
        this.password = db.getString("password");
        this.conn = connection();
    }

    /**
     * 关闭连接(数据库连接对象)
     *
     * @param conn
     */
    private void close(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            this.logger.error(e.toString(), e);
        }
    }

    /**
     * 关闭编译的 SQL 语句的对象
     *
     * @param stmt
     */
    private void close(Statement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            this.logger.error(e.toString(), e);
        }
    }

    /**
     * 关闭结果集
     *
     * @param rs
     */
    private void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            this.logger.error(e.toString(), e);
        }
    }


    void release() {
        close(this.conn);
    }

    private Toml getDB() {
        Toml toml = Func.getParameters();
        List<Toml> dbs = toml.getTables("db");
        for (Toml db : dbs) {
            if (db.getString("id").equals(this.id)) {
                return db;
            }
        }
        return null;
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
            this.logger.info(Func.getMinusSep());
            this.logger.info(sql);
            stmt = getConnection().prepareStatement(sql);
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            this.logger.error(e.toString());
        } finally {
            close(stmt);
            Func.printDuration(start, LocalDateTime.now());
        }
    }

    String getMysqlColumn(String table) {
        StringBuilder cs = new StringBuilder();
        String sql = "select column_name from information_schema.columns where table_schema = ? and table_name = ? order by ordinal_position";
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

    void load(String table, String timeType) {
        String dir = Func.getDataDir() + table + "/" + timeType + "/";
        try {
            Files.newDirectoryStream(Paths.get(dir), path -> path.toFile().isFile() && !path.toFile().isHidden() && path.toFile().length() > 0)
                .forEach(f -> {
                    String sql = String.format(
                        "load data local infile '%s' replace into table %s Fields Terminated By '%s' Lines Terminated By '\\n' ",
                        f, table, Func.getColumnDelimiter());
                    exeSQL(sql);
                });
        } catch (IOException e) {
            logger.error(e.toString(), e);
        }
    }

    private Dataset<Row> jdbcLoad(@NotNull SparkSession spark, String sql) {
        return spark.read().format("jdbc").option("url", this.jdbcUrl).option("query", sql)
            .option("user", this.user).option("password", this.password).load();
    }

    void jdbcSave(@NotNull Dataset<Row> df, String table) {
        LocalDateTime start = LocalDateTime.now();
        df.write().mode("append").format("jdbc").option("url", this.jdbcUrl).option("dbtable", table)
            .option("user", this.user).option("password", this.password).save();
        Func.printDuration(start, LocalDateTime.now());
    }

    Dataset<Row> sqlDF(SparkSession spark, String sql) {
        LocalDateTime start = LocalDateTime.now();
        logger.info(sql);
        Dataset<Row> df = jdbcLoad(spark, sql);
        Func.printDuration(start, LocalDateTime.now());
        return df;
    }

    void sqlView(SparkSession spark, String sql) {
        Dataset<Row> df = sqlDF(spark, sql);
        df.createOrReplaceTempView("v_tmp");
    }

    void sqlSpecialView(SparkSession spark, String table, String sql) {
        Dataset<Row> df = sqlDF(spark, sql);
        df.createOrReplaceTempView("v_" + table);
    }
}
