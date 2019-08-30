package etl.pub;

import com.moandjiezana.toml.Toml;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;


public class DB {
    private final Logger logger = LogManager.getLogger();
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
    private static void close(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭编译的 SQL 语句的对象
     *
     * @param stmt
     */
    private static void close(Statement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭结果集
     *
     * @param rs
     */
    private static void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 提交事务
     *
     * @param conn
     */
    public static void commit(Connection conn) {
        try {
            if (conn != null) {
                conn.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 回滚事务
     *
     * @param conn
     */
    public static void rollback(Connection conn) {
        try {
            if (conn != null) {
                conn.rollback();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @NotNull
    private static String parseDBTable(String s) {
        s = s.trim();
        if (s.substring(0, 7).equals("select ")) {
            s = "(" + s + ") t";
        }
        return s;
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
            e.printStackTrace();

        }

        // 2.创建Connection(数据库连接对象)
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(this.jdbcUrl, this.user, this.password);
            conn.setAutoCommit(false);
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
            this.logger.error(e);
        }
        /*
         * Connection是Statement的工厂，一个Connection可以生产多个Statement。
         * Statement是ResultSet的工厂，一个Statement却只能对应一个ResultSet（它们是一一对应的关系）。
         * 所以在一段程序里要用多个ResultSet的时候，必须再Connection中获得多个Statement，
         * 然后一个Statement对应一个ResultSet。
         */
        return null;
    }

    private DataFrameReader jdbcReader(@NotNull SparkSession session, String sql) {
        return session.read().format("jdbc").option("url", this.jdbcUrl).option("dbtable", sql)
            .option("user", this.user).option("password", this.password);
    }

    void dfTable(@NotNull Dataset<Row> df, String table) {
        LocalDateTime start = LocalDateTime.now();
        df.write().mode("append").format("jdbc").option("url", this.jdbcUrl).option("dbtable", table)
            .option("user", this.user).option("password", this.password).save();
        Func.printDuration(start, LocalDateTime.now());
    }

    Dataset<Row> sqlDF(SparkSession session, String sql) {
        LocalDateTime start = LocalDateTime.now();
        sql = parseDBTable(sql);
        logger.info(sql);
        Dataset<Row> df = jdbcReader(session, sql).load();
        Func.printDuration(start, LocalDateTime.now());
        return df;
    }


    void sqlView(SparkSession session, String sql) {
        Dataset<Row> df = sqlDF(session, sql);
        df.createOrReplaceTempView("v_tmp");
    }

    void sqlSpecialView(SparkSession session, String table, String sql) {
        Dataset<Row> df = sqlDF(session, sql);
        df.createOrReplaceTempView("v_" + table);
    }

    void exeSql(String sql) {
        PreparedStatement stmt = null;
        LocalDateTime start = LocalDateTime.now();
        try {
            stmt = getConnection().prepareStatement(sql);
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            this.logger.error(e);
        } finally {
            close(stmt);
        }
        Func.printDuration(start, LocalDateTime.now());
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
            this.logger.error(e);
        } finally {
            close(rs);
            close(stmt);
        }
        return s;
    }

    void load(String table, String timeType) {
        String dir = Func.getDataDir() + table + '/' + timeType + '/';
        // List<Path> files = Files.list(Paths.get(dir)).filter(Files::isRegularFile);
        try {
            Files.newDirectoryStream(Paths.get("."), path -> path.toFile().isFile() && !path.toFile().isHidden())
                .forEach(f -> {
                    String sql = String.format(
                        "load data local infile '%s' replace into table %s Fields Terminated By '\\x01' Lines Terminated By '\\n' ",
                        dir + f, table); // TODO:
                    exeSql(sql);
                });
        } catch (IOException e) {
            logger.error(e);
        }
    }
}
