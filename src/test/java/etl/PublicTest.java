package etl;

import etl.utils.Public;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PublicTest {
    @Test
    public void TestJdbcUrlSplitter() {
        String MySQL = "jdbc:mysql://db:3306/mysql_tester?local_infile=1&zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
        Public.JdbcUrlSplitter m = new Public.JdbcUrlSplitter(MySQL);
        assertEquals("mysql_tester", m.database);
        assertEquals("db", m.host);
        assertEquals("mysql", m.driverName);
        assertEquals("3306", m.port);


        String oracle = "jdbc:oracle:thin:@//db:1521/testdb";
        Public.JdbcUrlSplitter o = new Public.JdbcUrlSplitter(oracle);
        assertEquals("testdb", o.database);
        assertEquals("db", o.host);
        assertEquals("oracle", o.driverName);
        assertEquals("1521", o.port);

        String sqlserver = "jdbc:sqlserver://db:1433;DatabaseName=test";
        Public.JdbcUrlSplitter ss = new Public.JdbcUrlSplitter(sqlserver);
        assertEquals("test", ss.database);
        assertEquals("db", ss.host);
        assertEquals("sqlserver", ss.driverName);
        assertEquals("1433", ss.port);

        String postgresql = "jdbc:postgresql://db:5432/test";
        Public.JdbcUrlSplitter pg = new Public.JdbcUrlSplitter(postgresql);
        assertEquals("test", pg.database);
        assertEquals("db", pg.host);
        assertEquals("postgresql", pg.driverName);
        assertEquals("5432", pg.port);


        String db2 = "jdbc:db2://db:50000/test";
        Public.JdbcUrlSplitter db = new Public.JdbcUrlSplitter(db2);
        assertEquals("test", db.database);
        assertEquals("db", db.host);
        assertEquals("db2", db.driverName);
        assertEquals("50000", db.port);
    }


}
