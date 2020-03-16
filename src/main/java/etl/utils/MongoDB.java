package etl.utils;

import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoDB {
    private final Logger logger = LoggerFactory.getLogger(RDB.class);
    private final String id;
    private final String driverClass;
    private final String ConnectioncUrl;
    private final String user;
    private final String password;
    private final String dbType;
    private MongoClient conn;


    MongoDB(String ID) {
        this.id = ID;
        Toml db = getRDB();
        assert db != null;
        this.driverClass = db.getString("driver_class");
        this.ConnectioncUrl = db.getString("url");
        this.user = db.getString("user");
        this.password = db.getString("password");
        this.dbType = db.getString("db_type");
        this.conn = connection();
    }

    protected String getDbType() {
        return this.dbType;
    }

    protected void release() {
        this.conn.close();
    }

    private MongoClient connection() {
        MongoClient mongoClient = null;
        try {
            mongoClient = MongoClients.create(this.ConnectioncUrl);
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
        return mongoClient;
    }

    protected MongoClient getConnection() {
        return this.conn;
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

}

