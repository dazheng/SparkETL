package etl.utils;

import com.moandjiezana.toml.Toml;

public class DBFactory {
    public DB produce(String type, Toml db) throws Exception {
        switch (type) {
            case Public.DB_Rdb:
                return new Rdb(db);
            case Public.DB_MONGODB:
                return new MongoDB(db);
            case Public.DB_ELASTICSEARCH:
                return new Elasticsearch(db);
            case Public.DB_REDIS:
                return new Redis(db);
            default:
                return null;
        }
    }
}
