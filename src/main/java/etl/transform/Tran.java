package etl.transform;

import etl.utils.Transform;
import org.apache.spark.sql.SparkSession;

/**
 * 数据转换业务逻辑
 */
public class Tran extends Transform {
    public Tran(SparkSession spark, Integer timeType, String timeID, Integer backDate, Integer frequency) {
        super(spark, timeType, timeID, backDate, frequency);
    }

    /**
     * stgging层integrate层
     *
     * @throws Exception
     */
    public void s2iD() throws Exception {
        exeSQLFile("s2i_d.sql", "insert");  // exeType: insert 插入hive表中; view 生成v_${表名}的临时试图，供后续使用
    }

    /**
     * integerate层到datamart层
     *
     * @throws Exception
     */
    public void i2mD() throws Exception {
        exeSQLFile("i2m_d.sql", "insert");
    }
}

