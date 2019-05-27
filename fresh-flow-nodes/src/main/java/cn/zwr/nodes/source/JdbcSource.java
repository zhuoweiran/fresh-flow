package cn.zwr.nodes.source;

import cn.zwr.core.node.BoundDSSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;


public class JdbcSource extends BoundDSSource<Row> {
    public final String JDBC_URL = "jdbc.url";
    public final String JDBC_DRIVER = "jdbc.driver";
    public final String JDBC_USER = "jdbc.user";
    public final String JDBC_PASSWORD = "jdbc.password";
    public final String JDBC_TABLE = "jdbc.table";


    @Override
    public Dataset<Row> read() {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", getOption(JDBC_USER));
        connectionProperties.put("password", getOption(JDBC_PASSWORD));
        Dataset<Row> jdbcDataSet = sparkSession.read()
                .jdbc(getOption(JDBC_URL), getOption(JDBC_TABLE), connectionProperties);
        return null;
    }

    public JdbcSource(SparkSession sparkSession) {
        super(sparkSession);
    }
}
