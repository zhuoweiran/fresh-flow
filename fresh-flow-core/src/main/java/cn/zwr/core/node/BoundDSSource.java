package cn.zwr.core.node;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * <title>BoundDSSource</title>
 * <p>有边界的Source父类,所有返回Dataset Source都要重写read方法</p>
 *
 * @param <T>
 *
 * @author Alex Han
 * @version 1.1
 */
@AllArgsConstructor
public abstract class BoundDSSource<T> extends BaseNode{
    @Getter
    @Setter
    protected transient SparkSession sparkSession;
    public abstract Dataset<T> read();
}
