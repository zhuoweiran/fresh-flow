package cn.zwr.core.node;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

/**
 * <title>BoundSource</title>
 * <p>有边界的Source父类,所有Source都要重写read方法</p>
 *
 * @param <T>
 *
 * @author Alex Han
 * @version 1.1
 */
@AllArgsConstructor
public abstract class BoundSource<T> extends BaseNode{
    @Getter
    @Setter
    protected transient SparkConf conf;
    public abstract JavaRDD<T> read();
}
