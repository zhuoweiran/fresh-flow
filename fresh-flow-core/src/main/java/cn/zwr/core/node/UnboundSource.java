package cn.zwr.core.node;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


/**
 * <title>UnboundSource</title>
 * <p>没有边界的Source父类,所有Source都要重写read方法</p>
 *
 * @param <T>
 *
 * @author Alex Han
 * @version 1.1
 */
@AllArgsConstructor
public abstract class UnboundSource<T> extends BaseNode{
    private static final long serialVersionUID = 8702747072022686313L;

    @Getter
    @Setter
    protected transient JavaStreamingContext javaStreamingContext;

    /**
     * 从Unbound数据中读取处DStream
     */
    public abstract JavaDStream<T> read();
}
