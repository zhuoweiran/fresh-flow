package cn.zwr.core.node;

import org.apache.spark.api.java.JavaRDD;

public abstract class BoundProcess<in, out> {
    public abstract JavaRDD<out> process(JavaRDD<in> inRdd);
}
