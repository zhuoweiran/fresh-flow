package cn.zwr.nodes.process.common;


import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * Create with IDEA.
 * Date: 2019-11-08 16:24
 */
public abstract class BoundProcess<In,Out> implements Serializable {
    public abstract JavaRDD<Out> process(JavaRDD<In> in);
}
