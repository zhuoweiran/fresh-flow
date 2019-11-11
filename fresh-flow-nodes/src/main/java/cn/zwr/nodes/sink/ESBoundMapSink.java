package cn.zwr.nodes.sink;

import cn.zwr.core.node.BoundSink;
import com.google.common.collect.Maps;
import org.apache.spark.api.java.JavaRDD;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

/**
 * Create with IDEA.
 * Date: 2019-11-08 10:39
 */
public class ESBoundMapSink extends BoundSink<Map> {
    public final String CLUSTER_NAME = "cluster.name";
    public final String ES_NODES = "es.nodes";
    public final String ES_PORT = "es.port";
    public final String ES_RESOURCE = "es.index.name";


    @Override
    public void wirte(JavaRDD<Map> rdd) {
        Map<String,String> cfg = Maps.newConcurrentMap();
        cfg.put(CLUSTER_NAME, getOption(CLUSTER_NAME)) ;
        cfg.put(ES_NODES, getOption(ES_NODES)) ;
        cfg.put(ES_PORT, getOption(ES_PORT)) ;
//        cfg.put(ES_NODES_WAN_ONLY, getOption(ES_NODES_WAN_ONLY,"true")) ;
//        cfg.put(ES_TYPE, getOption(ES_TYPE,"default")) ;

        JavaEsSpark.saveToEs(rdd, getOption(ES_RESOURCE), cfg);
    }
}
