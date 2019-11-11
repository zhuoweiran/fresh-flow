package cn.zwr.nodes.sink;

import cn.zwr.core.node.BoundSink;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.EsSpark;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Create with IDEA.
 * Date: 2019-11-08 16:32
 */
public class ESJsonSink extends BoundSink<JSONObject> {
    public final String CLUSTER_NAME = "cluster.name";
    public final String ES_NODES = "es.nodes";
    public final String ES_PORT = "es.port";
    public final String ES_RESOURCE = "es.index.name";
    public final String ES_AUTO_CREATE = "es.index.auto.create";



    @Override
    public void wirte(JavaRDD<JSONObject> rdd) {
        Map<String,String> cfg = Maps.newConcurrentMap();
        cfg.put(ES_AUTO_CREATE, "true");
        cfg.put(CLUSTER_NAME, getOption(CLUSTER_NAME)) ;
        cfg.put(ES_NODES, getOption(ES_NODES)) ;
        cfg.put("es.resource.write", getOption(ES_RESOURCE));
        cfg.put(ES_PORT, getOption(ES_PORT)) ;

        JavaRDD mapRdd = rdd.map(new Function<JSONObject, Map<String,Object>>() {
            @Override
            public Map<String, Object> call(JSONObject json) throws Exception {
                return json.getInnerMap();
            }
        });

        JavaEsSpark.saveToEs(mapRdd, getOption(ES_RESOURCE), cfg);
    }
}
