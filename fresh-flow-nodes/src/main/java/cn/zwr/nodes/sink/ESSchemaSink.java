package cn.zwr.nodes.sink;

import cn.zwr.core.node.BoundSink;
import cn.zwr.core.pojo.Mapable;
import cn.zwr.nodes.common.ESConfigureAble;
import com.google.common.collect.Maps;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

public class ESSchemaSink<T extends Mapable> extends BoundSink<T> implements ESConfigureAble {

    @Override
    public void wirte(JavaRDD<T> rdd) {
        Map<String,String> cfg = Maps.newConcurrentMap();
        cfg.put(ES_RESOURCE, getOption(ES_RESOURCE) + "/" + getOption(ES_TYPE,"default"));
//        cfg.put(ES_MAPPING_DATE_RICH, getOption(ES_MAPPING_DATE_RICH)) ;
        cfg.put(ES_NODES, getOption(ES_NODES)) ;
        cfg.put(ES_PORT, getOption(ES_PORT, "9200")) ;
        cfg.put(ES_INDEX_AUTO_CREATE, getOption(ES_INDEX_AUTO_CREATE,"true")) ;
        cfg.put(ES_NODES_WAN_ONLY, getOption(ES_NODES_WAN_ONLY, "true")) ;
        cfg.put(ES_BATCH_WRITE_RETRY_COUNT, getOption(ES_BATCH_WRITE_RETRY_COUNT,"10")) ;
        cfg.put(ES_BATCH_WRITE_RETRY_WAIT,getOption(ES_BATCH_WRITE_RETRY_WAIT, "10")) ;

        JavaRDD<Map<String, Object>> mapRdd = rdd.map(new Function<T, Map<String, Object>>() {
            @Override
            public Map<String, Object> call(T t) throws Exception {
                return t.transformMap();
            }
        });
        JavaEsSpark.saveToEs(mapRdd, cfg);
    }
}
