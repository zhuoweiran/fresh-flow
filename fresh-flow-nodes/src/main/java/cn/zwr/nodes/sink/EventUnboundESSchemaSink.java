package cn.zwr.nodes.sink;

import cn.zwr.nodes.sink.common.UnboundESSchemaSink;
import cn.zwr.pojo.common.EventCommon;
import cn.zwr.pojo.common.EventCommonDataDetail;
import com.google.common.collect.Maps;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

import java.util.Map;

/**
 * <title>EventUnboundESSchemaSink</title>
 * <p>EventCommon存入ES Sink</p>
 *
 * @param <T>
 * @param <D>
 *
 * @author
 * @version 1.0
 */
@NoArgsConstructor
public class EventUnboundESSchemaSink<T extends EventCommon, D extends EventCommonDataDetail> extends UnboundESSchemaSink<T> {

    @Override
    public void write(JavaDStream<T> data) {
        Map<String,String> cfg = Maps.newConcurrentMap();
        cfg.put(ES_RESOURCE, getOption(ES_RESOURCE) + "/" + getOption(ES_TYPE,"default"));
        cfg.put(ES_MAPPING_DATE_RICH, getOption(ES_MAPPING_DATE_RICH)) ;
        cfg.put(ES_NODES, getOption(ES_NODES)) ;
        cfg.put(ES_PORT, getOption(ES_PORT, "9200")) ;
        cfg.put(ES_INDEX_AUTO_CREATE, getOption(ES_INDEX_AUTO_CREATE,"true")) ;
        cfg.put(ES_NODES_WAN_ONLY, getOption(ES_NODES_WAN_ONLY, "true")) ;
        cfg.put(ES_BATCH_WRITE_RETRY_COUNT, getOption(ES_BATCH_WRITE_RETRY_COUNT,"10")) ;
        cfg.put(ES_BATCH_WRITE_RETRY_WAIT,getOption(ES_BATCH_WRITE_RETRY_WAIT, "10")) ;

        JavaDStream stream = data.map(new Function<T, Map<String, String>>() {

            @Override
            public Map<String, String> call(T t) throws Exception {
                return t.transformMap();
            }
        });
        JavaEsSparkStreaming.saveToEs(stream, cfg);

    }
}
