package cn.zwr.nodes.source.common;

import cn.zwr.core.node.BoundSource;
import cn.zwr.nodes.common.ESConfigure;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

/**
 * <title>ESSource</title>
 * <p>处理ES的通用父类，子类需要继承实现得到ES数据的后续处理</p>
 *
 * @param <T>
 *
 * @author Alex Han
 * @version 1.1
 */
public abstract class ESSource<T> extends BoundSource<T> implements ESConfigure {
    public final String QUERY = "query";


    public ESSource(SparkConf conf) {
        super(conf);
    }

    public JavaPairRDD<String, Map<String, Object>> before(){
        conf.set(CLUSTER_NAME , getOption(CLUSTER_NAME));
        conf.set(ES_NODES, getOption(ES_NODES));
        conf.set(ES_PORT, getOption(ES_PORT, "9200"));
        conf.set(ES_INDEX_READ_MISSING_AS_EMPTY, getOption(ES_INDEX_READ_MISSING_AS_EMPTY));
        conf.set(ES_NODES_WAN_ONLY, getOption(ES_NODES_WAN_ONLY, "true"));
        conf.set(ES_SCROLL_SIZE, getOption(ES_SCROLL_SIZE, "10000"));


        JavaPairRDD<String, Map<String, Object>> esRdd = JavaEsSpark.esRDD(new JavaSparkContext(conf),
                getOption(ES_RESOURCE) + "/" + getOption(ES_TYPE,"default") ,
                getOption(QUERY));
        return esRdd;
    }
}
