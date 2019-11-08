package cn.zwr.nodes.source.common;

import cn.zwr.core.node.BoundSource;
import lombok.Getter;
import lombok.Setter;
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
 * @version 1.0
 */
public abstract class ESSource<T> extends BoundSource<T> {
    public final String CLUSTER_NAME = "cluster.name";
    public final String ES_NODES = "es.nodes";
    public final String ES_PORT = "es.port";
    public final String ES_INDEX_READ_MISSING_AS_EMPTY = "es.index.read.missing.as.empty";
    public final String ES_NODES_WAN_ONLY = "es.nodes.wan.only";
    public final String ES_SCROLL_SIZE = "es.scroll.size";
    public final String ES_RESOURCE = "es.resource.read";
    public final String ES_TYPE = "es.type";
    public final String QUERY = "query";

    @Getter
    @Setter
    private transient SparkConf conf;

    public ESSource(SparkConf conf) {
        super();
        this.conf = conf;
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
