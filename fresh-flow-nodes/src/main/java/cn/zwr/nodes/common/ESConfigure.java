package cn.zwr.nodes.common;

public interface ESConfigure {
    String CLUSTER_NAME = "cluster.name";
    String ES_INDEX_AUTO_CREATE = "es.index.auto.create";
    String ES_NODES = "es.nodes";
    String ES_PORT = "es.port";

    String ES_RESOURCE = "es.resource";
    String ES_TYPE = "es.type";
    String ES_MAPPING_DATE_RICH = "es.mapping.date.rich";
    String ES_NODES_WAN_ONLY = "es.nodes.wan.only";
    String ES_BATCH_WRITE_RETRY_COUNT = "es.batch.write.retry.count";
    String ES_BATCH_WRITE_RETRY_WAIT = "es.batch.write.retry.wait";

    String ES_INDEX_READ_MISSING_AS_EMPTY = "es.index.read.missing.as.empty";
    String ES_SCROLL_SIZE = "es.scroll.size";

}
