package cn.zwr.generator;

import cn.zwr.core.node.NodeOptions;
import cn.zwr.nodes.sink.CsvLocalFileSink;
import cn.zwr.nodes.source.ESSchemaSource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

public class SimpleJob2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName(SimpleJob2.class.getName()).setMaster("local[1]").set("appName", SimpleJob2.class.getName());

        System.out.println(conf.get("appName"));

//        JavaSparkContext sc = new JavaSparkContext(conf);

        ESSchemaSource<DeviceWarn> ESSchemaSource = new ESSchemaSource<>(conf);
        NodeOptions options2 = new NodeOptions();
        options2.setOption(ESSchemaSource.ES_NODES, "zdbd01,zdbd02,zdbd03");
        options2.setOption(ESSchemaSource.CLUSTER_NAME, "es_nfdw");
        options2.setOption(ESSchemaSource.ES_RESOURCE, "devicewarn");
        options2.setOption(ESSchemaSource.ES_TYPE, "default");
        options2.setOption(ESSchemaSource.QUERY, "{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}}}");
        options2.setOption(ESSchemaSource.ES_PORT, "9200");
        options2.setOption(ESSchemaSource.ES_INDEX_READ_MISSING_AS_EMPTY, "true");
        options2.setOption(ESSchemaSource.ES_NODES_WAN_ONLY, "true");
        options2.setOption(ESSchemaSource.ES_SCROLL_SIZE, "10000");

        DeviceWarn deviceWarn = new DeviceWarn();

        ESSchemaSource.setNodeOptions(options2);
        ESSchemaSource.setT(deviceWarn);

        JavaRDD<DeviceWarn> helloJavaRDD = ESSchemaSource.read();

        CsvLocalFileSink<DeviceWarn> helloCsvLocalFileSink = new CsvLocalFileSink<>();
        NodeOptions options = new NodeOptions();
        options.setOption(helloCsvLocalFileSink.FILE_SPLIT_BY,",");
        options.setOption(helloCsvLocalFileSink.FILE_NUM, "1");
        options.setOption(helloCsvLocalFileSink.LOCAL_PATH, "/Users/hanwenlong/Downloads/");
        options.setOption(helloCsvLocalFileSink.FILE_NAME, "test.csv");

        helloCsvLocalFileSink.setNodeOptions(options);
        System.out.println(helloCsvLocalFileSink.getNodeOptions());
        helloCsvLocalFileSink.wirte(helloJavaRDD);

    }
}
