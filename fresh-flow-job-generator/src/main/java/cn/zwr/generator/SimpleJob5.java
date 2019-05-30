package cn.zwr.generator;


import cn.zwr.core.node.NodeOptions;
import cn.zwr.nodes.sink.BoundTextFileSink;
import cn.zwr.nodes.source.ESJsonSource;
import org.apache.spark.SparkConf;

import java.io.FileInputStream;
import java.util.Properties;

public class SimpleJob5 {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String path = "conf/SimpleJob5.properties";
        if(args.length > 0){
            path = args[0];
        }

        FileInputStream fileInputStream = new FileInputStream(path);
        properties.load(fileInputStream);

        SparkConf conf = new SparkConf();
        conf.setAppName(SimpleJob4.class.getName()).setMaster("local[1]").set("appName", SimpleJob4.class.getName());

        System.out.println(conf.get("appName"));

        ESJsonSource esJsonSource = new ESJsonSource(conf);
        NodeOptions options2 = new NodeOptions();
        options2.setOption(esJsonSource.ES_NODES, properties.getProperty(esJsonSource.ES_NODES));
        options2.setOption(esJsonSource.CLUSTER_NAME, properties.getProperty(esJsonSource.CLUSTER_NAME));
        options2.setOption(esJsonSource.ES_RESOURCE, properties.getProperty("es.index.name"));
        options2.setOption(esJsonSource.ES_TYPE, properties.getProperty("es.index.type","defalut"));
        options2.setOption(esJsonSource.QUERY, properties.getProperty("es.query","{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}}}"));
        options2.setOption(esJsonSource.ES_PORT, properties.getProperty("es.port","9200"));
        options2.setOption(esJsonSource.ES_INDEX_READ_MISSING_AS_EMPTY, "true");
        options2.setOption(esJsonSource.ES_NODES_WAN_ONLY, "true");
        options2.setOption(esJsonSource.ES_SCROLL_SIZE, "10000");

        esJsonSource.setNodeOptions(options2);

        BoundTextFileSink boundTextFileSink = new BoundTextFileSink();
        NodeOptions options = new NodeOptions();
        options.setOption(boundTextFileSink.LOCAL_PATH, properties.getProperty("file.local.path"));
        options.setOption(boundTextFileSink.FILE_NAME, properties.getProperty("file.num"));
        options.setOption(boundTextFileSink.FILE_NAME, properties.getProperty("file.floder"));

        boundTextFileSink.setNodeOptions(options);

        boundTextFileSink.wirte(esJsonSource.read());

    }
}
