package cn.zwr.generator;

import cn.zwr.core.node.NodeOptions;
import cn.zwr.nodes.sink.ESBoundMapSink;
import cn.zwr.nodes.source.CSVMapSource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Create with IDEA.
 * Date: 2019-11-08 10:07
 */
public class Importer {
    private static final Logger logger = LoggerFactory.getLogger(Importer.class);

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String path = "src/main/resources/importer.properties";
        if(args.length > 0){
            path = args[0];
        }

        FileInputStream fileInputStream = new FileInputStream(path);
        properties.load(fileInputStream);

        SparkConf conf = new SparkConf();
        conf.setAppName(Exporter.class.getName()).setMaster("local[1]").set("appName", Exporter.class.getName());

        logger.info(conf.get("appName"));

        CSVMapSource csvMapSource = new CSVMapSource(conf);
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setOption(csvMapSource.FIELDS, properties.getProperty(csvMapSource.FIELDS));
        nodeOptions.setOption(csvMapSource.PARTITION_NUM, properties.getProperty(csvMapSource.PARTITION_NUM));
        nodeOptions.setOption(csvMapSource.FILE_PATH, properties.getProperty(csvMapSource.FILE_PATH));
        nodeOptions.setOption(csvMapSource.SPLITER, properties.getProperty(csvMapSource.SPLITER));
        nodeOptions.setOption(csvMapSource.DELETE_EXTERIOR, properties.getProperty(csvMapSource.DELETE_EXTERIOR));

        csvMapSource.setNodeOptions(nodeOptions);
        JavaRDD<Map> sourceRdd = csvMapSource.read();


        ESBoundMapSink esBoundMapSink = new ESBoundMapSink();
        NodeOptions nodeOptions1 = new NodeOptions();
        nodeOptions1.setOption(esBoundMapSink.CLUSTER_NAME, properties.getProperty(esBoundMapSink.CLUSTER_NAME));
        nodeOptions1.setOption(esBoundMapSink.ES_NODES, properties.getProperty(esBoundMapSink.ES_NODES));
        nodeOptions1.setOption(esBoundMapSink.ES_PORT, properties.getProperty(esBoundMapSink.ES_PORT));
        nodeOptions1.setOption(esBoundMapSink.ES_RESOURCE, properties.getProperty(esBoundMapSink.ES_RESOURCE));

        esBoundMapSink.setNodeOptions(nodeOptions1);
        esBoundMapSink.wirte(sourceRdd);
    }
}
