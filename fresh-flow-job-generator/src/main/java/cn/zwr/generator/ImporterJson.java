package cn.zwr.generator;

import cn.zwr.core.node.NodeOptions;
import cn.zwr.nodes.process.StringToJsonParser;
import cn.zwr.nodes.sink.BoundFileSink;
import cn.zwr.nodes.sink.ESBoundMapSink;
import cn.zwr.nodes.sink.ESJsonSink;
import cn.zwr.nodes.source.CSVMapSource;
import cn.zwr.nodes.source.FileSource;
import com.alibaba.fastjson.JSONObject;
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
 * 导入json文件到ES
 */
public class ImporterJson {
    private static final Logger logger = LoggerFactory.getLogger(ImporterJson.class);

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String path = "src/main/resources/importerJson.properties";
        if(args.length > 0){
            path = args[0];
        }

        FileInputStream fileInputStream = new FileInputStream(path);
        properties.load(fileInputStream);

        SparkConf conf = new SparkConf();
        conf.setAppName(Exporter.class.getName()).setMaster("local[4]").set("appName", Exporter.class.getName());

        logger.info(conf.get("appName"));

        FileSource source = new FileSource(conf);
        NodeOptions options1 = new NodeOptions();
        options1.setOption(source.FILE_PATH, properties.getProperty(source.FILE_PATH));
        source.setNodeOptions(options1);

        StringToJsonParser parser = new StringToJsonParser();

        ESJsonSink sink = new ESJsonSink();
        NodeOptions sinkOptions = new NodeOptions();
        sinkOptions.setOption(sink.CLUSTER_NAME, properties.getProperty(sink.CLUSTER_NAME));
        sinkOptions.setOption(sink.ES_NODES, properties.getProperty(sink.ES_NODES));
        sinkOptions.setOption(sink.ES_PORT, properties.getProperty(sink.ES_PORT));
        sinkOptions.setOption(sink.ES_RESOURCE, properties.getProperty(sink.ES_RESOURCE));
        sink.setNodeOptions(sinkOptions);

        JavaRDD<String> sourceRdd = source.read();
        JavaRDD<JSONObject> processRdd = parser.process(sourceRdd);
        sink.wirte(processRdd);

    }
}
