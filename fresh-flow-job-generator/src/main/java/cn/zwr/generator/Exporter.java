package cn.zwr.generator;

import cn.zwr.core.node.NodeOptions;
import cn.zwr.nodes.sink.CsvJsonLocalFileSink;
import cn.zwr.nodes.sink.CsvLocalFileSink;
import cn.zwr.nodes.source.ESJsonSource;
import cn.zwr.nodes.source.ESSchemaSource;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import java.io.FileInputStream;
import java.util.Properties;

public class Exporter {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String path = "src/main/resources/exporter.properties";
        if(args.length > 0){
            path = args[0];
        }

        FileInputStream fileInputStream = new FileInputStream(path);
        properties.load(fileInputStream);

        SparkConf conf = new SparkConf();
        conf.setAppName(Exporter.class.getName()).setMaster("local[1]").set("appName", Exporter.class.getName());

        System.out.println(conf.get("appName"));

        ESJsonSource esJsonSource = new ESJsonSource(conf);
        NodeOptions options2 = new NodeOptions();
        options2.setOption(esJsonSource.ES_NODES, properties.getProperty(esJsonSource.ES_NODES));
        options2.setOption(esJsonSource.CLUSTER_NAME, properties.getProperty(esJsonSource.CLUSTER_NAME));
        options2.setOption(esJsonSource.ES_RESOURCE, properties.getProperty("es.index.name"));
        options2.setOption(esJsonSource.ES_TYPE, properties.getProperty("es.index.type","defalut"));
        options2.setOption(esJsonSource.QUERY, properties.getProperty("es.query"));
        options2.setOption(esJsonSource.ES_PORT, properties.getProperty("es.port","9200"));
        options2.setOption(esJsonSource.ES_INDEX_READ_MISSING_AS_EMPTY, "true");
        options2.setOption(esJsonSource.ES_NODES_WAN_ONLY, "true");
        options2.setOption(esJsonSource.ES_SCROLL_SIZE, "10000");

        esJsonSource.setNodeOptions(options2);

        JavaRDD<JSONObject> helloJavaRDD = esJsonSource.read();

        CsvJsonLocalFileSink csvJsonLocalFileSink = new CsvJsonLocalFileSink();
        NodeOptions options = new NodeOptions();
        options.setOption(csvJsonLocalFileSink.FILE_SPLIT_BY,properties.getProperty(csvJsonLocalFileSink.FILE_SPLIT_BY,","));
        //todo
        int fileNum = Integer.valueOf(properties.getProperty(csvJsonLocalFileSink.FILE_NUM));
        if(fileNum > 0)
            options.setOption(csvJsonLocalFileSink.FILE_NUM, "1");
        options.setOption(csvJsonLocalFileSink.LOCAL_PATH, properties.getProperty("file.local.path"));
        options.setOption(csvJsonLocalFileSink.FILE_NAME, properties.getProperty("file.floder"));

        csvJsonLocalFileSink.setNodeOptions(options);
        csvJsonLocalFileSink.wirte(helloJavaRDD);


    }
}
