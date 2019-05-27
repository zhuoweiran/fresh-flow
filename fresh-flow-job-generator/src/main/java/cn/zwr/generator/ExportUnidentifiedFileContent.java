package cn.zwr.generator;

import cn.zwr.core.node.NodeOptions;
import cn.zwr.nodes.sink.CsvLocalFileSink;
import cn.zwr.nodes.source.ESSchemaSource;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * 导出可以文件内容
 */
public class ExportUnidentifiedFileContent {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String path = "conf/ExportUnidentifiedFileContent.properties";

        SparkConf conf = new SparkConf();
        conf.setAppName(ExportUnidentifiedFile.class.getName()).setMaster("local[1]").set("appName", ExportUnidentifiedFileContent.class.getName());


        FileInputStream fileInputStream = new FileInputStream(path);
        properties.load(fileInputStream);

        ESSchemaSource<UnidentifiedFileContent> ESSchemaSource = new ESSchemaSource<>(conf);
        NodeOptions options2 = new NodeOptions();
        options2.setOption(ESSchemaSource.ES_NODES, properties.getProperty(ESSchemaSource.ES_NODES));
        options2.setOption(ESSchemaSource.CLUSTER_NAME, properties.getProperty(ESSchemaSource.CLUSTER_NAME));
        options2.setOption(ESSchemaSource.ES_RESOURCE, properties.getProperty("es.index.name"));
        options2.setOption(ESSchemaSource.ES_TYPE, properties.getProperty("es.index.type","defalut"));
        options2.setOption(ESSchemaSource.QUERY, properties.getProperty("es.query","{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}}}"));
        options2.setOption(ESSchemaSource.ES_PORT, properties.getProperty("es.port","9200"));
        options2.setOption(ESSchemaSource.ES_INDEX_READ_MISSING_AS_EMPTY, "true");
        options2.setOption(ESSchemaSource.ES_NODES_WAN_ONLY, "true");
        options2.setOption(ESSchemaSource.ES_SCROLL_SIZE, "10000");

        UnidentifiedFileContent unidentifiedFile = new UnidentifiedFileContent();

        ESSchemaSource.setNodeOptions(options2);
        ESSchemaSource.setT(unidentifiedFile);

        JavaRDD<UnidentifiedFileContent> helloJavaRDD = ESSchemaSource.read();

        CsvLocalFileSink<UnidentifiedFileContent> helloCsvLocalFileSink = new CsvLocalFileSink<>();
        NodeOptions options = new NodeOptions();
        options.setOption(helloCsvLocalFileSink.FILE_SPLIT_BY,properties.getProperty(helloCsvLocalFileSink.FILE_SPLIT_BY,","));
        //todo
        int fileNum = Integer.valueOf(properties.getProperty(helloCsvLocalFileSink.FILE_NUM));
        if(fileNum > 0)
            options.setOption(helloCsvLocalFileSink.FILE_NUM, "1");
        options.setOption(helloCsvLocalFileSink.LOCAL_PATH, properties.getProperty("file.local.path"));
        options.setOption(helloCsvLocalFileSink.FILE_NAME, properties.getProperty("file.floder"));

        helloCsvLocalFileSink.setNodeOptions(options);
        helloCsvLocalFileSink.wirte(helloJavaRDD);

    }
}
