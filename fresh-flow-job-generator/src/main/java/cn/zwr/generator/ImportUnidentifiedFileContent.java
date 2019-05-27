package cn.zwr.generator;

import cn.zwr.core.node.NodeOptions;
import cn.zwr.nodes.sink.ESSchemaSink;
import cn.zwr.nodes.source.CsvSchemaFileSorce;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * 导入可疑文件内容
 */
public class ImportUnidentifiedFileContent {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String path = "conf/ImportUnidentifiedFileContent.properties";
        if(args.length > 0){
            path = args[0];
        }

        FileInputStream fileInputStream = new FileInputStream(path);
        properties.load(fileInputStream);

        SparkConf conf = new SparkConf();
        conf.setAppName(ImportUnidentifiedFileContent.class.getName()).setMaster("local[1]").set("appName", ImportUnidentifiedFileContent.class.getName());

        System.out.println(conf.get("appName"));

        CsvSchemaFileSorce<UnidentifiedFileContent> csvSchemaFileSorce = new CsvSchemaFileSorce<>(conf);
        NodeOptions options = new NodeOptions();
        options.setOption(csvSchemaFileSorce.FILE_PATH, properties.getProperty("file.local.path"));
        options.setOption(csvSchemaFileSorce.PARTITION_NUM, properties.getProperty("partition.num"));
        options.setOption(csvSchemaFileSorce.FILE_FOLDER, properties.getProperty("file.folder"));
        options.setOption(csvSchemaFileSorce.FILE_FILTER, properties.getProperty("file.filter"));
        options.setOption(csvSchemaFileSorce.FILE_FIELDS_ORDER, properties.getProperty("file.fields.order"));

        csvSchemaFileSorce.setNodeOptions(options);
        csvSchemaFileSorce.setT(new UnidentifiedFileContent());

        ESSchemaSink<UnidentifiedFileContent> esSchemaSink = new ESSchemaSink<>();
        NodeOptions options1 = new NodeOptions();
        options1.setOption(ESSchemaSink.CLUSTER_NAME, properties.getProperty("cluster.name"));
        options1.setOption(ESSchemaSink.ES_NODES, properties.getProperty("es.nodes"));
        options1.setOption(ESSchemaSink.ES_PORT, properties.getProperty("es.port"));
        options1.setOption(ESSchemaSink.ES_RESOURCE, properties.getProperty("es.index.name"));
        options1.setOption(ESSchemaSink.ES_TYPE, properties.getProperty("es.type"));

        esSchemaSink.setNodeOptions(options1);

        JavaRDD<UnidentifiedFileContent> csvRdd = csvSchemaFileSorce.read();
        esSchemaSink.wirte(csvRdd);
    }
}
