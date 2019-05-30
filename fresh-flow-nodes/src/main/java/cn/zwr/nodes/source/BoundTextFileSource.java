package cn.zwr.nodes.source;

import cn.zwr.core.node.BoundSource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class BoundTextFileSource extends BoundSource<String> {
    public final String FILE_PATH = "file.path";
    public final String FILE_FIELDS_ORDER = "file.fields.order";
    public final String PARTITION_NUM = "partition.num";
    public final String FILE_FOLDER = "file.folder";
    public final String FILE_FILTER = "file.filter";

    public BoundTextFileSource(SparkConf conf) {
        super(conf);
    }

    @Override
    public JavaRDD<String> read() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRdd = sc.textFile(
                getOption(FILE_PATH) + getOption(FILE_FOLDER) + getOption(FILE_FILTER),
                getOptionAsInt(PARTITION_NUM, 1));
        return fileRdd;
    }
}
