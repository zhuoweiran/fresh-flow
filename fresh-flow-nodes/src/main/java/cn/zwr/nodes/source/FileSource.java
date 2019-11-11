package cn.zwr.nodes.source;

import cn.zwr.core.node.BoundSource;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create with IDEA.
 * Date: 2019-11-08 16:18
 */
public class FileSource extends BoundSource<String> {
    public final static String FILE_PATH = "file.path";

    @Getter
    @Setter
    private transient SparkConf conf;

    public FileSource(SparkConf conf){
        this.conf = conf;
    }

    @Override
    public JavaRDD<String> read() {
        JavaSparkContext jsc = new JavaSparkContext(this.conf);
        return jsc.textFile(getOption(FILE_PATH));
    }
}
