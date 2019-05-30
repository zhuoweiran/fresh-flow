package cn.zwr.nodes.sink;

import cn.zwr.core.node.BoundSink;
import org.apache.spark.api.java.JavaRDD;

public class BoundTextFileSink extends BoundSink<String> {
    public final String LOCAL_PATH = "local.path";
    public final String FILE_NAME = "file.name";
    public final String FILE_NUM = "file.num";

    @Override
    public void wirte(JavaRDD<String> rdd) {
        int fileNum = getOptionAsInt(FILE_NUM, -1);
        if(fileNum > 0){
            rdd.repartition(fileNum);
        }
        rdd.saveAsTextFile(getOption(LOCAL_PATH,"./") + getOption(FILE_NAME, "test.csv"));
    }
}
