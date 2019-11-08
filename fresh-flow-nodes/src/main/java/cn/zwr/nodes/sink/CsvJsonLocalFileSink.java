package cn.zwr.nodes.sink;

import cn.zwr.core.node.BoundSink;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.text.SimpleDateFormat;
import java.util.StringJoiner;

public class CsvJsonLocalFileSink extends BoundSink<JSONObject> {
    public final String LOCAL_PATH = "local.path";
    public final String FILE_NAME = "file.name";
    public final String FILE_NUM = "file.num";
    public final String FILE_SPLIT_BY = "file.split.by";

    @Override
    public void wirte(JavaRDD<JSONObject> rdd) {
        int filenum = rdd.getNumPartitions();
        if(filenum != getOptionAsInt(FILE_NUM, filenum))
            rdd = rdd.repartition(getOptionAsInt(FILE_NUM, filenum));
        String splite = getOption(FILE_SPLIT_BY,",");
        JavaRDD<String> strRdd = rdd.map(new Function<JSONObject, String>() {
            @Override
            public String call(JSONObject jsonObject) throws Exception {
                try {
                    if (jsonObject == null) {
                        return null;
                    }
                    StringJoiner stringJoiner = new StringJoiner(splite);
                    for (String key : jsonObject.keySet()) {
                        System.out.println(key);
                        stringJoiner.add("\"" + jsonObject.getString(key) + "\"");
                    }
                    return stringJoiner.toString();
                }catch (Exception e){
                    e.printStackTrace();
                    return null;
                }
            }
        });
        strRdd.saveAsTextFile(getOption(LOCAL_PATH,"./") + getOption(FILE_NAME, "test.csv"));
    }
}
