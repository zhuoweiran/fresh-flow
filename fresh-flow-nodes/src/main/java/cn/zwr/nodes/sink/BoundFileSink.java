package cn.zwr.nodes.sink;

import cn.zwr.core.node.BoundSink;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

/**
 * Create with IDEA.
 * Date: 2019-11-08 15:44
 */
public class BoundFileSink extends BoundSink<JSONObject> {
    public final String LOCAL_PATH = "local.path";
    public final String FILE_NUM = "file.num";

    @Override
    public void wirte(JavaRDD<JSONObject> rdd) {
        JavaRDD jsonRdd = rdd.map(new Function<JSONObject, String>() {
            @Override
            public String call(JSONObject v1) throws Exception {
                return JSONObject.toJSONString(v1);
            }
        });
        if(getOptionAsInt(FILE_NUM,1) > 0){
            jsonRdd = jsonRdd.repartition(getOptionAsInt(FILE_NUM,1));
        }
        jsonRdd.saveAsTextFile(getOption(LOCAL_PATH,"./"));
    }
}
