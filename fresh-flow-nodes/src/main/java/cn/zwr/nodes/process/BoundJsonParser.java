package cn.zwr.nodes.process;

import cn.zwr.core.node.BoundProcess;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class BoundJsonParser extends BoundProcess<String, JSONObject> {
    @Override
    public JavaRDD<JSONObject> process(JavaRDD<String> inRdd) {
        return inRdd.map(new Function<String, JSONObject>() {

            @Override
            public JSONObject call(String s) throws Exception {
                return JSONObject.parseObject(s);
            }
        });
    }
}
