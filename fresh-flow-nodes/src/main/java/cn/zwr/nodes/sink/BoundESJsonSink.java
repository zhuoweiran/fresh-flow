package cn.zwr.nodes.sink;

import cn.zwr.core.node.BoundSink;
import cn.zwr.nodes.common.ESConfigureAble;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.Map;

public class BoundESJsonSink extends BoundSink<JSONObject> implements ESConfigureAble {

    @Override
    public void wirte(JavaRDD<JSONObject> rdd) {
        rdd.map(new Function<JSONObject, Map<String, Object>>() {
            @Override
            public Map<String, Object> call(JSONObject jsonObject) throws Exception {
                return jsonObject.getInnerMap();
            }
        });
    }
}
