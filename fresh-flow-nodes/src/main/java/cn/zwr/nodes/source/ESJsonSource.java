package cn.zwr.nodes.source;

import cn.zwr.nodes.source.common.ESSource;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.Map;

public class ESJsonSource<T extends JSONObject> extends ESSource<JSONObject> {

    public ESJsonSource(SparkConf conf) {
        super(conf);
    }

    @Override
    public JavaRDD<JSONObject> read() {
        JavaPairRDD<String, Map<String, Object>> beforeRdd = before();
        return beforeRdd.mapValues(new Function<Map<String, Object>, JSONObject>() {
            @Override
            public JSONObject call(Map<String, Object> stringObjectMap) throws Exception {
                try {
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.putAll(stringObjectMap);
                    return jsonObject;
                }catch (Exception e){
                    e.printStackTrace();
                    return null;
                }
            }
        }).values();
    }
}
