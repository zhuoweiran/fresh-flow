package cn.zwr.nodes.process;

import cn.zwr.nodes.process.common.BoundProcess;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create with IDEA.
 * Date: 2019-11-08 16:23
 */
public class StringToJsonParser extends BoundProcess<String, JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(StringToJsonParser.class);

    @Override
    public JavaRDD<JSONObject> process(JavaRDD<String> in) {

        JavaRDD<JSONObject> result = in.map(new Function<String, JSONObject>() {

            @Override
            public JSONObject call(String v1) throws Exception {
                JSONObject object = new JSONObject();
                try {
                    object = JSONObject.parseObject(v1);
                }catch (Exception e){
                    e.printStackTrace();
                    logger.error("parse string : [{}] to json catch exception", v1);
//                    System.out.println(v1);
                    return object;
                }
                return object;
            }
        });
        return result;
    }
}
