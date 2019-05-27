package cn.zwr.nodes.source;

import cn.zwr.nodes.source.common.ESSource;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * <title>ESSchemaSource</title>
 * <p>从Es读取数据生成Object</p>
 *
 * @param <T>
 *
 * @author Alex Han
 * @version 1.0
 */
public class ESSchemaSource<T> extends ESSource<T> {
    @Getter@Setter
    private T t;

    public ESSchemaSource(SparkConf conf) {
        super(conf);
    }


    @Override
    public JavaRDD<T> read() {

        JavaPairRDD<String, Map<String, Object>> esRdd = before();

        JavaRDD<T> tRdd = esRdd.mapValues(new Function<Map<String, Object>, T>() {
            @Override
            public T call(Map<String, Object> map) throws Exception {
                Class clazz = t.getClass();
                T t1 = (T) clazz.newInstance();
                for(Method method : clazz.getMethods()){
                    if (method.getName().startsWith("set")) {
                        String field = method.getName();
                        field = field.substring(field.indexOf("set") + 3);
                        field = field.toLowerCase().charAt(0) + field.substring(1);
                        if (map.containsKey(field)) {
                            method.invoke(t1, map.get(field) == null ? null : map.get(field));
                        }
                    }
                }

                return t1;
            }}).values();

        return tRdd;
    }
}
