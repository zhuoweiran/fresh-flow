package cn.zwr.nodes.source;

import cn.zwr.core.node.BoundSource;
import cn.zwr.core.pojo.Mapable;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class CsvSchemaFileSorce<T extends Mapable> extends BoundSource<T> {
    public final String FILE_PATH = "file.path";
    public final String FILE_FIELDS_ORDER = "file.fields.order";
    public final String PARTITION_NUM = "partition.num";
    public final String FILE_FOLDER = "file.folder";
    public final String FILE_FILTER = "file.filter";

    @Getter@Setter
    private T t;


    public CsvSchemaFileSorce(SparkConf conf) {
        super(conf);
    }

    @Override
    public JavaRDD<T> read() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRdd = sc.textFile(
                getOption(FILE_PATH) + getOption(FILE_FOLDER) + getOption(FILE_FILTER),
                getOptionAsInt(PARTITION_NUM, 1));
        String fileFieldsOrder = getOption(FILE_FIELDS_ORDER);

        Class clazz = t.getClass();

        return fileRdd.map(new Function<String, T>() {
            @Override
            public T call(String s) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
                T t1 = (T) clazz.newInstance();
                String[] filedVals = s.split(",");
                List<Method> methods = Lists.newArrayList(clazz.getMethods());
                List<String> list = Lists.newArrayList(fileFieldsOrder.split(","));

                for(int i = 0 ; i < list.size() ; i++) {
                    String filedName = list.get(i);
                    String filedVal = filedVals[i].substring(1, filedVals[i].length()-1);
                    for(Method method : methods) {
                        if (method.getName().startsWith("set")) {
                            String methodName = method.getName();
                            methodName = methodName.substring(methodName.indexOf("set") + 3);
                            methodName = methodName.toLowerCase().charAt(0) + methodName.substring(1);
                            if (filedName.equals(methodName)) {
                                Type type = method.getParameterTypes()[0];
                                if(type == short.class || type == Short.class){
                                    method.invoke(t1, Short.valueOf(filedVal));
                                    continue;
                                }else if(type == long.class || type == Long.class){
                                    method.invoke(t1, Long.valueOf(filedVal));
                                    continue;
                                }else if(type == Date.class){
                                    method.invoke(t1, sdf.parse(filedVal));
                                    continue;
                                }else if(type == int.class || type == Integer.class){
                                    method.invoke(t1, Integer.valueOf(filedVal));
                                    continue;
                                }
                                method.invoke(t1, filedVal);
                            }
                        }
                    }
                }
                return t1;
            }
        });
    }
}
