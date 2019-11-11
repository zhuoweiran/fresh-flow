package cn.zwr.nodes.source;

import cn.zwr.core.node.BoundSource;
import com.google.gson.JsonObject;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.HashMap;
import java.util.Map;

/**
 * Create with IDEA.
 * Date: 2019-11-08 10:08
 */
public class CSVMapSource extends BoundSource<Map> {
    public final String FIELDS = "csv.fields";
    public final String PARTITION_NUM = "csv.partition.num";
    public final String FILE_PATH = "csv.file.path";
    public final String SPLITER = "csv.spilt.by";
    public final String DELETE_EXTERIOR = "csv.delete.exterior";

    @Getter
    @Setter
    private transient SparkConf conf;

    public CSVMapSource(SparkConf conf){
        this.conf = conf;
    }

    @Override
    public JavaRDD<Map> read() {
        JavaSparkContext sc = new JavaSparkContext(this.conf);
        System.out.println(getOption(FILE_PATH));
        System.out.println(getOptionAsInt(PARTITION_NUM,4));
        JavaRDD<String> stringRdd =  sc.textFile(getOption(FILE_PATH),getOptionAsInt(PARTITION_NUM,4));
        String[] fields = getOption(FIELDS).split(",");
        return stringRdd.map(new Function<String, Map>() {
            String spliter = getOption(SPLITER, ",");
            boolean deleteExterior = getOptionAsBoolean(DELETE_EXTERIOR, false);

            @Override
            public Map call(String fieldValues) throws Exception {
                if(fieldValues.length() < fields.length){
                    return null;
                }
                String[] values = fieldValues.split(spliter);

                if(fields.length != values.length){
                    return null;
                }

                Map<String,String> map = new HashMap<>();
                for(int i = 0 ; i < fields.length ; i++){
                    String value = values[i];

                    if(deleteExterior && value.length() >= 2){
                        value = value.substring(1, value.length() - 1);
                    }
                    map.put(fields[i], value);
                }
                return map;
            }
        });
//                .filter(new Function<Map, Boolean>() {
//            @Override
//            public Boolean call(Map v1) throws Exception {
//                if(v1 == null){
//                    return false;
//                }
//                return true;
//            }
//        });
    }
}
