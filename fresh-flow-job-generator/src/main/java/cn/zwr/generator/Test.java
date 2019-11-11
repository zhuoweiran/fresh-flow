package cn.zwr.generator;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Create with IDEA.
 * Date: 2019-11-08 10:47
 */
public class Test {
    private static final Logger logger = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) {
        String a = "{\"securityPartition\":\"I区\",\"destIp\":\"10.88.94.1\",\"srcPort\":\"53364\",\"source\":\"厂站\",\"deviceName\":\"I区A平面纵向加密装置1\",\"processTime\":1572886922000,\"detailType\":\"1\",\"speciality\":\"networkSecurity\",\"firm\":\"南方电网\",\"protocol\":\"TCP\",\"destPort\":\"2404\",\"devGuid\":\"00-336e58ca-db8a-47a2-823b-957feda9c9a3\",\"id\":\"f2d0d364b6fd4e1bb352428e5d0d579f\",\"tag\":\"SecurePolicy\",\"funClassValue\":\"deny\",\"destMac\":\"-\",\"timestamp\":\"1572887324000\",\"deviceType\":\"VEAD\",\"severity\":\"1\",\"srcIp\":\"10.88.4.4\",\"corpId\":\"00\",\"discoverTime\":1572887324000,\"ip\":\"10.88.1.11\",\"srcDeviceGUID\":\"00-336e58ca-db8a-47a2-823b-957feda9c9a3\",\"sessionStartTime\":\"2019-11-05T01:08:44+08:00\",\"corpName\":\"南网总调主站\",\"dcdGuid\":\"00-a72dec95-8743-46cb-b7a7-e6c6aa39e77a\",\"eventCode\":\"NetBehavior\",\"transferUnit\":\"南网总调\",\"unit\":\"南方电网\",\"alarmType\":\"0\",\"srcMac\":\"-\",\"destDeviceGUID\":\"00-a72dec95-8743-46cb-b7a7-e6c6aa39e77a\",\"status\":\"0\"}";
        System.out.println(JSONObject.parse(a));
    }
}
