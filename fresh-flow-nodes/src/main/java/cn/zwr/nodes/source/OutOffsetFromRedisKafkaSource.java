package cn.zwr.nodes.source;

import cn.zwr.pojo.KafkaOffsetRedisObject;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * <title>OutOffsetFromRedisKafkaSource</title>
 * <p>在redis中读取offset的kafka source</p>
 *
 * @author Alex Han
 * @version 1.0
 */
public class OutOffsetFromRedisKafkaSource extends KafkaSource{
    private static final long serialVersionUID = 4637581512826072637L;

    @Getter@Setter
    private transient Jedis jedis;
    public static final String REDIS_INDEX_DB = "redis.index.DB";
    @Getter
    private String appName;

    public OutOffsetFromRedisKafkaSource(JavaStreamingContext context) {
        super(context);
        this.appName = context.sparkContext().getConf().get("appName");
    }

    /**
     * 重写第一步，从redis中读取offset
     * @param topics
     * @param kafkaParams
     * @return
     */
    @Override
    public JavaInputDStream<ConsumerRecord<String, String>> step1(
            Collection<String> topics,
            Map<String, Object> kafkaParams
    ) {
        jedis.select(getOptionAsInt(REDIS_INDEX_DB,0));
        String topicOffsetJson = jedis.get(getAppName() + ":" + getOption(KAFKA_TOPIC));
        List<KafkaOffsetRedisObject> list = JSONArray.parseArray(topicOffsetJson, KafkaOffsetRedisObject.class);

        Map<TopicPartition, Long> fromOffsets = Maps.newHashMap();
        if(list != null)
        for (KafkaOffsetRedisObject offsetRedisObject : list){
            if(offsetRedisObject.getUntilOffset() != null && offsetRedisObject.getPartition() != null){
                fromOffsets.put(new TopicPartition(offsetRedisObject.getTopic(), offsetRedisObject.getPartition()),
                        offsetRedisObject.getUntilOffset());
            }
        }
        if(fromOffsets.size() > 0){ //从redis记录的offset处消费
            return KafkaUtils.createDirectStream(
                    getJavaStreamingContext(),
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Assign(fromOffsets.keySet(), kafkaParams, fromOffsets)
            );
        }
        // 否则从开始消费
        return super.step1(topics, kafkaParams);
    }

    /**
     * 重写第二步，存储offset到redis
     * @param stream
     * @return
     */
    @Override
    public JavaDStream<String> step2(JavaInputDStream<ConsumerRecord<String, String>> stream) {
        stream.foreachRDD(
                rdd ->{
                    OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    List<KafkaOffsetRedisObject> topicOffsetList = Lists.newArrayList();
                    for (OffsetRange offsetRange : offsetRanges) {
                        topicOffsetList.add(
                                KafkaOffsetRedisObject.builder()
                                        .topic(getOption(KAFKA_TOPIC))
                                        .partition(offsetRange.partition())
                                        .untilOffset(offsetRange.fromOffset())
                                        .untilOffset(offsetRange.untilOffset())
                                        .build()
                        );
                    }
                    jedis.select(getOptionAsInt(REDIS_INDEX_DB,0));
                    jedis.set(getAppName() + ":" + getOption(KAFKA_TOPIC), JSONArray.toJSONString(topicOffsetList));
                }

        );
        return super.step2(stream);
    }
}
