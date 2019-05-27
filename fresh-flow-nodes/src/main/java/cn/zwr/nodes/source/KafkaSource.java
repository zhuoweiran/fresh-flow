package cn.zwr.nodes.source;


import cn.zwr.core.node.UnboundSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * <title>KafkaSource</title>
 * <p>读取Kafka的数据，只留value，返回</p>
 *
 * @author Alex Han
 * @version 1.1
 */
public class KafkaSource extends UnboundSource<String> {
    private static final long serialVersionUID = -189514862188250323L;

    public final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public final String KAFKA_TOPIC = "kafka.topic";
    public final String GROUP_ID = "group.id";
    public final String AUTO_OFFSET_RESET = "auto.offset.reset";
    public final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
    private final String KEY_DESERIALIZER = "key.deserializer";
    private final String VALUE_DESERIALIZER = "value.deserializer";
    public final String MAX_PARTITION_FETCH_BYTES = "max.partition.fetch.bytes";
    public final String WINDOW_SECONDS = "window.seconds.size";

    @Override
    public JavaDStream<String> read() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(BOOTSTRAP_SERVERS,getOption(BOOTSTRAP_SERVERS));
        kafkaParams.put(GROUP_ID,getOption(GROUP_ID));
        kafkaParams.put(AUTO_OFFSET_RESET,getOption(AUTO_OFFSET_RESET));
        kafkaParams.put(ENABLE_AUTO_COMMIT,getOptionAsBoolean(ENABLE_AUTO_COMMIT,false));
        kafkaParams.put(KEY_DESERIALIZER, StringDeserializer.class);
        kafkaParams.put(VALUE_DESERIALIZER,StringDeserializer.class);
        kafkaParams.put(MAX_PARTITION_FETCH_BYTES,getOptionAsInt(MAX_PARTITION_FETCH_BYTES,10485760));

        Collection<String> topics = Arrays.asList(getOption(KAFKA_TOPIC));

        JavaInputDStream<ConsumerRecord<String, String>> stream = step1(topics, kafkaParams);
        JavaDStream<String> result = step2(stream);

        return result;
    }

    public KafkaSource(JavaStreamingContext context) {
        super(context);

    }
    public JavaInputDStream<ConsumerRecord<String, String>> step1(
            Collection<String> topics,
            Map<String, Object> kafkaParams
    ){
        getJavaStreamingContext();
        return KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );
    }

    public JavaDStream<String> step2(JavaInputDStream<ConsumerRecord<String, String>> stream){
        return stream.mapToPair(record -> new Tuple2<>(record.key(),record.value()))
                .map(tuple2 -> tuple2._2);
    }

}
