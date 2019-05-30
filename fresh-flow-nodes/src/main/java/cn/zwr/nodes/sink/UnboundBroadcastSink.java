package cn.zwr.nodes.sink;

import cn.zwr.core.node.UnboundSink;
import org.apache.spark.streaming.api.java.JavaDStream;


public class UnboundBroadcastSink<T> extends UnboundSink<T> {
    @Override
    public void write(JavaDStream<T> data) {

    }
}
