package cn.zwr.core.pojo;

import scala.Tuple2;

import java.util.Map;

/**
 * <title>Mapable</title>
 * <p>使所有的pojo具有转化为map的方法，便于存储于ES</p>
 * <p>所有需要存储于ES的pojo都要实现这个接口</p>
 *
 * @author Alex Han
 * @version 1.0
 */
public interface Tupleable {
    //转化为可以存储的map类型
    Tuple2<String,Object> transformMap();
}
