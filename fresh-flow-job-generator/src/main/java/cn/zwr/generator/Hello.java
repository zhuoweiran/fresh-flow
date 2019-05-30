package cn.zwr.generator;


import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data@Builder
public class Hello {
    private String a;
    private int b;
    public static void main(String[] args) {
        Map map = Maps.newHashMap();
        map.put("a", 1);
        map.put("b", "b");
        map.put("c", 1.1);
        JSONObject jsonObject = new JSONObject();
        jsonObject.putAll(map);
        System.out.println(jsonObject.toJSONString());
        for(String key : jsonObject.getInnerMap().keySet()){
            System.out.println(key + " : " + jsonObject.get(key) + "(" + jsonObject.get(key).getClass().getName() + ")");
        }
    }
}
