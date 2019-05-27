package cn.zwr.generator;

import cn.zwr.core.pojo.Mapable;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@Data
@ToString
public class UnidentifiedFileContent implements Mapable, Serializable {
    private String content;
    private String md5;
    @Override
    public Map<String, Object> transformMap() {
        Map<String, Object> result = Maps.newHashMap();
        result.put("content", content);
        result.put("md5", md5);
        return result;
    }
}
