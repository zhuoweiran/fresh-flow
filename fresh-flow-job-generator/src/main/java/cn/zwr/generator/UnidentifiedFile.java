package cn.zwr.generator;

import cn.zwr.core.pojo.Mapable;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

@Data
@ToString
public class UnidentifiedFile implements Mapable,Serializable {
    private String destIp;
    private String srcIp;
    private short summitStatus;
    private String fileName;
    private String corpId;
    private String destArea;
    private Date lastCheckDate;
    private String srcPort;
    private int threatScore;
    private Date sessionStartDate;
    private String protocol;
    private String destPort;
    private long fileSize;
    private String srcArea;
    private String srcGuName;
    private short verdict;
    private String destGuName;
    private Date sessionEndDate;
    private String md5;
    private Date createDate;
    private short status;
    private String verdictContent;

    @Override
    public Map<String, Object> transformMap() {
        Map map = Maps.newHashMap();
        map.put("destIp", destIp);
        map.put("srcIp", srcIp);
        map.put("summitStatus", summitStatus);
        map.put("fileName", fileName);
        map.put("corpId", corpId);
        map.put("destArea", destArea);
        map.put("lastCheckDate", lastCheckDate);
        map.put("srcPort", srcPort);
        map.put("threatScore", threatScore);
        map.put("sessionStartDate", sessionStartDate);
        map.put("protocol", protocol);
        map.put("destPort", destPort);
        map.put("fileSize", fileSize);
        map.put("srcArea", srcArea);
        map.put("srcGuName", srcGuName);
        map.put("verdict", verdict);
        map.put("destGuName", destGuName);
        map.put("sessionEndDate", sessionEndDate);
        map.put("md5", md5);
        map.put("createDate", createDate);
        map.put("status", status);
        map.put("verdictContent", verdictContent);
        return map;
    }
}
