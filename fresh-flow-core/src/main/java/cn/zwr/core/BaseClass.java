package cn.zwr.core;

import cn.zwr.core.node.NodeOptions;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <title>Base Class</title>
 * <p>所有需要使用nodeOption的类的共同父类，便于分类和设置各个node的conf</p>
 * @author Alex Han
 * @version 1.0
 */
public class BaseClass {

    public static final Logger logger = LoggerFactory.getLogger(BaseClass.class);
    @Getter@Setter
    private String name;
    @Getter@Setter
    protected String catalog;
    @Getter@Setter
    protected NodeOptions nodeOptions;

    public String getOption(String name){
        return nodeOptions.getOption(name);
    }
    public String getOption(String name, String defautValue){
        String optStr = getOption(name);
        if(optStr == null){
            optStr = defautValue;
        }
        return optStr;
    }
    public long getOptionAsLong(String option, int defautValue) {
        String optStr = getOption(option);
        long value = defautValue;
        if (optStr != null) {
            try{
                value = Long.parseLong(optStr);
            }
            catch(NumberFormatException e){
                logger.warn("{} should be number instead of {}", option, optStr);
            }
        }
        return value;
    }
    public int getOptionAsInt(String option, int defautValue) {
        String optStr = getOption(option);
        int value = defautValue;
        if (optStr != null) {
            try{
                value = Integer.parseInt(optStr);
            }
            catch(NumberFormatException e){
                logger.warn("{} should be number instead of {}", option, optStr);
            }
        }
        return value;
    }

    public boolean getOptionAsBoolean(String option, boolean defautValue) {
        String optStr = getOption(option);
        boolean value = defautValue;
        if (optStr != null) {
            try{
                value = Boolean.parseBoolean(optStr);
            }
            catch(NumberFormatException e){
                logger.warn("{} should be number instead of {}", option, optStr);
            }
        }
        return value;
    }
}
