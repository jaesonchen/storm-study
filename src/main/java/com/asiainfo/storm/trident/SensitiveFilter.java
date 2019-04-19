package com.asiainfo.storm.trident;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

/**   
 * @Description: trident stream filter
 * 
 * @author chenzq  
 * @date 2019年4月19日 下午4:12:40
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
@SuppressWarnings("serial")
public class SensitiveFilter extends BaseFilter {

    List<String> sensitiveList = new ArrayList<>();
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        sensitiveList.addAll(Arrays.asList(new String[] { "fuck", "cnm", "sb" }));
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return !sensitiveList.contains(tuple.get(0));
    }
}
