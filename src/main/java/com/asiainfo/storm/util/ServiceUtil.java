package com.asiainfo.storm.util;

import java.util.concurrent.TimeUnit;

/**   
 * @Description: TODO
 * 
 * @author chenzq  
 * @date 2019年4月18日 下午2:40:00
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class ServiceUtil {
    
    public static void sleep(long timeout) {
        try {
            TimeUnit.MILLISECONDS.sleep(timeout);
        } catch (InterruptedException e) {
            // ignor
        }
    }
    
    public static void sleep(long timeout, TimeUnit unit) {
        try {
            long millis = unit.toMillis(timeout);
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            // ignor
        }
    }
}
