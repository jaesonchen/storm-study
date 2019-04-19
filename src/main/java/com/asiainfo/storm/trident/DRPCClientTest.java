package com.asiainfo.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

/**   
 * @Description: DRPCClient
 * 
 * @author chenzq  
 * @date 2019年4月19日 下午5:43:59
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class DRPCClientTest {

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        //3772是drpc对外默认的服务端口
        try (DRPCClient client = new DRPCClient(conf, "localhost",3772)) {
            System.out.println("DRPC result:" + client.execute("words", "the man storm"));
        }
    }
}
