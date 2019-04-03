package com.github.chenfeikun.raft;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desciption: BootStrap
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class BootStrap {

    private static final Logger logger = LoggerFactory.getLogger(BootStrap.class);

    public static void main(String[] args) {
        NodeConfig config = new NodeConfig();
        JCommander.newBuilder().addObject(config).build().parse(args);
        NodeServer server = new NodeServer(config);
        server.startup();
        logger.info("[{}] group {} start ok with config {}", config.getSelfId(), config.getGroup(), JSON.toJSONString(config));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;

            @Override
            public void run() {
                synchronized (this) {
                    logger.info("Shutdown hook was invoked");
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        server.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        logger.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        }, "ShutdownHook"));
    }
}
