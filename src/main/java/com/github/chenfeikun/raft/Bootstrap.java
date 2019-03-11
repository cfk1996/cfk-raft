package com.github.chenfeikun.raft;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @desciption: Bootstrap
 * @CreateTime: 2019-03-10
 * @author: chenfeikun
 */
public class Bootstrap {

    public static ScheduledExecutorService service = Executors.newScheduledThreadPool(3);

    public static void main(String[] args) throws Exception {
//         service.scheduleAtFixedRate(new Task(), 0, 20, TimeUnit.MILLISECONDS);
//         Thread.sleep(10000);
//         System.out.println("======================");
        System.out.println(5/2);
    }

    public static class Task implements Runnable {
        @Override
        public void run() {
            System.out.println("start");
            try {
                Thread.sleep(10000);
            } catch (Exception e) {

            }
            System.out.println("end");
        }
    }
}
