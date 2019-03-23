package com.github.chenfeikun.raft.utils;

/**
 * @desciption: Utils
 * @CreateTime: 2019-03-21
 * @author: chenfeikun
 */
public class Utils {

    public static void sleep(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (Throwable ignored) {

        }
    }


    public static long elapsed(long start) {
        return System.currentTimeMillis() - start;
    }
}
