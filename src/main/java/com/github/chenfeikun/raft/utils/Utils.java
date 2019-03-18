package com.github.chenfeikun.raft.utils;

import java.util.concurrent.TimeUnit;

/**
 * @desciption: Utils
 * @CreateTime: 2019-03-17
 * @author: chenfeikun
 */
public class Utils {

    /** static variable*/
    public static final String IP_ANY = "0.0.0.0";


    /**
     * Get system cpu counts.
     * @return
     */
    public static int cpus() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Gets the current monotonic time in milliseconds.
     */
    public static long monotonicMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }
}
