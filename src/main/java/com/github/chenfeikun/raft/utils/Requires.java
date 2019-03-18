package com.github.chenfeikun.raft.utils;

/**
 * @desciption: Requires
 * @CreateTime: 2019-03-17
 * @author: chenfeikun
 */
public class Requires {

    public static void requireNonNull(Object obj, String errorMsg) {
        if (obj == null) {
            throw new NullPointerException(errorMsg);
        }
    }
}
