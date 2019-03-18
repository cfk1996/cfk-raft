package com.github.chenfeikun.raft;

/**
 * @desciption: LifeCycle
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public interface LifeCycle<T> {

    boolean init(T config);

    void shutdown();
}
