package com.github.chenfeikun.raft;

/**
 * @desciption: LifeCycle
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public interface LifeCycle {

    void startup();

    void shutdown();
}
