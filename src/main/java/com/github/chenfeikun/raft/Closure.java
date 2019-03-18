package com.github.chenfeikun.raft;

import com.github.chenfeikun.raft.entity.Status;

/**
 * @desciption: Closure
 * @CreateTime: 2019-03-17
 * @author: chenfeikun
 */
public interface Closure {

    /**
     * Called when task is done.
     * @param status task status
     */
    void run(Status status);
}
