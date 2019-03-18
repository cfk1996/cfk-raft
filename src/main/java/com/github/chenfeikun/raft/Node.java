package com.github.chenfeikun.raft;

import com.github.chenfeikun.raft.options.NodeOptions;
import com.github.chenfeikun.raft.entity.PeerId;
import com.github.chenfeikun.raft.entity.Task;

/**
 * @desciption: Node
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public interface Node extends LifeCycle<NodeOptions>{

    /**
     * 将任务应用到复制状态机
     * @param task
     */
    void apply(Task task);

    /**
     * get leader's peerid
     * @return
     */
    PeerId getLeaderID();

    void join();

    void snapshot(Closure done);
}
