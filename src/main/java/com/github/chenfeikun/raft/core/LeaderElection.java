package com.github.chenfeikun.raft.core;

import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.rpc.RpcService;

/**
 * @desciption: LeaderElection
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class LeaderElection implements LifeCycle {
    public LeaderElection(NodeConfig config, MemberState memberState, RpcService rpcService) {

    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }
}
