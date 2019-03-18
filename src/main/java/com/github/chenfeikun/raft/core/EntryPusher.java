package com.github.chenfeikun.raft.core;

import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.rpc.RpcService;
import com.github.chenfeikun.raft.store.RaftStore;

/**
 * @desciption: EntryPusher
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class EntryPusher implements LifeCycle {
    public EntryPusher(NodeConfig config, MemberState memberState, RaftStore store, RpcService rpcService) {
    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }
}
