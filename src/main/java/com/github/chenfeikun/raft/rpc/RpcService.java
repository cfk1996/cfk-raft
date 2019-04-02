package com.github.chenfeikun.raft.rpc;

import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.core.RaftProtocol;
import com.github.chenfeikun.raft.core.RaftProtocolHander;

/**
 * @desciption: RpcService
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public abstract class RpcService implements LifeCycle, RaftProtocol, RaftProtocolHander {

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }
}
