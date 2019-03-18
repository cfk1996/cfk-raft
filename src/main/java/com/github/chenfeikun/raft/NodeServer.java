package com.github.chenfeikun.raft;

import com.github.chenfeikun.raft.core.EntryPusher;
import com.github.chenfeikun.raft.core.LeaderElection;
import com.github.chenfeikun.raft.core.MemberState;
import com.github.chenfeikun.raft.rpc.RpcService;
import com.github.chenfeikun.raft.store.memory.MemoryStore;
import com.github.chenfeikun.raft.store.file.MmapFileStore;
import com.github.chenfeikun.raft.store.RaftStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desciption: NodeServer
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class NodeServer implements LifeCycle {

    private static final Logger LOG = LoggerFactory.getLogger(NodeServer.class);

    private MemberState memberState;
    private NodeConfig config;

    private RaftStore store;
    private RpcService rpcService;
    private EntryPusher entryPusher;
    private LeaderElection leaderElection;

    public NodeServer(NodeConfig config) {
        this.config = config;
        this.memberState = new MemberState(config);
        this.store = createStore(this.config.getStoreType(), this.config, memberState);
        this.rpcService = new RpcService(this);
        this.entryPusher = new EntryPusher(config, memberState, store, rpcService);
        this.leaderElection = new LeaderElection(config, memberState, rpcService);
    }


    private RaftStore createStore(String type, NodeConfig config, MemberState state) {
        if (type.equals(NodeConfig.MEMORY)) {
            return new MemoryStore(config, memberState);
        } else {
            return new MmapFileStore(config, memberState);
        }
    }

    @Override
    public void startup() {
        this.store.startup();
        this.rpcService.startup();
        this.entryPusher.startup();
        this.leaderElection.startup();
    }

    @Override
    public void shutdown() {
        this.store.shutdown();
        this.rpcService.shutdown();
        this.entryPusher.shutdown();
        this.leaderElection.shutdown();
    }

    public MemberState getMemberState() {
        return memberState;
    }

    public void setMemberState(MemberState memberState) {
        this.memberState = memberState;
    }

    public NodeConfig getConfig() {
        return config;
    }

    public void setConfig(NodeConfig config) {
        this.config = config;
    }

    public RaftStore getStore() {
        return store;
    }

    public void setStore(RaftStore store) {
        this.store = store;
    }

    public RpcService getRpcService() {
        return rpcService;
    }

    public void setRpcService(RpcService rpcService) {
        this.rpcService = rpcService;
    }
}
