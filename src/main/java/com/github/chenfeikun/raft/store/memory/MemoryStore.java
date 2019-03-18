package com.github.chenfeikun.raft.store.memory;

import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.core.MemberState;
import com.github.chenfeikun.raft.store.RaftStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desciption: MemoryStore
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class MemoryStore extends RaftStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryStore.class);

    private NodeConfig config;
    private MemberState state;

    public MemoryStore(NodeConfig config, MemberState state) {
        this.config = config;
        this.state = state;
    }

}
