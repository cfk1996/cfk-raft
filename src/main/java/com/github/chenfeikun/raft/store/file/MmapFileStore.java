package com.github.chenfeikun.raft.store.file;

import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.core.MemberState;
import com.github.chenfeikun.raft.store.RaftStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desciption: MmapFileStore
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class MmapFileStore extends RaftStore {

    /** static variable*/
    private static final Logger LOG = LoggerFactory.getLogger(MmapFileStore.class);

    public static final int INDEX_UNIT_SIZE = 32;

    /** internal variable*/
    private NodeConfig config;
    private MemberState state;

    public MmapFileStore(NodeConfig config, MemberState state) {
        this.config = config;
        this.state = state;
    }

}
