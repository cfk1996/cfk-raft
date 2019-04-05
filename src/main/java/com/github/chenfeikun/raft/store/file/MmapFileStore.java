package com.github.chenfeikun.raft.store.file;

import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.core.Entry;
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

    @Override
    public long getEndIndex() {
        return 0;
    }

    @Override
    public long getBeginIndex() {
        return 0;
    }

    @Override
    public Entry get(Long index) {
        return null;
    }

    @Override
    public Entry appendAsFollower(Entry entry, long leaderTerm, String leaderId) {
        return null;
    }

    @Override
    public Entry appendAsLeader(Entry entry) {
        return null;
    }

    @Override
    public long getCommittedIndex() {
        return 0;
    }

    @Override
    public long truncate(Entry entry, long leaderTerm, String leaderId) {
        return 0;
    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }
}
