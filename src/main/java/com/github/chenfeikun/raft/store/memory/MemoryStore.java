package com.github.chenfeikun.raft.store.memory;

import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.core.Entry;
import com.github.chenfeikun.raft.core.MemberState;
import com.github.chenfeikun.raft.rpc.entity.ResponseCode;
import com.github.chenfeikun.raft.store.RaftStore;
import com.github.chenfeikun.raft.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @desciption: MemoryStore
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class MemoryStore extends RaftStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryStore.class);

    private long beginIndex = -1;
    private long endIndex = -1;
    private long committedIndex = -1;
    private long endTerm;

    private Map<Long, Entry> cachedEntries = new ConcurrentHashMap<>();

    private NodeConfig nodeConfig;
    private MemberState memberState;

    public MemoryStore(NodeConfig config, MemberState state) {
        this.nodeConfig = config;
        this.memberState = state;
    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public long getEndIndex() {
        return endIndex;
    }

    @Override
    public long getBeginIndex() {
        return beginIndex;
    }

    @Override
    public Entry get(Long index) {
        return cachedEntries.get(index);
    }

    @Override
    public Entry appendAsFollower(Entry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), ResponseCode.NOT_FOLLOWER);
        synchronized (memberState) {
            PreConditions.check(memberState.isFollower(), ResponseCode.NOT_FOLLOWER);
            PreConditions.check(leaderTerm == memberState.getCurrTerm(), ResponseCode.INCONSISTENT_TERM);
            PreConditions.check(leaderId == memberState.getLeaderId(), ResponseCode.INCONSISTENT_LEADER);
            endTerm = memberState.getCurrTerm();
            endIndex = entry.getIndex();
            committedIndex = entry.getIndex();
            cachedEntries.put(entry.getIndex(), entry);
            if (beginIndex == -1) {
                beginIndex = endIndex;
            }
            updateEndIndexAndTerm();
            return entry;
        }
    }

    @Override
    public Entry appendAsLeader(Entry entry) {
        PreConditions.check(memberState.isLeader(), ResponseCode.NOT_LEADER);
        synchronized (memberState) {
            PreConditions.check(memberState.isLeader(), ResponseCode.NOT_LEADER);
            endIndex++;
            committedIndex++;
            endTerm = memberState.getCurrTerm();
            entry.setIndex(endIndex);
            entry.setTerm(memberState.getCurrTerm());
            cachedEntries.put(entry.getIndex(), entry);
            if (beginIndex == -1) {
                beginIndex = endIndex;
            }
            updateEndIndexAndTerm();
            return entry;
        }
    }

    private void updateEndIndexAndTerm() {
        if (getMemberState() != null) {
            getMemberState().updateEndIndexAndTerm(endIndex, endTerm);
        }
    }

    @Override
    public MemberState getMemberState() {
        return this.memberState;
    }

    @Override
    public long getCommittedIndex() {
        return committedIndex;
    }
}
