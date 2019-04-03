package com.github.chenfeikun.raft.store;

import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.core.Entry;
import com.github.chenfeikun.raft.core.MemberState;

/**
 * @desciption: RaftStore
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public abstract class RaftStore implements LifeCycle {


    /** abstract method*/
    public abstract long getEndIndex();

    public abstract long getBeginIndex();

    public abstract Entry get(Long index);

    public abstract Entry appendAsFollower(Entry entry, long leaderTerm, String leaderId);

    public abstract Entry appendAsLeader(Entry entry);

    public abstract long getCommittedIndex();

    /** default method*/
    public void updateCommittedIndex(long term, long committedIndex) {

    }

    protected MemberState getMemberState() {
        return null;
    }
}
