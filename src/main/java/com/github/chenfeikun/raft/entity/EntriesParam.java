package com.github.chenfeikun.raft.entity;

import com.github.chenfeikun.raft.LogEntry;

/**
 * @desciption: EntriesParam
 * arguments for append entries rpc
 * @CreateTime: 2019-03-07
 * @author: chenfeikun
 */
public class EntriesParam {

    private long term;
    // leader's server id, followers use it to redirect
    private int leaderId;
    // index of prev log
    private int prevLogIndex;
    // term of prev log
    private long prevLogItem;
    // log entries to append
    private LogEntry[] entries;
    // commit index of leader
    private int leaderCommit;

    public EntriesParam(long term, int leaderId, int prevLogIndex, long prevLogItem, LogEntry[] entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogItem = prevLogItem;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public long getPrevLogItem() {
        return prevLogItem;
    }

    public void setPrevLogItem(long prevLogItem) {
        this.prevLogItem = prevLogItem;
    }

    public LogEntry[] getEntries() {
        return entries;
    }

    public void setEntries(LogEntry[] entries) {
        this.entries = entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }
}
