package com.github.chenfeikun.raft.entity;

import com.github.chenfeikun.raft.logModule.LogEntry;

import java.util.Arrays;

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
    private long prevLogTerm;
    // log entries to append
    private LogEntry[] entries;
    // commit index of leader
    private int leaderCommit;

    private EntriesParam(Builder builder) {
        setTerm(builder.term);
        setLeaderId(builder.leaderId);
        setPrevLogIndex(builder.prevLogIndex);
        setPrevLogTerm(builder.term);
        setEntries(builder.entries);
        setLeaderCommit(builder.leaderCommit);
    }

    public static final Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private long term;
        private int leaderId;
        private int prevLogIndex;
        private long prevLogTerm;
        private LogEntry[] entries;
        private int leaderCommit;

        private Builder() {}

        public Builder term(long term) {
            this.term = term;
            return this;
        }

        public Builder leaderId(int id) {
            this.leaderId = id;
            return this;
        }

        public Builder prevLogIndex(int index) {
            this.prevLogIndex = index;
            return this;
        }

        public Builder prevLogTerm(long term) {
            this.prevLogTerm = term;
            return this;
        }

        public Builder entries(LogEntry[] entries) {
            this.entries = entries;
            return this;
        }

        public Builder leaderCommit(int index) {
            this.leaderCommit = index;
            return this;
        }

        public EntriesParam build() {
            return new EntriesParam(this);
        }

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

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(long prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
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

    @Override
    public String toString() {
        return "EntriesParam{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entries=" + Arrays.toString(entries) +
                ", leaderCommit=" + leaderCommit +
                '}';
    }
}
