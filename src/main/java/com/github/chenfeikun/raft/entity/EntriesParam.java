package com.github.chenfeikun.raft.entity;

import com.github.chenfeikun.raft.logModule.LogEntry;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @desciption: EntriesParam
 * arguments for append entries rpc
 * @CreateTime: 2019-03-07
 * @author: chenfeikun
 */
public class EntriesParam implements Serializable {

    private long term;
    // leader's server id, followers use it to redirect
    private int leaderId;
    // index of prev log
    private int prevLogIndex;
    // term of prev log
    private long prevLogTerm;
    // log entries to append
    private LogEntry entry;
    // commit index of leader
    private int leaderCommit;

    private EntriesParam(Builder builder) {
        setTerm(builder.term);
        setLeaderId(builder.leaderId);
        setPrevLogIndex(builder.prevLogIndex);
        setPrevLogTerm(builder.term);
        setEntry(builder.entry);
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
        private LogEntry entry;
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

        public Builder entry(LogEntry logEntry) {
            this.entry = logEntry;
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

    public LogEntry getEntry() {
        return entry;
    }

    public void setEntry(LogEntry entry) {
        this.entry = entry;
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
                ", entry=" + entry +
                ", leaderCommit=" + leaderCommit +
                '}';
    }
}
