package com.github.chenfeikun.raft.entity;

/**
 * @desciption: VoteParam
 * @CreateTime: 2019-03-07
 * @author: chenfeikun
 */
public class VoteParam {

    private long term;
    // follower's server id
    private int candidateId;
    // index of candidate's last log
    private int lastLogIndex;
    // term of candidate's last log
    private long lastLogTerm;

    public VoteParam(long term, int candidateId, int lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    private VoteParam(Builder builder) {
        this.term = builder.term;
        this.candidateId = builder.candidateId;
        this.lastLogIndex = builder.lastLogIndex;
        this.lastLogTerm = builder.lastLogTerm;
    }

    public static final Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private long term;
        private int candidateId;
        private int lastLogIndex;
        private long lastLogTerm;

        private Builder() {}

        public Builder term(long term) {
            this.term = term;
            return this;
        }

        public Builder candidateId(int id) {
            this.candidateId = id;
            return this;
        }

        public Builder lastLogIndex(int index) {
            this.lastLogIndex = index;
            return this;
        }

        public Builder lastLogTerm(long term) {
            this.lastLogTerm = term;
            return this;
        }

        public VoteParam build() {
            return new VoteParam(this);
        }
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(int candidateId) {
        this.candidateId = candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(int lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public String toString() {
        return "VoteParam{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
