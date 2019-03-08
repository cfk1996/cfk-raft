package com.github.chenfeikun.raft.entity;

/**
 * @desciption: VoteResult
 * @CreateTime: 2019-03-07
 * @author: chenfeikun
 */
public class VoteResult {

    // term of response server
    private long term;
    // vote result
    private boolean voteGranted;

    public VoteResult(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    private VoteResult(Builder builder) {
        this.term = builder.term;
        this.voteGranted = builder.voteGranted;
    }

    public static final Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private long term;
        private boolean voteGranted;

        private Builder() {}

        public Builder term(long term) {
            this.term = term;
            return this;
        }

        public Builder voteGranted(boolean isVote) {
            this.voteGranted = isVote;
            return this;
        }

        public VoteResult build() {
            return new VoteResult(this);
        }
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    @Override
    public String toString() {
        return "VoteResult{" +
                "term=" + term +
                ", voteGranted=" + voteGranted +
                '}';
    }
}
