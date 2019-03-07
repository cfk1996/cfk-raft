package com.github.chenfeikun.raft.entity;

/**
 * @desciption: EntriesResult
 * @CreateTime: 2019-03-07
 * @author: chenfeikun
 */
public class EntriesResult {

    // term of follower
    private long term;
    private boolean success;

    public EntriesResult(long term, boolean success) {
        this.term = term;
        this.success = success;
    }

    private EntriesResult(Builder builder) {
        setTerm(builder.term);
        setSuccess(builder.success);
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public static final Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private long term;
        private boolean success;

        private Builder() {}

        public Builder term(long term) {
            this.term = term;
            return this;
        }

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public EntriesResult build() {
            return new EntriesResult(this);
        }

    }
}
