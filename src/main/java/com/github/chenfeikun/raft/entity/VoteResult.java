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
}
