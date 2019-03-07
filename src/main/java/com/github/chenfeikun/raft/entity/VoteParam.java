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
}
