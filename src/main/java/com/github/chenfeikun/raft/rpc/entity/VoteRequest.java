package com.github.chenfeikun.raft.rpc.entity;

/**
 * @desciption: VoteRequest
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class VoteRequest extends RequestOrResponse {

    private long endIndex = -1;

    private long endTerm = -1;

    public long getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(long endIndex) {
        this.endIndex = endIndex;
    }

    public long getEndTerm() {
        return endTerm;
    }

    public void setEndTerm(long endTerm) {
        this.endTerm = endTerm;
    }
}
