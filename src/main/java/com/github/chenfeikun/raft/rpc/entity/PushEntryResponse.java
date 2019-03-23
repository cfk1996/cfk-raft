package com.github.chenfeikun.raft.rpc.entity;

/**
 * @desciption: PushEntryResponse
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class PushEntryResponse extends RequestOrResponse {

    private Long index;

    private long beginIndex;
    private long endIndex;

    public Long getIndex() {
        return index;
    }

    public void setIndex(Long index) {
        this.index = index;
    }

    public long getBeginIndex() {
        return beginIndex;
    }

    public void setBeginIndex(long beginIndex) {
        this.beginIndex = beginIndex;
    }

    public long getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(long endIndex) {
        this.endIndex = endIndex;
    }
}
