package com.github.chenfeikun.raft.rpc.entity;

/**
 * @desciption: PullEntriesRequest
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class PullEntriesRequest extends RequestOrResponse {

    private String nodeId;
    private Long beginIndex;

    public Long getBeginIndex() {
        return beginIndex;
    }

    public void setBeginIndex(Long beginIndex) {
        this.beginIndex = beginIndex;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
