package com.github.chenfeikun.raft.rpc.entity;

/**
 * @desciption: HeartBeatResponse
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class HeartBeatResponse extends RequestOrResponse {

    public HeartBeatResponse term(long term) {
        this.term = term;
        return this;
    }

    public HeartBeatResponse code(int code) {
        this.code = code;
        return this;
    }
}
