package com.github.chenfeikun.raft.rpc.entity;

/**
 * @desciption: AppendEntryRequest
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class AppendEntryRequest extends RequestOrResponse {

    private byte[] body;

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
