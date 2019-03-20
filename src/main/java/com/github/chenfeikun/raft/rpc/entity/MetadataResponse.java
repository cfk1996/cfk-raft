package com.github.chenfeikun.raft.rpc.entity;

import java.util.Map;

/**
 * @desciption: MetadataResponse
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class MetadataResponse extends RequestOrResponse {

    private Map<String, String> peers;

    public Map<String, String> getPeers() {
        return peers;
    }

    public void setPeers(Map<String, String> peers) {
        this.peers = peers;
    }
}
