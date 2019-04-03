package com.github.chenfeikun.raft.cmdClient;

import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.core.RaftClientProtocol;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @desciption: clientRpcService
 * @CreateTime: 2019-04-02
 * @author: chenfeikun
 */
public abstract class ClientRpcService implements RaftClientProtocol, LifeCycle {

    private Map<String, String> peerMap = new ConcurrentHashMap<>();

    public void updatePeers(String peers) {
        for (String peerInfo : peers.split(";")) {
            peerMap.put(peerInfo.split("-")[0], peerInfo.split("-")[1]);
        }
    }

    public void updatePeers(Map<String, String> peers) {
        peerMap.putAll(peers);
    }

    public String getPeerAddr(String key) {
        return peerMap.get(key);
    }
}
