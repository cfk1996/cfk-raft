package com.github.chenfeikun.raft.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @desciption: Peer
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public class Peer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Peer.class);

    // peers包含所有除Local server外的server实例
    private Map<Integer, Server> peers;
    // 本机服务器Id及server实例
    private Integer localID;
    private Server localServer;
    // leader服务器
    private Integer leaderID;

    public Peer(Server[] servers, Server localServer) {
        for (Server s : servers) {
            if (s.getServerId() == localServer.getServerId()) {
                continue;
            }
            peers.put(s.getServerId(), s);
        }
        this.localID = localServer.getServerId();
        this.localServer = localServer;
        leaderID = -1;
    }

    public void addServer(Server server) {
        if (!peers.containsKey(server.getServerId())) {
            peers.put(server.getServerId(), server);
        }
    }

    public void removeServer(int serverId) {
        if (serverId != localID) {
            peers.remove(serverId);
        }

    }

    public Map<Integer, Server> getPeers() {
        return peers;
    }

    public Integer getLocalID() {
        return localID;
    }

    public Server getLocalServer() {
        return localServer;
    }

    public List<Server> getServerListExceptLocal() {
        return new ArrayList<>(peers.values());
    }

    public int getServerSizeWithoutLocal() {
        return peers.size();
    }

    public int getServerSize() {
        return peers.size() + 1;
    }

    public Integer getLeaderID() {
        return leaderID;
    }

    public void setLeaderID(Integer leaderID) {
        this.leaderID = leaderID;
    }
}
