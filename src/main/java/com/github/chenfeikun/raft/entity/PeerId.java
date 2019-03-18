package com.github.chenfeikun.raft.entity;

import com.github.chenfeikun.raft.common.Copiable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @desciption: PeerId,代表raft协议参与者
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public class PeerId implements Copiable<PeerId>, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeerId.class);

    private Endpoint endpoint;

    public PeerId(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PeerId)) return false;
        PeerId peerId = (PeerId) o;
        return getEndpoint().equals(peerId.getEndpoint());
    }

    public String getIp() {
        return endpoint.getIp();
    }

    public int getPort() {
        return endpoint.getPort();
    }

    @Override
    public int hashCode() {
        return endpoint.hashCode();
    }

    @Override
    public PeerId copy() {
        return new PeerId(endpoint.copy());
    }
}
