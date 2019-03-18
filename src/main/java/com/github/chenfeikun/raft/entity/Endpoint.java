package com.github.chenfeikun.raft.entity;

import com.github.chenfeikun.raft.common.Copiable;

import java.io.Serializable;

/**
 * @desciption: Endpoint, server address
 * @CreateTime: 2019-03-17
 * @author: chenfeikun
 */
public class Endpoint implements Copiable<Endpoint>, Serializable {

    private String ip;
    private int port;

    public Endpoint(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "Endpoint{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                '}';
    }

    @Override
    public int hashCode() {
        return ip.hashCode() + port;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Endpoint other = (Endpoint) obj;
        if (this.ip == null) {
            if (other.ip != null) {
                return false;
            }
        } else if (!this.ip.equals(other.ip)) {
            return false;
        }
        return this.port == other.port;
    }

    @Override
    public Endpoint copy() {
        return new Endpoint(this.ip, this.port);
    }
}
