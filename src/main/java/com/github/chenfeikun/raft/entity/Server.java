package com.github.chenfeikun.raft.entity;

import com.github.chenfeikun.raft.exception.ParseServerException;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @desciption: Server
 * @CreateTime: 2019-03-05
 * @author: chenfeikun
 */
public class Server {

    // ipv4 regex
    public static final String IPV4_REGEX = "(^((22[0-3]|2[0-1][0-9]|[0-1][0-9][0-9]|([0-9]){1,2})"
            + "([.](25[0-5]|2[0-4][0-9]|[0-1][0-9][0-9]|([0-9]){1,2})){3})$)";
    public static final Pattern IPV4_PATTERN = Pattern.compile(IPV4_REGEX);


    private String url;

    private int port;

    private int serverId;

    public Server(String url, int port, int serverId) {
        this.url = url;
        this.port = port;
        this.serverId = serverId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    @Override
    public String toString() {
        return "Server{" +
                "url='" + url + '\'' +
                ", port=" + port +
                ", serverId=" + serverId +
                '}';
    }

    public static Server parseServerFromStr(String host, int id) {
        String[] ipAndPort = host.split(":");
        if (ipAndPort.length != 2) {
            throw new ParseServerException("host format error");
        }
        Matcher matcher = IPV4_PATTERN.matcher(ipAndPort[0]);
        if (!matcher.matches()) {
            throw new ParseServerException("ip format error, not ipv4");
        }
        int port = Integer.parseInt(ipAndPort[1]);
        return new Server(host, port, id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Server server = (Server) o;
        return port == server.port &&
                serverId == server.serverId &&
                Objects.equals(url, server.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url);
    }
}
