package com.github.chenfeikun.raft.rpc;

/**
 * @desciption: RpcServer
 * @CreateTime: 2019-03-09
 * @author: chenfeikun
 */
public interface RpcServer {

    void start();

    void stop();

    Response handleRequest(Request request);
}
