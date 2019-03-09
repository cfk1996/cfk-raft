package com.github.chenfeikun.raft.rpc;

/**
 * @desciption: RpcClient
 * @CreateTime: 2019-03-09
 * @author: chenfeikun
 */
public interface RpcClient {

    Response invoke(Request request);
}
