package com.github.chenfeikun.raft.options;

/**
 * @desciption: RpcOptions
 * @CreateTime: 2019-03-17
 * @author: chenfeikun
 */
public class RpcOptions {

    /**
     * Rpc connect timeout in milliseconds
     * Default: 1000(1s)
     */
    private int rpcConnectTimeoutMs = 1000;

    /**
     * RPC request default timeout in milliseconds
     * Default: 5000(5s)
     */
    private int rpcDefaultTimeout = 5000;

    /**
     * Rpc process thread pool size
     * Default: 10
     */
    private int rpcProcessorThreadPoolSize = 10;

    public int getRpcConnectTimeoutMs() {
        return rpcConnectTimeoutMs;
    }

    public void setRpcConnectTimeoutMs(int rpcConnectTimeoutMs) {
        this.rpcConnectTimeoutMs = rpcConnectTimeoutMs;
    }

    public int getRpcDefaultTimeout() {
        return rpcDefaultTimeout;
    }

    public void setRpcDefaultTimeout(int rpcDefaultTimeout) {
        this.rpcDefaultTimeout = rpcDefaultTimeout;
    }

    public int getRpcProcessorThreadPoolSize() {
        return rpcProcessorThreadPoolSize;
    }

    public void setRpcProcessorThreadPoolSize(int rpcProcessorThreadPoolSize) {
        this.rpcProcessorThreadPoolSize = rpcProcessorThreadPoolSize;
    }

    @Override
    public String toString() {
        return "RpcOptions{" +
                "rpcConnectTimeoutMs=" + rpcConnectTimeoutMs +
                ", rpcDefaultTimeout=" + rpcDefaultTimeout +
                ", rpcProcessorThreadPoolSize=" + rpcProcessorThreadPoolSize +
                '}';
    }
}
