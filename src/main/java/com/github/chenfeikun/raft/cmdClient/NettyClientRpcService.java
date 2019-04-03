package com.github.chenfeikun.raft.cmdClient;

import com.alibaba.fastjson.JSON;
import com.github.chenfeikun.raft.rpc.entity.*;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.CompletableFuture;

/**
 * @desciption: NettyClientRpcService
 * @CreateTime: 2019-04-02
 * @author: chenfeikun
 */
public class NettyClientRpcService extends ClientRpcService {

    private NettyRemotingClient remotingClient;

    public NettyClientRpcService() {
        this.remotingClient = new NettyRemotingClient(new NettyClientConfig(), null);
    }

    @Override
    public void startup() {
        this.remotingClient.start();
    }

    @Override
    public void shutdown() {
        this.remotingClient.shutdown();
    }

    @Override
    public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(RequestCode.GET.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        GetEntriesResponse response = JSON.parseObject(wrapperResponse.getBody(), GetEntriesResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(RequestCode.APPEND.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        AppendEntryResponse response = JSON.parseObject(wrapperResponse.getBody(), AppendEntryResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(RequestCode.METADATA.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        MetadataResponse response = JSON.parseObject(wrapperResponse.getBody(), MetadataResponse.class);
        return CompletableFuture.completedFuture(response);
    }
}
