package com.github.chenfeikun.raft.rpc;

import com.alibaba.fastjson.JSON;
import com.github.chenfeikun.raft.NodeServer;
import com.github.chenfeikun.raft.core.MemberState;
import com.github.chenfeikun.raft.rpc.entity.AppendEntryRequest;
import com.github.chenfeikun.raft.rpc.entity.AppendEntryResponse;
import com.github.chenfeikun.raft.rpc.entity.GetEntriesRequest;
import com.github.chenfeikun.raft.rpc.entity.GetEntriesResponse;
import com.github.chenfeikun.raft.rpc.entity.HeartBeatRequest;
import com.github.chenfeikun.raft.rpc.entity.HeartBeatResponse;
import com.github.chenfeikun.raft.rpc.entity.MetadataRequest;
import com.github.chenfeikun.raft.rpc.entity.MetadataResponse;
import com.github.chenfeikun.raft.rpc.entity.PullEntriesRequest;
import com.github.chenfeikun.raft.rpc.entity.PullEntriesResponse;
import com.github.chenfeikun.raft.rpc.entity.PushEntryRequest;
import com.github.chenfeikun.raft.rpc.entity.PushEntryResponse;
import com.github.chenfeikun.raft.rpc.entity.RequestCode;
import com.github.chenfeikun.raft.rpc.entity.VoteRequest;
import com.github.chenfeikun.raft.rpc.entity.VoteResponse;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * @desciption: NettyRpcService
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class NettyRpcService extends RpcService {

    private static final Logger LOG = LoggerFactory.getLogger(NettyRpcService.class);

    /** raft*/
    private NodeServer server;
    private MemberState memberState;
    /** rpc*/
    private NettyRemotingServer remotingServer;
    private NettyRemotingClient remotingClient;


    public NettyRpcService(NodeServer server) {
        this.server = server;
        this.memberState = server.getMemberState();
        startServer();
        // start remoting client
        startClient();
    }

    /**
     * start the remoting server
     */
    private void startServer() {
        NettyRequestProcessor protocolProcessor = new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                return NettyRpcService.this.processRequest(ctx, request);
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        };
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(Integer.valueOf(memberState.getSelfAddr().split(":")[1]));
        this.remotingServer = new NettyRemotingServer(nettyServerConfig, null);
        this.remotingServer.registerProcessor(RequestCode.METADATA.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(RequestCode.APPEND.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(RequestCode.GET.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(RequestCode.PULL.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(RequestCode.PUSH.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(RequestCode.VOTE.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT.getCode(), protocolProcessor, null);

    }

    /**
     * start the remoting client
     */
    private void startClient() {
        this.remotingClient = new NettyRemotingClient(new NettyClientConfig(), null);
    }

    private void writeResponse() {}

    private RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        RequestCode code = RequestCode.valueOf(request.getCode());
        switch (code) {
            case UNKNOWN:
                break;
            case METADATA: {
                MetadataRequest metaDataRequest = JSON.parseObject(request.getBody(), MetadataRequest.class);
                CompletableFuture<MetadataResponse> future = handleMetadata(metaDataRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case APPEND:
                break;
            case GET:
                break;
            case VOTE:
                break;
            case HEART_BEAT:
                break;
            case PULL:
                break;
            case PUSH:
                break;
        }
    }

    @Override
    public CompletableFuture<VoteResponse> vote(VoteRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<PushEntryResponse> push(PushEntryRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {

    }

    @Override
    public CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        return null;
    }

    @Override
    public void startup() {
        this.remotingServer.start();
        this.remotingClient.start();
    }

    @Override
    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingClient.shutdown();
    }
}
