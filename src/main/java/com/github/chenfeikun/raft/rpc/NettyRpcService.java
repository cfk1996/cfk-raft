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
import com.github.chenfeikun.raft.rpc.entity.RequestOrResponse;
import com.github.chenfeikun.raft.rpc.entity.ResponseCode;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @desciption: NettyRpcService
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class NettyRpcService extends RpcService {

    private static final Logger LOG = LoggerFactory.getLogger(NettyRpcService.class);

    /** raft*/
    private NodeServer nodeServer;
    private MemberState memberState;
    /** rpc*/
    private NettyRemotingServer remotingServer;
    private NettyRemotingClient remotingClient;

    /** executor*/
    private ExecutorService futureExecutor = Executors.newFixedThreadPool(4, new ThreadFactory() {
        private AtomicInteger threadIndex = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "FutureExecutor_" + this.threadIndex.incrementAndGet());
        }
    });

    public NettyRpcService(NodeServer server) {
        this.nodeServer = server;
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

    public RemotingCommand handleResponse(RequestOrResponse body, RemotingCommand request) {
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS.getCode(), null);
        remotingCommand.setBody(JSON.toJSONBytes(body));
        remotingCommand.setOpaque(request.getOpaque());
        return remotingCommand;
    }

    private void writeResponse(RequestOrResponse body, Throwable t, RemotingCommand request, ChannelHandlerContext ctx) {
        RemotingCommand response = null;
        try {
            if (t != null) {
                throw t;
            } else {
                response = handleResponse(body, request);
                response.markResponseType();
                ctx.writeAndFlush(response);
            }
        } catch (Throwable e) {
            LOG.error("Process request over, but failed to get response. Request={},Response={}", request, response);
        }
    }

    private RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        RequestCode code = RequestCode.valueOf(request.getCode());
        switch (code) {
            case METADATA: {
                MetadataRequest metadataRequest = JSON.parseObject(request.getBody(), MetadataRequest.class);
                CompletableFuture<MetadataResponse> future = handleMetadata(metadataRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case APPEND: {
                AppendEntryRequest appendEntryRequest = JSON.parseObject(request.getBody(), AppendEntryRequest.class);
                CompletableFuture<AppendEntryResponse> future = handleAppend(appendEntryRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case GET: {
                GetEntriesRequest getEntriesRequest = JSON.parseObject(request.getBody(), GetEntriesRequest.class);
                CompletableFuture<GetEntriesResponse> future = handleGet(getEntriesRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case PULL: {
                PullEntriesRequest pullEntriesRequest = JSON.parseObject(request.getBody(), PullEntriesRequest.class);
                CompletableFuture<PullEntriesResponse> future = handlePull(pullEntriesRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case PUSH: {
                PushEntryRequest pushEntryRequest = JSON.parseObject(request.getBody(), PushEntryRequest.class);
                CompletableFuture<PushEntryResponse> future = handlePush(pushEntryRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case VOTE: {
                VoteRequest voteRequest = JSON.parseObject(request.getBody(), VoteRequest.class);
                CompletableFuture<VoteResponse> future = handleVote(voteRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case HEART_BEAT: {
                HeartBeatRequest heartBeatRequest = JSON.parseObject(request.getBody(), HeartBeatRequest.class);
                CompletableFuture<HeartBeatResponse> future = handleHeartBeat(heartBeatRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            default:
                LOG.error("Unknown request code {} from {}", request.getCode(), request);
                break;
        }
        return null;
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
    public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        return null;
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
    public CompletableFuture<PushEntryResponse> push(PushEntryRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        return nodeServer.handleVote(request);
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        return nodeServer.handleHeartBeat(request);
    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
        return nodeServer.handlePull(request);
    }

    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return nodeServer.handlePush(request);
    }

    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws Exception {
        return nodeServer.handleAppend(request);
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        return nodeServer.handleGet(request);
    }

    @Override
    public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        return nodeServer.handleMetadata(request);
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
