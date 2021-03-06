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
        this.remotingServer.registerProcessor(RequestCode.PUSH.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(RequestCode.VOTE.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT.getCode(), protocolProcessor, null);
        // register a backdoor for raft dashboard
        NettyRequestProcessor backdoor = new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                return NettyRpcService.this.processBackdoor(ctx, request);
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        };
        this.remotingServer.registerProcessor(RequestCode.GET_SINGLE.getCode(), backdoor, null);
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

    private RemotingCommand processBackdoor(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
         RequestCode code = RequestCode.valueOf(request.getCode());
         switch (code) {
             case GET_SINGLE:
                 GetEntriesRequest getEntriesRequest = JSON.parseObject(request.getBody(), GetEntriesRequest.class);
                 CompletableFuture<GetEntriesResponse> future = handleGetSingle(getEntriesRequest);
                 future.whenCompleteAsync((x, y) -> {
                     writeResponse(x, y, request, ctx);
                 }, futureExecutor);
                 break;
             default:
                 LOG.error("error request code");
                 break;
         }
         return null;
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
//            case PULL: {
//                PullEntriesRequest pullEntriesRequest = JSON.parseObject(request.getBody(), PullEntriesRequest.class);
//                CompletableFuture<PullEntriesResponse> future = handlePull(pullEntriesRequest);
//                future.whenCompleteAsync((x, y) -> {
//                    writeResponse(x, y, request, ctx);
//                }, futureExecutor);
//                break;
//            }
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

    private String getPeerAddr(RequestOrResponse request) {
        return memberState.getPeerAddr(request.getRemoteId());
    }

    @Override
    public CompletableFuture<VoteResponse> vote(VoteRequest request) throws Exception {
        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.VOTE.getCode(), null);
            requestCommand.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request), requestCommand, 3000, responseFuture -> {
                VoteResponse response = JSON.parseObject(responseFuture.getResponseCommand().getBody(), VoteResponse.class);
                future.complete(response);
            });
        } catch (Throwable t) {
            LOG.error("send vote request error. request info = {}", request.baseInfo(), t);
            future.complete(new VoteResponse());
        }
        return future;
    }

    @Override
    public CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request) throws Exception {
        CompletableFuture<HeartBeatResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT.getCode(), null);
            remotingCommand.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request), remotingCommand, 3000, responseFuture -> {
                HeartBeatResponse response = JSON.parseObject(responseFuture.getResponseCommand().getBody(), HeartBeatResponse.class);
                future.complete(response);
            });
        } catch (Throwable t) {
            LOG.error("send heartbeat request failed. Request = {}", request.baseInfo(), t);
            future.complete(new HeartBeatResponse().code(ResponseCode.NETWORK_ERROR.getCode()));
        }
        return future;
    }

//    @Override
//    public CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request) throws Exception {
//        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL.getCode(), null);
//        remotingCommand.setBody(JSON.toJSONBytes(request));
//        RemotingCommand remotingResponse = remotingClient.invokeSync(getPeerAddr(request), remotingCommand, 3000);
//        PullEntriesResponse response = JSON.parseObject(remotingResponse.getBody(), PullEntriesResponse.class);
//        return CompletableFuture.completedFuture(response);
//    }

    @Override
    public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        GetEntriesResponse response = new GetEntriesResponse();
        // TODO
        return null;
    }

    @Override
    public CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception {
        CompletableFuture<AppendEntryResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(RequestCode.APPEND.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                AppendEntryResponse response = JSON.parseObject(responseFuture.getResponseCommand().getBody(), AppendEntryResponse.class);
                future.complete(response);
            });
        } catch (Throwable t) {
            LOG.error("send append request failed. Request = {}", request.baseInfo(), t);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.code(ResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }
        return future;
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception {
        // TODO
        return null;
    }

    @Override
    public CompletableFuture<PushEntryResponse> push(PushEntryRequest request) throws Exception {
        CompletableFuture<PushEntryResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(RequestCode.PUSH.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                PushEntryResponse response = JSON.parseObject(responseFuture.getResponseCommand().getBody(), PushEntryResponse.class);
                future.complete(response);
            });
        } catch (Throwable t) {
            LOG.error("send push request failed. Request = {}", request, t);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(ResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }
        return future;
    }

    public CompletableFuture<GetEntriesResponse> handleGetSingle(GetEntriesRequest request) throws Exception {
        return nodeServer.handleGetSingle(request);
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        return nodeServer.handleVote(request);
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        return nodeServer.handleHeartBeat(request);
    }

//    @Override
//    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
//        return nodeServer.handlePull(request);
//    }

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
