package com.github.chenfeikun.raft;

import com.github.chenfeikun.raft.core.EntryPusher;
import com.github.chenfeikun.raft.core.LeaderElection;
import com.github.chenfeikun.raft.core.MemberState;
import com.github.chenfeikun.raft.core.RaftProtocolHander;
import com.github.chenfeikun.raft.exception.RaftException;
import com.github.chenfeikun.raft.rpc.NettyRpcService;
import com.github.chenfeikun.raft.rpc.RpcService;
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
import com.github.chenfeikun.raft.rpc.entity.ResponseCode;
import com.github.chenfeikun.raft.rpc.entity.VoteRequest;
import com.github.chenfeikun.raft.rpc.entity.VoteResponse;
import com.github.chenfeikun.raft.store.memory.MemoryStore;
import com.github.chenfeikun.raft.store.file.MmapFileStore;
import com.github.chenfeikun.raft.store.RaftStore;
import com.github.chenfeikun.raft.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * @desciption: NodeServer
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class NodeServer implements LifeCycle, RaftProtocolHander {

    private static final Logger LOG = LoggerFactory.getLogger(NodeServer.class);

    private MemberState memberState;
    private NodeConfig config;

    private RaftStore store;
    private RpcService rpcService;
    private EntryPusher entryPusher;
    private LeaderElection leaderElection;

    public NodeServer(NodeConfig config) {
        this.config = config;
        this.memberState = new MemberState(config);
        this.store = createStore(this.config.getStoreType(), this.config, memberState);
        this.rpcService = new NettyRpcService(this);
        this.entryPusher = new EntryPusher(config, memberState, store, rpcService);
        this.leaderElection = new LeaderElection(config, memberState, rpcService);
    }


    private RaftStore createStore(String type, NodeConfig config, MemberState state) {
        if (type.equals(NodeConfig.MEMORY)) {
            return new MemoryStore(config, memberState);
        } else {
            return new MmapFileStore(config, memberState);
        }
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
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()),
                    ResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()),
                    ResponseCode.UNKNOWN_GROUP,
                    "%s != %s", request.getCode(), memberState.getGroup());
            PreConditions.check(memberState.isLeader(), ResponseCode.NOT_LEADER);
            long currentTerm = memberState.getCurrTerm();
            if (entryPusher.isPendingFull(currentTerm)) {

            } else {

            }
        }
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        try {
            /** preconditions check*/
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()),
                    ResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()),
                    ResponseCode.UNKNOWN_GROUP,
                    "%s != %s", request.getCode(), memberState.getGroup());
            MetadataResponse response = new MetadataResponse();
            response.setGroup(memberState.getGroup());
            response.setLeaderId(memberState.getLeaderId());
            response.setPeers(memberState.getPeerMap());
            return CompletableFuture.completedFuture(response);
        } catch (RaftException e) {
            LOG.error("{} handle metadata failed", memberState.getSelfId(), e);
            MetadataResponse response = new MetadataResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public void startup() {
        this.store.startup();
        this.rpcService.startup();
        this.entryPusher.startup();
        this.leaderElection.startup();
    }

    @Override
    public void shutdown() {
        this.store.shutdown();
        this.rpcService.shutdown();
        this.entryPusher.shutdown();
        this.leaderElection.shutdown();
    }

    public MemberState getMemberState() {
        return memberState;
    }

    public void setMemberState(MemberState memberState) {
        this.memberState = memberState;
    }

    public NodeConfig getConfig() {
        return config;
    }

    public void setConfig(NodeConfig config) {
        this.config = config;
    }

    public RaftStore getStore() {
        return store;
    }

    public void setStore(RaftStore store) {
        this.store = store;
    }

    public RpcService getRpcService() {
        return rpcService;
    }

    public void setRpcService(RpcService rpcService) {
        this.rpcService = rpcService;
    }
}
