package com.github.chenfeikun.raft;

import com.github.chenfeikun.raft.concurrent.AppendFuture;
import com.github.chenfeikun.raft.core.Entry;
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

import java.util.Collections;
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

    private RaftStore raftStore;
    private RpcService rpcService;
    private EntryPusher entryPusher;
    private LeaderElection leaderElection;

    public NodeServer(NodeConfig config) {
        this.config = config;
        this.memberState = new MemberState(config);
        this.raftStore = createStore(this.config.getStoreType(), this.config, memberState);
        this.rpcService = new NettyRpcService(this);
        this.entryPusher = new EntryPusher(config, memberState, raftStore, rpcService);
        this.leaderElection = new LeaderElection(config, memberState, rpcService);
    }

    @Override
    public void startup() {
        this.raftStore.startup();
        this.rpcService.startup();
        this.entryPusher.startup();
        this.leaderElection.startup();
    }

    @Override
    public void shutdown() {
        this.raftStore.shutdown();
        this.rpcService.shutdown();
        this.entryPusher.shutdown();
        this.leaderElection.shutdown();
    }

    private RaftStore createStore(String type, NodeConfig config, MemberState state) {
        if (type.equals(NodeConfig.MEMORY)) {
            return new MemoryStore(config, state);
        } else {
            return new MmapFileStore(config, state);
        }
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), ResponseCode.UNKNOWN_MEMBER,
                    "%s != %s", memberState.getSelfId(), request.getRemoteId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), ResponseCode.UNKNOWN_GROUP,
                    "%s != %s", memberState.getGroup(), request.getGroup());
            return leaderElection.handleVote(request, false);
        } catch (RaftException e) {
            LOG.error("[{}} handle vote failed.", memberState.getSelfId(), e);
            VoteResponse response = new VoteResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), ResponseCode.UNKNOWN_MEMBER);
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), ResponseCode.UNKNOWN_GROUP);
            return leaderElection.handleHeartBeat(request);
        } catch (RaftException e) {
            LOG.error("[{}] handle heart beat failed. Request = {}", memberState.getSelfId(), request.baseInfo());
            HeartBeatResponse response = new HeartBeatResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

//    @Override
//    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
//        return null;
//    }

    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), ResponseCode.UNKNOWN_MEMBER);
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), ResponseCode.UNKNOWN_GROUP);
            return entryPusher.handlePush(request);
        } catch (RaftException e) {
            LOG.error("[{}] handle push failed.", memberState.getSelfId(), e);
            PushEntryResponse responses = new PushEntryResponse();
            responses.copyBaseInfo(request);
            responses.setCode(e.getCode().getCode());
            responses.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(responses);
        }
    }


    /**
     * Handle the append request.
     * 1. append the entry to local store
     * 2. submit the future to entrypusher and wait the quorum ack
     * 3. if the pending requests are full, then reject it immediately
     * @param request
     * @return
     * @throws Exception
     */
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
                AppendEntryResponse response = new AppendEntryResponse();
                response.setGroup(memberState.getGroup());
                response.setCode(ResponseCode.LEADER_PENDING_FULL.getCode());
                response.setTerm(currentTerm);
                response.setLeaderId(memberState.getSelfId());
                return AppendFuture.newCompletedFuture(-1, response);
            } else {
                Entry entry = new Entry();
                entry.setBody(request.getBody());
                Entry ackEntry = raftStore.appendAsLeader(entry);
                return entryPusher.waitAck(ackEntry);
            }
        } catch (RaftException e) {
            LOG.error("handle append error. id={}", memberState.getSelfId(), e);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), ResponseCode.UNKNOWN_MEMBER,
                    "%s != %s", memberState.getSelfId(), request.getRemoteId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), ResponseCode.UNKNOWN_GROUP,
                    "%s != %s", memberState.getGroup(), request.getGroup());
            PreConditions.check(memberState.isLeader(), ResponseCode.NOT_LEADER);
            Entry entry = raftStore.get(request.getBeginIndex());
            GetEntriesResponse response = new GetEntriesResponse();
            response.setGroup(memberState.getGroup());
            if (entry != null) {
                response.setEntries(Collections.singletonList(entry));
            }
            return CompletableFuture.completedFuture(response);
        } catch (RaftException e) {
            LOG.error("[{}] handle get failed.", memberState.getSelfId(), e);
            GetEntriesResponse response = new GetEntriesResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
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
        return raftStore;
    }

    public void setStore(RaftStore store) {
        this.raftStore = store;
    }

    public RpcService getRpcService() {
        return rpcService;
    }

    public void setRpcService(RpcService rpcService) {
        this.rpcService = rpcService;
    }
}
