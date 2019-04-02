package com.github.chenfeikun.raft.core;

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
import com.github.chenfeikun.raft.rpc.entity.VoteRequest;
import com.github.chenfeikun.raft.rpc.entity.VoteResponse;


import java.util.concurrent.CompletableFuture;

/**
 * @desciption: RaftProtocolHander
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public interface RaftProtocolHander {

    CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception;

    CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception;

//    CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception;

    CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception;

    /** client*/
    CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws Exception;

    CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception;

    CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception;
}
