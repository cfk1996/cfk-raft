package com.github.chenfeikun.raft.core;

import com.github.chenfeikun.raft.rpc.entity.*;

import java.util.concurrent.CompletableFuture;

/**
 * @desciption: RaftProtocol
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public interface RaftProtocol {
    CompletableFuture<VoteResponse> vote(VoteRequest request) throws Exception;

    CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request) throws Exception;

    CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request) throws Exception;

    CompletableFuture<PushEntryResponse> push(PushEntryRequest request) throws Exception;

    /** client */
    CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception;

    CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception;

    CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception;

}
