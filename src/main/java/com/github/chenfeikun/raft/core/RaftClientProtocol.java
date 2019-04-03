package com.github.chenfeikun.raft.core;

import com.github.chenfeikun.raft.rpc.entity.*;

import java.util.concurrent.CompletableFuture;

/**
 * @desciption: RaftClientProtocol
 * @CreateTime: 2019-04-02
 * @author: chenfeikun
 */
public interface RaftClientProtocol {

    CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception;

    CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception;

    CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception;
}
