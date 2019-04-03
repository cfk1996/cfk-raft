package com.github.chenfeikun.raft.cmdClient;

import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.rpc.entity.*;
import com.github.chenfeikun.raft.utils.ShutdownableThread;
import com.github.chenfeikun.raft.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @desciption: Client
 * @CreateTime: 2019-04-02
 * @author: chenfeikun
 */
public class Client implements LifeCycle {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private final Map<String, String> peerMap = new ConcurrentHashMap<>();
    private String group;
    private ClientRpcService clientRpcService;
    private String leaderId;

    private MetadataUpdater metadataUpdater = new MetadataUpdater("metadata updater", logger);

    public Client(String group, String peers) {
        this.group = group;
        updatePeers(peers);
        this.clientRpcService = new NettyClientRpcService();
        this.clientRpcService.updatePeers(peers);
        leaderId = peerMap.keySet().iterator().next();
    }

    private void updatePeers(String peers) {
        for (String peerInfo : peers.split(";")) {
            peerMap.put(peerInfo.split("-")[0], peerInfo.split("-")[1]);
        }
    }

    public AppendEntryResponse append(byte[] body) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                AppendEntryResponse response = new AppendEntryResponse();
                response.setCode(ResponseCode.METADATA_ERROR.getCode());
                return response;
            }
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup(group);
            request.setRemoteId(leaderId);
            request.setBody(body);
            AppendEntryResponse response = clientRpcService.append(request).get();
            if (response.getCode() == ResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    request.setRemoteId(leaderId);
                    response = clientRpcService.append(request).get();
                }
            }
            return response;
        } catch (Exception e) {
            needFreshMetadata();
            logger.error("append error", e);
            AppendEntryResponse response = new AppendEntryResponse();
            response.setCode(ResponseCode.INTERNAL_ERROR.getCode());
            return response;
        }
    }

    public GetEntriesResponse get(long index) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                GetEntriesResponse response = new GetEntriesResponse();
                response.setCode(ResponseCode.METADATA_ERROR.getCode());
                return response;
            }

            GetEntriesRequest request = new GetEntriesRequest();
            request.setGroup(group);
            request.setRemoteId(leaderId);
            request.setBeginIndex(index);
            GetEntriesResponse response = clientRpcService.get(request).get();
            if (response.getCode() == ResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    request.setRemoteId(leaderId);
                    response = clientRpcService.get(request).get();
                }
            }
            return response;
        } catch (Exception t) {
            needFreshMetadata();
            logger.error("", t);
            GetEntriesResponse getEntriesResponse = new GetEntriesResponse();
            getEntriesResponse.setCode(ResponseCode.INTERNAL_ERROR.getCode());
            return getEntriesResponse;
        }
    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }

    private synchronized void needFreshMetadata() {
        leaderId = null;
        metadataUpdater.wakeup();
    }

    private synchronized void waitOnUpdatingMetadata(long maxWaitMs, boolean needFresh) {
        if (needFresh) {
            leaderId = null;
        } else if (leaderId != null) {
            return;
        }
        long start = System.currentTimeMillis();
        while (Utils.elapsed(start) < maxWaitMs && leaderId == null) {
            metadataUpdater.wakeup();
            try {
                wait(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public MetadataResponse metadata(String peerId) {
        try {
            MetadataRequest request = new MetadataRequest();
            request.setGroup(group);
            request.setRemoteId(peerId);
            CompletableFuture<MetadataResponse> future = clientRpcService.metadata(request);
            MetadataResponse response = future.get(1500, TimeUnit.MILLISECONDS);
            ;
        } catch (Exception t) {
            needFreshMetadata();
            logger.error("", t);
            GetEntriesResponse getEntriesResponse = new GetEntriesResponse();
            getEntriesResponse.setCode(ResponseCode.INTERNAL_ERROR.getCode());
            return getEntriesResponse;
        }
    }

    private class MetadataUpdater extends ShutdownableThread {

        public MetadataUpdater(String name, Logger logger) {
            super(name, logger);
        }

        private void getMetadata(String peerId, boolean isLeader) {
            try {
                MetadataRequest request = new MetadataRequest();
                request.setGroup(group);
                request.setRemoteId(peerId);
                CompletableFuture<MetadataResponse> future = clientRpcService.metadata(request);
                MetadataResponse response = future.get(1500, TimeUnit.MILLISECONDS);
                if (response.getLeaderId() != null) {
                    leaderId = response.getLeaderId();
                    if (response.getPeers() != null) {
                        peerMap.putAll(response.getPeers());
                        clientRpcService.updatePeers(response.getPeers());
                    }
                }
            } catch (Throwable t) {
                if (isLeader) {
                    needFreshMetadata();
                }
                logger.warn("Get metadata failed from {}", peerId, t);
            }

        }

        @Override
        public void dowork() {
            try {
                if (leaderId == null) {
                    for (String peer : peerMap.keySet()) {
                        getMetadata(peer, false);
                        if (leaderId != null) {
                            synchronized (Client.this) {
                                Client.this.notifyAll();
                            }
                            Utils.sleep(1000);
                            break;
                        }
                    }
                } else {
                    getMetadata(leaderId, true);
                }
                waitForRunning(3000);
            } catch (Throwable t) {
                logger.error("Error", t);
                Utils.sleep(1000);
            }
        }
    }
}
