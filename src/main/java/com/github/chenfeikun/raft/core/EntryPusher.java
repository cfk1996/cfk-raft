package com.github.chenfeikun.raft.core;

import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.concurrent.TimeoutFuture;
import com.github.chenfeikun.raft.rpc.RpcService;
import com.github.chenfeikun.raft.rpc.entity.PushEntryRequest;
import com.github.chenfeikun.raft.rpc.entity.PushEntryResponse;
import com.github.chenfeikun.raft.rpc.entity.ResponseCode;
import com.github.chenfeikun.raft.store.RaftStore;
import com.github.chenfeikun.raft.utils.Pair;
import com.github.chenfeikun.raft.utils.PreConditions;
import com.github.chenfeikun.raft.utils.ShutdownableThread;
import com.github.chenfeikun.raft.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @desciption: EntryPusher
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class EntryPusher implements LifeCycle {

    private static final Logger LOG = LoggerFactory.getLogger(EntryPusher.class);

    private NodeConfig nodeConfig;
    private MemberState memberState;
    private RaftStore raftStore;
    private RpcService rpcService;

    private Map<String, EntryDispatcher> dispatcherMap = new HashMap<>();

    private Map<Long, ConcurrentMap<String, Long>> peerWaterMarksByTerm = new ConcurrentHashMap<>();

    /** backend handler*/
    private EntryHandler entryHandler = new EntryHandler(LOG);
    private AckChecker ackChecker = new AckChecker(LOG);

    public EntryPusher(NodeConfig config, MemberState memberState, RaftStore store, RpcService rpcService) {
        this.nodeConfig = config;
        this.memberState = memberState;
        this.raftStore = store;
        this.rpcService = rpcService;
        for (String peer : memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                dispatcherMap.put(peer, new EntryDispatcher(peer, LOG));
            }
        }
    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }

    /**
     * This thread will be activated by the follower.
     * Accept the push request and order it by the index, then append to ledger store one by one.
     */
    private class EntryHandler extends ShutdownableThread {

        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();

        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> compareOrTruncateRequests = new ArrayBlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>(100);


        public EntryHandler(Logger LOG) {
            super("EntryHandler", LOG);
        }

        @Override
        public void dowork() {
            try {
                if (!memberState.isLeader()) {
                    waitForRunning(1);
                    return;
                }
                if (compareOrTruncateRequests.peek() != null) {
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, ResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {
                        case TRUNCATE:
                            handleTruncate(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE:
                            handleCompare(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                } else {
                    long nextIndex = raftStore.getEndIndex() + 1;
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                    if (pair != null) {
                        checkAbnormalFuture(raftStore.getEndIndex());
                        waitForRunning(1);
                        return;
                    }
                    PushEntryRequest request = pair.getKey();
                    handleAppend(nextIndex, request, pair.getValue());
                }

            } catch (InterruptedException e) {
                 EntryPusher.LOG.error("error in {}", getName(), e);
                 Utils.sleep(100);
            }
        }

        private void handleAppend(long nextIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(nextIndex == request.getEntry().getIndex(), ResponseCode.INCONSISTENT_INDEX);
                Entry entry = raftStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(entry.getIndex() == nextIndex, ResponseCode.INCONSISTENT_INDEX);
                future.complete(buildResponse(request, ResponseCode.SUCCESS.getCode()));
                raftStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                LOG.error("handle append error, nextIndex=[{}]", nextIndex, t);
                future.complete(buildResponse(request, ResponseCode.INCONSISTENT_INDEX.getCode()));
            }
        }

        private void handleCompare(long index, PushEntryRequest key, CompletableFuture<PushEntryResponse> value) {
        }

        public CompletableFuture<PushEntryResponse> handleCommit(long commitIndex, PushEntryRequest request,
                                                                 CompletableFuture<PushEntryResponse> future) {
//                PreConditions.check(commitIndex == request.getCommitIndex(), ResponseCode.UNKNOWN);
            raftStore.updateCommittedIndex(request.getTerm(), commitIndex);
            future.complete(buildResponse(request, ResponseCode.SUCCESS.getCode()));
            return future;
        }

        public CompletableFuture<PushEntryResponse> handleTruncate(long index, PushEntryRequest request,
                                                                   CompletableFuture<PushEntryResponse> future) {
            return null;
        }



        /**
         * The leader does push entries to follower, and record the pushed index. But in the following conditions, the push may get stopped.
         *   * If the follower is abnormally shutdown, its ledger end index may be smaller than before. At this time, the leader may push fast-forward entries, and retry all the time.
         *   * If the last ack is missed, and no new message is coming in.The leader may retry push the last message, but the follower will ignore it.
         * @param endIndex
         */
        private void checkAbnormalFuture(long endIndex) {
            if (Utils.elapsed(lastCheckFastForwardTimeMs) < 1000) {
                return;
            }
            lastCheckFastForwardTimeMs  = System.currentTimeMillis();
            if (writeRequestMap.isEmpty()) {
                return;
            }
            long minFastForwardIndex = Long.MAX_VALUE;
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                long index = pair.getKey().getEntry().getIndex();
                //Fall behind
                if (index <= endIndex) {
                    try {
                        Entry local = raftStore.get(index);
                        PreConditions.check(pair.getKey().getEntry().equals(local), ResponseCode.INCONSISTENT_STATE);
                        pair.getValue().complete(buildResponse(pair.getKey(), ResponseCode.SUCCESS.getCode()));
                        LOG.warn("[PushFallBehind]The leader pushed an entry index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", index, endIndex);
                    } catch (Throwable t) {
                        LOG.error("[PushFallBehind]The leader pushed an entry index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", index, endIndex, t);
                        pair.getValue().complete(buildResponse(pair.getKey(), ResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    writeRequestMap.remove(index);
                    continue;
                }
                //Just OK
                if (index ==  endIndex + 1) {
                    //The next entry is coming, just return
                    return;
                }
                //Fast forward
                TimeoutFuture<PushEntryResponse> future  = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                if (index < minFastForwardIndex) {
                    minFastForwardIndex = index;
                }
            }
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.get(minFastForwardIndex);
            if (pair == null) {
                return;
            }
            LOG.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            pair.getValue().complete(buildResponse(pair.getKey(), ResponseCode.INCONSISTENT_STATE.getCode()));
        }

        /**
         * TODO：　is response correct? Should it get its term via localServer's term instead of request's term?
         */
        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                response.setIndex(request.getEntry().getIndex());
            }
            response.setBeginIndex(raftStore.getBeginIndex());
            response.setEndIndex(raftStore.getEndIndex());
            return response;
        }
    }

    /**
     * This thread will be activated by the leader.
     * This thread will push the entry to follower(identified by peerId) and update the completed pushed index to index map.
     * Should generate a single thread for each peer.
     * The push has 4 types:
     *   APPEND : append the entries to the follower
     *   COMPARE : if the leader changes, the new leader should compare its entries to follower's
     *   TRUNCATE : if the leader finished comparing by an index, the leader will send a request to truncate the follower's ledger
     *   COMMIT: usually, the leader will attach the committed index with the APPEND request, but if the append requests are few and scattered,
     *           the leader will send a pure request to inform the follower of committed index.
     *
     *   The common transferring between these types are as following:
     *
     *   COMPARE ---- TRUNCATE ---- APPEND ---- COMMIT
     *   ^                             |
     *   |---<-----<------<-------<----|
     *
     */
    private class EntryDispatcher extends ShutdownableThread {

        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.COMPARE);
        private String peerId;
        private long compareIndex = -1;
        private long writeIndex = -1;
        private ConcurrentMap<Long, Long> pendingMap = new ConcurrentHashMap<>();
        private long term = -1;
        private String leaderId = null;

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-"+ memberState.getSelfId()+"-"+peerId, logger);
            this.peerId = peerId;
        }

        private boolean checkAndFreshState() {
            if (!memberState.isLeader()) {
                return false;
            }
            if (term != memberState.getCurrTerm() || leaderId == null || !leaderId.equals(memberState.getLeaderId())) {
                synchronized (memberState) {
                    if (!memberState.isLeader()) {
                        return false;
                    }
                    PreConditions.check(memberState.getSelfId().equals(memberState.getLeaderId()), ResponseCode.UNKNOWN);
                    term = memberState.getCurrTerm();
                    leaderId = memberState.getLeaderId();
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                }
            }
            return true;
        }

        private synchronized void changeState(long index, PushEntryRequest.Type target) {
            LOG.info("state change from [{}] to [{}]", type.get(), target);
            switch (target) {
                case APPEND:
                    compareIndex = -1;
                    updatePeerWaterMark(term, peerId, index);
                    ackChecker.wakeup();
                    writeIndex = index + 1;
                    break;
                case COMPARE:
                    if (this.type.compareAndSet(PushEntryRequest.Type.APPEND, PushEntryRequest.Type.COMPARE)) {
                        compareIndex = -1;
                        pendingMap.clear();
                    }
                    break;
                case TRUNCATE:
                    compareIndex = -1;
                    break;
                default:
                    break;
            }
            type.set(target);
        }

        private void updatePeerWaterMark(long term, String peerId, long index) {
            synchronized (peerWaterMarksByTerm) {
                checkTermForWaterMark(term, "update water mark");
                if 
            }
        }

        @Override
        public void dowork() {
            try {
                if (!checkAndFreshState()) {
                    waitForRunning(1);
                    return;
                }
                if (type.get() == PushEntryRequest.Type.APPEND) {
                    doAppend();
                } else {
                    doCompare();
                }
                waitForRunning(1);
            } catch (Throwable t) {
                LOG.error("entry dispatcher error.", t);
                Utils.sleep(500);
            }
        }
    }
}
