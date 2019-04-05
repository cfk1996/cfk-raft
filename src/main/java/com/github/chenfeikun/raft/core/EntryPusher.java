package com.github.chenfeikun.raft.core;

import com.alibaba.fastjson.JSON;
import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.concurrent.AppendFuture;
import com.github.chenfeikun.raft.concurrent.TimeoutFuture;
import com.github.chenfeikun.raft.rpc.RpcService;
import com.github.chenfeikun.raft.rpc.entity.AppendEntryResponse;
import com.github.chenfeikun.raft.rpc.entity.PushEntryRequest;
import com.github.chenfeikun.raft.rpc.entity.PushEntryResponse;
import com.github.chenfeikun.raft.rpc.entity.ResponseCode;
import com.github.chenfeikun.raft.store.RaftStore;
import com.github.chenfeikun.raft.store.memory.MemoryStore;
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
import java.util.concurrent.TimeUnit;
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


    private Map<Long, ConcurrentMap<String, Long>> peerWaterMarksByTerm = new ConcurrentHashMap<>();
    private Map<Long, ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>>> pendingAppendResponsesByTerm = new ConcurrentHashMap<>();

    /** backend handler*/
    private EntryHandler entryHandler = new EntryHandler(LOG);
    private AckChecker ackChecker = new AckChecker(LOG);
    private Map<String, EntryDispatcher> dispatcherMap = new HashMap<>();

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
        entryHandler.start();
        ackChecker.start();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.start();
        }
    }

    @Override
    public void shutdown() {
        entryHandler.shutdown();
        ackChecker.shutdown();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }

    public CompletableFuture<AppendEntryResponse> waitAck(Entry entry) {
        updatePeerWaterMark(entry.getTerm(), memberState.getSelfId(), entry.getIndex());
        if (memberState.getPeerMap().size() == 1) {
            AppendEntryResponse response = new AppendEntryResponse();
            response.setGroup(memberState.getGroup());
            response.setLeaderId(memberState.getSelfId());
            response.setIndex(entry.getIndex());
            response.setTerm(entry.getTerm());
            response.setPos(entry.getPos());
            return AppendFuture.newCompletedFuture(entry.getPos(), response);
        } else {
            checkTermForPendingMark(entry.getTerm(), "waitAck");
            AppendFuture<AppendEntryResponse> future = new AppendFuture<>(nodeConfig.getMaxWaitAckTimeMs());
            future.setPos(entry.getPos());
            CompletableFuture<AppendEntryResponse> old = pendingAppendResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), future);
            if (old != null) {
                LOG.warn("wait at old index = {}", entry.getIndex());
            }
            wakeupDispatchers();
            return future;
        }
    }

    public void wakeupDispatchers() {
        for (EntryDispatcher entryDispatcher : dispatcherMap.values()) {
            entryDispatcher.wakeup();
        }
    }

    public boolean isPendingFull(long term) {
        checkTermForPendingMark(term, "isPendingfull");
        return pendingAppendResponsesByTerm.get(term).size() > nodeConfig.getMaxPendingRequestsNum();
    }

    private void checkTermForWaterMark(long term, String msg) {
        if (!peerWaterMarksByTerm.containsKey(term)) {
            LOG.info("Initialize the watermark in {} for term={}", msg, term);
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            for (String peer : memberState.getPeerMap().keySet()) {
                waterMarks.put(peer, -1L);
            }
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }

    private void checkTermForPendingMark(long term, String msg) {
        if (!pendingAppendResponsesByTerm.containsKey(term)) {
            pendingAppendResponsesByTerm.putIfAbsent(term, new ConcurrentHashMap<>());
        }
    }

    private void updatePeerWaterMark(long term, String peerId, long index) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "update water mark");
            if (peerWaterMarksByTerm.get(term).get(peerId) < index) {
                peerWaterMarksByTerm.get(term).put(peerId, index);
            }
        }
    }

    private long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }

    /**
     * This thread will be activated by the follower.
     * Accept the push request and order it by the index, then append to ledger store one by one.
     */
    private class EntryHandler extends ShutdownableThread {

        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();

        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> compareOrTruncateRequests = new ArrayBlockingQueue<>(100);


        public EntryHandler(Logger LOG) {
            super("EntryHandler", LOG);
        }

        @Override
        public void dowork() {
            try {
                if (!memberState.isFollower()) {
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
                        case COMPARE:
                            handleCompare(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                } else {
                    long nextIndex = raftStore.getEndIndex() + 1;
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                    if (pair == null) {
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

        private CompletableFuture<PushEntryResponse> handleCompare(long compareIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(compareIndex == request.getEntry().getIndex(), ResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, ResponseCode.UNKNOWN);
                Entry local = raftStore.get(compareIndex);
                PreConditions.check(request.getEntry().equals(local), ResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, ResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                LOG.error("[{}] handle compare error.", memberState.getSelfId(), t);
                future.complete(buildResponse(request, ResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
            CompletableFuture<PushEntryResponse> future = new TimeoutFuture<>(1000);
            switch (request.getType()) {
                case APPEND:
                    PreConditions.check(request.getEntry() != null, ResponseCode.UNEXPECTED_ARGUMENT);
                    long index = request.getEntry().getIndex();
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> old = writeRequestMap.putIfAbsent(index, new Pair<>(request, future));
                    if (old != null) {
                        future.complete(buildResponse(request, ResponseCode.REPEATED_PUSH.getCode()));
                    }
                    break;
                case COMMIT:
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                case COMPARE:
                case TRUNCATE:
                    PreConditions.check(request.getEntry() != null, ResponseCode.UNEXPECTED_ARGUMENT);
                    writeRequestMap.clear();
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                default:
                    LOG.error("unknown type in handle push, type = {}", request.getType());
                    future.complete(buildResponse(request, ResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            return future;
        }

        public CompletableFuture<PushEntryResponse> handleCommit(long commitIndex, PushEntryRequest request,
                                                                 CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(commitIndex == request.getCommitIndex(), ResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, ResponseCode.UNKNOWN);
                raftStore.updateCommittedIndex(request.getTerm(), commitIndex);
                future.complete(buildResponse(request, ResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                LOG.error("[{}] handle commit error.", memberState.getSelfId(), t);
                future.complete(buildResponse(request, ResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }

        public CompletableFuture<PushEntryResponse> handleTruncate(long truncateIndex, PushEntryRequest request,
                                                                   CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(truncateIndex == request.getEntry().getIndex(), ResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, ResponseCode.UNKNOWN);
                long index = raftStore.truncate(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == truncateIndex, ResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, ResponseCode.SUCCESS.getCode()));
                raftStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                LOG.error("[{}] handleTruncate error.", memberState.getSelfId(), t);
                future.complete(buildResponse(request, ResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
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

        private long lastPushCommitTimeMs = -1;
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        private int maxPendingSize = 1000;

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
                LOG.error("[{}] .peer - [{}] entry dispatcher error.", memberState.getSelfId(), peerId, t);
                Utils.sleep(500);
            }
        }

        private void doCompare() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.COMPARE
                    && type.get() != PushEntryRequest.Type.TRUNCATE) {
                    break;
                }
                if (compareIndex == -1 && raftStore.getEndIndex() == -1) {
                    break;
                }
                if (compareIndex == -1) {
                    compareIndex = raftStore.getEndIndex();
                    LOG.info("[Push-{}][DoCompare] compareIndex=-1 means start to compare", peerId);
                } else if (compareIndex > raftStore.getEndIndex() || compareIndex < raftStore.getBeginIndex()) {
                    LOG.info("[Push-{}][DoCompare] compareIndex={} out of range {}-{}", peerId, compareIndex,
                            raftStore.getBeginIndex(), raftStore.getEndIndex());
                    compareIndex = raftStore.getBeginIndex();
                }

                Entry entry = raftStore.get(compareIndex);
                PreConditions.check(entry != null, ResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.COMPARE);
                CompletableFuture<PushEntryResponse> future = rpcService.push(request);
                PushEntryResponse response = future.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, ResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PreConditions.check(response.getCode() == ResponseCode.INCONSISTENT_STATE.getCode() ||
                        response.getCode() == ResponseCode.SUCCESS.getCode(),
                        ResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);
                long truncateIndex = -1;
                if (response.getCode() == ResponseCode.SUCCESS.getCode()) {
                    /*
                     * The comparison is successful.
                     * 1.just change to append state if the follower's end index is equal the compared index
                     * 2.Truncate the follower if it has dirty entries
                     */
                    if (compareIndex == response.getEndIndex()) {
                        changeState(compareIndex, PushEntryRequest.Type.APPEND);
                        break;
                    } else {
                        truncateIndex = compareIndex;
                    }
                } else if (response.getEndIndex() < raftStore.getBeginIndex() ||
                    response.getBeginIndex() > raftStore.getEndIndex()) {
                    /*
                     * The follower's entries dose not intersect with the leader.
                     * This usually happens when follower has crashed for a long time while leader deleted its expired entries.
                     * Just truncate the follower.
                     */
                    truncateIndex = raftStore.getBeginIndex();
                } else if (compareIndex < response.getBeginIndex()) {
                    truncateIndex = raftStore.getBeginIndex();
                } else if (compareIndex > raftStore.getEndIndex()) {
                    /*
                    the compareIndex is bigger than follower's end index
                    this is frequently because compareIndex is usually init at leader's end index.
                     */
                    compareIndex = raftStore.getBeginIndex();
                } else {
                    /*
                    Compare failed and the index is in the follower'ss range.
                    Decrease and retry.
                     */
                    compareIndex--;
                }

                if (compareIndex < raftStore.getBeginIndex()) {
                    truncateIndex = raftStore.getBeginIndex();
                }

                if (truncateIndex != -1) {
                    changeState(truncateIndex, PushEntryRequest.Type.TRUNCATE);
                    doTruncate(truncateIndex);
                    break;
                }
            }
        }

        private void doTruncate(long truncateIndex) throws Exception {
            PreConditions.check(type.get() == PushEntryRequest.Type.TRUNCATE, ResponseCode.UNKNOWN);
            Entry entry = raftStore.get(truncateIndex);
            PreConditions.check(entry != null, ResponseCode.UNKNOWN);
            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.TRUNCATE);
            PushEntryResponse response = rpcService.push(request).get(3, TimeUnit.SECONDS);
            PreConditions.check(response != null, ResponseCode.UNKNOWN);
            PreConditions.check(response.getCode() == ResponseCode.SUCCESS.getCode(), ResponseCode.valueOf(response.getCode()));
            lastPushCommitTimeMs = System.currentTimeMillis();
            changeState(truncateIndex, PushEntryRequest.Type.APPEND);
        }

        private void doAppend() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.APPEND) {
                    break;
                }
                if (writeIndex > raftStore.getEndIndex()) {
                    doCommit();
                    doCheckAppendResponse();
                    break;
                }
                if (pendingMap.size() >= maxPendingSize || Utils.elapsed(lastCheckLeakTimeMs) > 1000) {
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Long index : pendingMap.keySet()) {
                        if (index < peerWaterMark) {
                            pendingMap.remove(index);
                        }
                        lastCheckLeakTimeMs = System.currentTimeMillis();
                    }
                }
                if (pendingMap.size() >= maxPendingSize) {
                    doCheckAppendResponse();
                    break;
                }
                doAppendInner(writeIndex);
                writeIndex++;
            }
        }

        private void doCheckAppendResponse() throws Exception {
            long peerWaterMark = getPeerWaterMark(term, peerId);
            Long sendTimeMs = pendingMap.get(peerWaterMark+1);
            if (sendTimeMs != null && System.currentTimeMillis() - sendTimeMs > nodeConfig.getMaxPushTimeOutMs()) {
                LOG.warn("[Push-{}]Retry to push entry at {}", peerId, peerWaterMark + 1);
                doAppendInner(peerWaterMark+1);
            }
        }

        private void checkQuotaAndWait(Entry entry) {
            if (raftStore.getEndIndex() - entry.getIndex() <= maxPendingSize) {
                return;
            }
            if (raftStore instanceof MemoryStore) {
                return;
            }
            // TODO: MmapFileStore
            throw new IllegalArgumentException("mmap file not support");
        }

        private void doAppendInner(long index) throws Exception {
            Entry entry = raftStore.get(index);
            PreConditions.check(entry != null, ResponseCode.UNKNOWN, "write index=%d", index);
            checkQuotaAndWait(entry);
            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.APPEND);
            CompletableFuture<PushEntryResponse> future = rpcService.push(request);
            pendingMap.put(index, System.currentTimeMillis());
            future.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, ResponseCode.UNKNOWN);
                    ResponseCode code = ResponseCode.valueOf(x.getCode());
                    switch (code) {
                        case SUCCESS:
                            pendingMap.remove(x.getIndex());
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            ackChecker.wakeup();
                            break;
                        case INCONSISTENT_TERM:
                            LOG.info("get inconsisent state when push index={}, term = {}", x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            LOG.warn("error response code, index = {}, term = {}", x.getIndex(), x.getTerm());
                            break;
                    }
                } catch (Throwable t) {
                    LOG.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();

        }

        private void doCommit() throws Exception {
            if (Utils.elapsed(lastPushCommitTimeMs) > 1000) {
                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);
                rpcService.push(request);
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        private PushEntryRequest buildPushRequest(Entry entry, PushEntryRequest.Type type) {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setTerm(term);
            request.setEntry(entry);
            request.setType(type);
            request.setCommitIndex(raftStore.getCommittedIndex());
            return request;
        }
    }

    /**
     * Check the quorum index and complete the pending requests.
     */
    private class AckChecker extends ShutdownableThread {


        private long lastPrintWatermarkTimeMs = System.currentTimeMillis();
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        private long lastQuorumIndex = -1;


        public AckChecker(Logger LOG) {
            super("AckChecker", LOG);
        }

        @Override
        public void dowork() {
            try {
                if (Utils.elapsed(lastPrintWatermarkTimeMs) > 15000) {
                    LOG.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} watermarks={}",
                            memberState.getSelfId(), memberState.getRole(), memberState.getCurrTerm(),
                            raftStore.getBeginIndex(), raftStore.getEndIndex(), raftStore.getCommittedIndex(),
                            JSON.toJSONString(peerWaterMarksByTerm));
                    lastPrintWatermarkTimeMs = System.currentTimeMillis();
                }
                if (!memberState.isLeader()) {
                    waitForRunning(1000);
                    return;
                }
                long currTerm = memberState.getCurrTerm();
                checkTermForPendingMark(currTerm, "ackcheck");
                checkTermForWaterMark(currTerm, "ackcheck");
                if (pendingAppendResponsesByTerm.size() > 1) {
                    for (Long term : pendingAppendResponsesByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry :
                                pendingAppendResponsesByTerm.get(term).entrySet()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setIndex(futureEntry.getKey());
                            response.setCode(ResponseCode.TERM_CHANGED.getCode());
                            response.setLeaderId(memberState.getLeaderId());
                            futureEntry.getValue().complete(response);
                        }
                        pendingAppendResponsesByTerm.remove(term);
                    }
                }
                if (peerWaterMarksByTerm.size() > 1) {
                    for (Long term : peerWaterMarksByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        peerWaterMarksByTerm.remove(term);
                    }
                }
                Map<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);
                long quorumIndex = -1;
                for (Long index : peerWaterMarks.values()) {
                    int num = 0;
                    for (Long another : peerWaterMarks.values()) {
                        if (another >= index) {
                            num++;
                        }
                    }
                    if (memberState.isQuorum(num) && index > quorumIndex) {
                        quorumIndex = index;
                    }
                }
                raftStore.updateCommittedIndex(currTerm, quorumIndex);
                ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>> responses = pendingAppendResponsesByTerm.get(currTerm);
                boolean needCheck = false;
                int ackNum = 0;
                if (quorumIndex >= 0) {
                    for (Long i = quorumIndex; i >= 0 ; i--) {
                        try {
                            CompletableFuture<AppendEntryResponse> future = responses.remove(i);
                            if (future == null) {
                                needCheck = lastQuorumIndex != -1 && lastQuorumIndex != quorumIndex && i != lastQuorumIndex;
                                break;
                            } else if (!future.isDone()) {
                                AppendEntryResponse response = new AppendEntryResponse();
                                response.setGroup(memberState.getGroup());
                                response.setTerm(currTerm);
                                response.setIndex(i);
                                response.setLeaderId(memberState.getSelfId());
                                response.setPos(((AppendFuture) future).getPos());
                                future.complete(response);
                            }
                            ackNum++;
                        } catch (Throwable t) {
                            LOG.error("Error in ack to index = {},  term = {}", i, currTerm, t);
                        }
                    }
                }

                if (ackNum == 0) {
                    for (long i = quorumIndex + 1; i < Integer.MAX_VALUE; i++) {
                        TimeoutFuture<AppendEntryResponse> future = responses.get(i);
                        if (future == null) {
                            break;
                        } else if (future.isTimeOut()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setCode(ResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode());
                            response.setTerm(currTerm);
                            response.setIndex(i);
                            response.setLeaderId(memberState.getSelfId());
                            future.complete(response);
                        } else {
                            break;
                        }
                    }
                    waitForRunning(1);
                }

                if (Utils.elapsed(lastCheckLeakTimeMs) > 1000 || needCheck) {
                    updatePeerWaterMark(currTerm, memberState.getSelfId(), raftStore.getEndIndex());
                    for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : responses.entrySet()) {
                        if (futureEntry.getKey() < quorumIndex) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setTerm(currTerm);
                            response.setIndex(futureEntry.getKey());
                            response.setLeaderId(memberState.getSelfId());
                            response.setPos(((AppendFuture) futureEntry.getValue()).getPos());
                            futureEntry.getValue().complete(response);
                            responses.remove(futureEntry.getKey());
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
            } catch (Throwable t) {
                EntryPusher.LOG.error("error in {}", getName(), t);
                Utils.sleep(100);
            }
        }
    }
}
