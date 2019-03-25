package com.github.chenfeikun.raft.core;

import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.concurrent.TimeoutFuture;
import com.github.chenfeikun.raft.rpc.RpcService;
import com.github.chenfeikun.raft.rpc.entity.PushEntryRequest;
import com.github.chenfeikun.raft.rpc.entity.PushEntryResponse;
import com.github.chenfeikun.raft.rpc.entity.ResponseCode;
import com.github.chenfeikun.raft.store.RaftStore;
import com.github.chenfeikun.raft.store.file.MmapFileStore;
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
                LOG.error("entry dispatcher error.", t);
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
                    doTruncate();
                    break;
                }

            }
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
}
