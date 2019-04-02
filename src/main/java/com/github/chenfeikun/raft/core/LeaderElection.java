package com.github.chenfeikun.raft.core;

import com.alibaba.fastjson.JSON;
import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.rpc.RpcService;
import com.github.chenfeikun.raft.rpc.entity.*;
import com.github.chenfeikun.raft.store.memory.MemoryStore;
import com.github.chenfeikun.raft.utils.ShutdownableThread;
import com.github.chenfeikun.raft.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @desciption: LeaderElection
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class LeaderElection implements LifeCycle {

    private static final Logger logger = LoggerFactory.getLogger(LeaderElection.class);

    private Random random = new Random();
    private NodeConfig nodeConfig;
    private MemberState memberState;
    private RpcService rpcService;

    /* Variables that record the last leader state as a server*/
    private long lastLeaderHeartBeatTime = -1;
    private long lastSendHeartBeatTime = -1;
    private long lastSuccHeartBeatTime = -1;
    private int heartBeatTimeIntervalMs = 2000;
    private int maxHeartBeatLeak = 3;
    /* as a client*/
    private long nextTimeToRequestVote = -1;
    private boolean needIncreaseTermImmediately = false;
    private int minVoteIntervalMs = 300;
    private int maxVoteIntervalMs = 1000;

    private long lastVoteCost = 0L;

    private List<RoleChangeHandler> roleChangeHandlers = new ArrayList<>();
    private VoteResponse.ParseResult lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;

    private StateMaintainer stateMaintainer = new StateMaintainer("state maintainer", logger);

    public LeaderElection(NodeConfig config, MemberState memberState, RpcService rpcService) {
        this.nodeConfig = config;
        this.memberState = memberState;
        this.rpcService = rpcService;
        refreshIntervals(this.nodeConfig);
    }

    @Override
    public void startup() {
        stateMaintainer.start();
        for (RoleChangeHandler handler : roleChangeHandlers) {
            handler.startup();
        }
    }

    @Override
    public void shutdown() {
        stateMaintainer.shutdown();
        for (RoleChangeHandler handler : roleChangeHandlers) {
            handler.shutdown();
        }
    }

    private void refreshIntervals(NodeConfig nodeConfig) {
        this.heartBeatTimeIntervalMs = nodeConfig.getHeartBeatTimeIntervalMs();
        this.maxHeartBeatLeak = nodeConfig.getMaxHeartBeatLeak();
        this.minVoteIntervalMs = nodeConfig.getMinVoteIntervalMs();
        this.maxVoteIntervalMs = nodeConfig.getMaxVoteIntervalMs();
    }


    /**The main method for StateMaintainer.
     * candidate ==> propose a vote
     * leader ==> send heartbeat to followers and step down to candidate when quorum followers do not respond
     * follower ==> accept heartbeat and change to candidate when not receive heartbeat form leader
     */
    private void maintainState() throws Exception {
        if (memberState.isLeader()) {
            maintainAsLeader();
        } else if (memberState.isFollower()) {
            maintainAsFollower();
        } else {
            maintainAsCandidate();
        }
    }

    private void maintainAsLeader() throws Exception {
        if (Utils.elapsed(lastSendHeartBeatTime) > heartBeatTimeIntervalMs) {
            long term;
            String leaderId;
            synchronized (memberState) {
                if (!memberState.isLeader()) {
                    return;
                }
                term = memberState.getCurrTerm();
                leaderId = memberState.getLeaderId();
                lastSendHeartBeatTime = System.currentTimeMillis();
            }
            sendHeartbeats(term, leaderId);
        }
    }

    private void sendHeartbeats(long term, String leaderId) throws Exception {
        final AtomicInteger allNum = new AtomicInteger(1);
        final AtomicInteger succNum = new AtomicInteger(1);
        final AtomicInteger notReadyNum = new AtomicInteger(0);
        final AtomicLong maxTerm = new AtomicLong(-1);
        final AtomicBoolean inconsistLeader = new AtomicBoolean(false);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        long startHeartBeatTimeMs = System.currentTimeMillis();
        for (String id : memberState.getPeerMap().keySet()) {
            if (memberState.getSelfId().equals(id)) {
                continue;
            }
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(memberState.getGroup());
            request.setLocalId(memberState.getSelfId());
            request.setRemoteId(id);
            request.setLeaderId(leaderId);
            request.setTerm(term);
            CompletableFuture<HeartBeatResponse> future = rpcService.heartBeat(request);
            future.whenComplete((HeartBeatResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }
                    switch (ResponseCode.valueOf(x.getCode())) {
                        case SUCCESS:
                            succNum.incrementAndGet();
                            break;
                        case EXPIRED_TERM:
                            maxTerm.set(x.getTerm());
                            break;
                        case INCONSISTENT_LEADER:
                            inconsistLeader.compareAndSet(false, true);
                            break;
                        case TERM_NOT_READY:
                            notReadyNum.incrementAndGet();
                            break;
                        default:
                            break;
                    }
                    if (memberState.isQuorum(succNum.get()) ||
                        memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                        countDownLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("parse heartbeat response error");
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        countDownLatch.countDown();
                    }
                }
            });
        }
        countDownLatch.await(heartBeatTimeIntervalMs, TimeUnit.MILLISECONDS);
        if (memberState.isQuorum(succNum.get())) {
            lastSuccHeartBeatTime = System.currentTimeMillis();
        } else {
            logger.info("[{}] Parse heartbeat responses in cost={} term={} allNum={} succNum={} notReadyNum={}" +
                    " inconsistLeader={} maxTerm={} peerSize={} lastSuccHeartBeatTime={}",
                    memberState.getSelfId(), Utils.elapsed(startHeartBeatTimeMs), term, allNum.get(), succNum.get(),
                    notReadyNum.get(), inconsistLeader.get(), maxTerm.get(), memberState.peerSize(), new Timestamp(lastSuccHeartBeatTime));
            if (memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                lastSendHeartBeatTime = System.currentTimeMillis();
            } else if (maxTerm.get() > term) {
                changeRoleToCandidate(maxTerm.get());
            } else if (inconsistLeader.get()) {
                changeRoleToCandidate(term);
            } else if (Utils.elapsed(lastSuccHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs) {
                changeRoleToCandidate(term);
            }
        }
    }

    private void changeRoleToFollower(long term, String leaderId) {
        logger.info("[{}] change role to follower. currTerm = {}, newTerm = {}, leaderId = {}", memberState.getCurrTerm(), term, leaderId);
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
//        handleRoleChange(term, MemberState.Role.FOLLOWER);
    }

    private void changeRoleToCandidate(long term) {
        synchronized (memberState) {
            if (term > memberState.getCurrTerm()) {
                memberState.changeToCandidate(term);
//                handleRoleChange(term, MemberState.Role.CANDIDATE);
                logger.info("[{}] [ChangeRoleToCandidate] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.getCurrTerm());
            }
        }
    }

//    private void handleRoleChange(long term, MemberState.Role role) {
//        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
//            try {
//                roleChangeHandler.handle(term, role);
//            } catch (Throwable t) {
//                logger.warn("Handle role change failed term={} role={} handler={}", term, role, roleChangeHandler.getClass(), t);
//            }
//        }
//    }

    private void maintainAsFollower() {
        if (Utils.elapsed(lastLeaderHeartBeatTime) > 2 * heartBeatTimeIntervalMs) {
            synchronized (memberState) {
                if (memberState.isFollower() && (Utils.elapsed(lastLeaderHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs)) {
                    logger.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {} lastLeader={}", memberState.getSelfId(), new Timestamp(lastLeaderHeartBeatTime), heartBeatTimeIntervalMs, memberState.getLeaderId());
                    changeRoleToCandidate(memberState.getCurrTerm());
                }
            }
        }
    }

    private void maintainAsCandidate() throws Exception {
        if (System.currentTimeMillis() < nextTimeToRequestVote && !needIncreaseTermImmediately) {
            return;
        }
        long term;
        long endTerm;
        long endIndex;
        synchronized (memberState) {
            if (!memberState.isCandidate()) {
                return;
            }
            if (lastParseResult == VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT || needIncreaseTermImmediately) {
                long prevTerm = memberState.getCurrTerm();
                term = memberState.nextTerm();
                logger.info("{}_[INCREASE_TERM] from {} to {}", memberState.getSelfId(), prevTerm, term);
                lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            } else {
                term = memberState.getCurrTerm();
            }
            endIndex = memberState.getEndIndex();
            endTerm = memberState.getEndTerm();
        }
        if (needIncreaseTermImmediately) {
            nextTimeToRequestVote = getNextTimeToRequestVote();
            needIncreaseTermImmediately = false;
            return;
        }

        long startVoteTimeMs = System.currentTimeMillis();
        final List<CompletableFuture<VoteResponse>> quorumVoteResponses = voteForQuorumResponses(term, endTerm, endIndex);
        final AtomicLong knownMaxTermInGroup = new AtomicLong(-1);
        final AtomicInteger allNum = new AtomicInteger(0);
        final AtomicInteger validNum = new AtomicInteger(0);
        final AtomicInteger acceptedNum = new AtomicInteger(0);
        final AtomicInteger notReadyTermNum = new AtomicInteger(0);
        final AtomicInteger biggerNum = new AtomicInteger(0);
        final AtomicBoolean alreadyHasLeader = new AtomicBoolean(false);

        CountDownLatch downLatch = new CountDownLatch(1);
        for (CompletableFuture<VoteResponse> future : quorumVoteResponses) {
            future.whenComplete((VoteResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }
                    logger.info("[{}] getvoteResponse {}", memberState.getSelfId(), JSON.toJSONString(x));
                    if (x.getVoteResult() != VoteResponse.RESULT.UNKNOWN) {
                        validNum.incrementAndGet();
                    }
                    synchronized (knownMaxTermInGroup) {
                        switch (x.getVoteResult()) {
                            case ACCEPT:
                                acceptedNum.incrementAndGet();
                                break;
                            case REJECT_ALREADY_HAS_LEADER:
                                alreadyHasLeader.compareAndSet(false, true);
                                break;
                            case REJECT_TERM_SMALL_THAN_LEDGER:
                            case REJECT_EXPIRED_VOTE_TERM:
                                if (x.getTerm() > knownMaxTermInGroup.get()) {
                                    knownMaxTermInGroup.set(x.getTerm());
                                }
                                break;
                            case REJECT_EXPIRED_TERM:
                            case REJECT_SMALL_LEDGER_END_INDEX:
                                biggerNum.incrementAndGet();
                                break;
                            case REJECT_TERM_NOT_READY:
                                notReadyTermNum.incrementAndGet();
                                break;
                            default:
                                break;
                        }
                    }
                    if (alreadyHasLeader.get()
                        || memberState.isQuorum(acceptedNum.get())
                        || memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
                        downLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("get error when parsing vote response.", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        downLatch.countDown();
                    }
                }
            });
        }
        try {
            downLatch.await(3000 + random.nextInt(maxVoteIntervalMs), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
             // ignore
        }
        lastVoteCost = Utils.elapsed(startVoteTimeMs);
        VoteResponse.ParseResult parseResult;
        if (knownMaxTermInGroup.get() > term) { // self's term小于其他候选者的任期，等待下一次选举
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
            changeRoleToCandidate(knownMaxTermInGroup.get());
        } else if (alreadyHasLeader.get()) { // 已经有leader了，等待心跳超时
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote() + heartBeatTimeIntervalMs*maxHeartBeatLeak;
        } else if (!memberState.isQuorum(validNum.get())) { // 可能是自己断网了，没有收到足够的response，不增加term,重新vote
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else if (memberState.isQuorum(acceptedNum.get())) { // 自己当选leader
            parseResult = VoteResponse.ParseResult.PASSED;
        } else if (memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
            parseResult = VoteResponse.ParseResult.REVOTE_IMMEDIATELY;
        } else if (memberState.isQuorum(acceptedNum.get() + biggerNum.get())) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else {
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        }
        lastParseResult = parseResult;
        logger.info("[{}] [PARSE_VOTE_RESULT] cost={} term={} memberNum={} allNum={} acceptedNum={} notReadyTermNum={}" +
                        " biggerLedgerNum={} alreadyHasLeader={} maxTerm={} result={}",
                memberState.getSelfId(), lastVoteCost, term, memberState.peerSize(), allNum, acceptedNum, notReadyTermNum,
                biggerNum, alreadyHasLeader, knownMaxTermInGroup.get(), parseResult);
        if (parseResult == VoteResponse.ParseResult.PASSED) {
            logger.info("[{}] [VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
            changeRoleToLeader(term);
        }
    }

    private void changeRoleToLeader(long term) {
        synchronized (memberState) {
            if (memberState.getCurrTerm() == term) {
                memberState.changeToLeader(term);
                lastSendHeartBeatTime = -1;
//                handleRoleChange(term, MemberState.Role.LEADER);
                logger.info("[{}] [ChangeRoleToLeader] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.getCurrTerm());
            }
        }
    }

    private List<CompletableFuture<VoteResponse>> voteForQuorumResponses(long term, long endTerm, long endIndex)
        throws Exception {
        List<CompletableFuture<VoteResponse>> responses = new ArrayList<>();
        for (String id : memberState.getPeerMap().keySet()) {
            VoteRequest request = new VoteRequest();
            request.setGroup(memberState.getGroup());
            request.setEndIndex(endIndex);
            request.setEndTerm(endTerm);
            request.setLeaderId(memberState.getSelfId());
            request.setTerm(term);
            request.setRemoteId(id);
            CompletableFuture<VoteResponse> voteResponse;
            if (memberState.getSelfId().equals(id)) {
                voteResponse = handleVote(request, true);
            } else {
                voteResponse = rpcService.vote(request);
            }
            responses.add(voteResponse);
        }
        return responses;
    }

    private long getNextTimeToRequestVote() {
        return System.currentTimeMillis() + lastVoteCost + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
    }

    /**Handle vote rpc.Implementation for receivers.
     * 1. candidate.term < local.term ==> reject
     * 2. if voteFor == null && candidate's log is the same as local, vote for it.
     * @param request
     * @param isSelf
     * @return
     */
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request, boolean isSelf) {
        synchronized (memberState) {
            if (!memberState.isPeerMember(request.getLeaderId())) {
                logger.warn("{} us a invalid member in handlevote", request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getCurrTerm())
                    .voteResult(VoteResponse.RESULT.REJECT_UNKNOWN_LEADER));
            }
            if (!isSelf && memberState.getSelfId().equals(request.getLeaderId())) {
                logger.warn("[bug] in handle vote, self id = {}, remoteId = {}", memberState.getSelfId(), request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getCurrTerm())
                    .voteResult(VoteResponse.RESULT.REJECT_UNEXPECTED_LEADER));
            }
            if (request.getTerm() < memberState.getCurrTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getCurrTerm())
                    .voteResult(VoteResponse.RESULT.REJECT_EXPIRED_VOTE_TERM));
            } else if (request.getTerm() == memberState.getCurrTerm()) {
                if (memberState.getCurrVoteFor() == null || memberState.getCurrVoteFor().equals(request.getLeaderId())) {
                    // let it go
                } else {
                    if (memberState.getLeaderId() != null) {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getCurrTerm())
                            .voteResult(VoteResponse.RESULT.REJECT_ALREADY_HAS_LEADER));
                    } else {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getCurrTerm())
                            .voteResult(VoteResponse.RESULT.REJECT_ALREADY_VOTED));
                    }
                }
            } else {
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getCurrTerm())
                    .voteResult(VoteResponse.RESULT.REJECT_TERM_NOT_READY));
            }

            // vote
            if (request.getEndTerm() < memberState.getEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getCurrTerm())
                    .voteResult(VoteResponse.RESULT.REJECT_EXPIRED_TERM));
            } else if (request.getEndTerm() == memberState.getEndTerm() && request.getEndIndex() < memberState.getEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getCurrTerm())
                    .voteResult(VoteResponse.RESULT.REJECT_SMALL_LEDGER_END_INDEX));
            }

            if (request.getTerm() < memberState.getEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getEndTerm())
                    .voteResult(VoteResponse.RESULT.REJECT_TERM_SMALL_THAN_LEDGER));
            }

            memberState.setCurrVoteFor(request.getLeaderId());
            return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getCurrTerm())
                .voteResult(VoteResponse.RESULT.ACCEPT));
        }
    }

    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) {
        if (!memberState.isPeerMember(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] remoteId={} is an unknown member", request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.getCurrTerm())
                .code(ResponseCode.UNKNOWN_MEMBER.getCode()));
        }

        if (memberState.getSelfId().equals(request.getLeaderId())) {
            logger.warn("[BUG] handleHeartBeat selfID = {}, remoteId = {}", memberState.getSelfId(), request.getRemoteId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.getCurrTerm())
                .code(ResponseCode.UNEXPECTED_MEMBER.getCode()));
        }

        if (request.getTerm() < memberState.getCurrTerm()) {
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.getCurrTerm())
                .code(ResponseCode.EXPIRED_TERM.getCode()));
        } else if (request.getTerm() == memberState.getCurrTerm()) {
            if (request.getLeaderId().equals(memberState.getLeaderId())) {
                lastLeaderHeartBeatTime = System.currentTimeMillis();
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }
        // exceptional situation : request.term > memberstate.currTerm
        synchronized (memberState) {
            if (request.getTerm() < memberState.getCurrTerm()) {
                return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.getCurrTerm())
                    .code(ResponseCode.EXPIRED_TERM.getCode()));
            } else if (request.getTerm() == memberState.getCurrTerm()) {
                if (memberState.getLeaderId() == null) {
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else if (request.getLeaderId().equals(memberState.getLeaderId())) {
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {
                    // Can't happen in common situation. Request.leaderId != local.leaderId
                    logger.error("[{}] error. Different leaderID, local leader id = {}, but request's leaderid = {}",
                            memberState.getSelfId(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.getCurrTerm())
                            .code(ResponseCode.INCONSISTENT_LEADER.getCode()));
                }
            } else {
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.getCurrTerm())
                    .code(ResponseCode.TERM_NOT_READY.getCode()));
            }
        }

    }

    public class StateMaintainer extends ShutdownableThread {

        public StateMaintainer(String name, Logger LOG) {
            super("StateMaintainer", LOG);
        }

        @Override
        public void dowork() {
            try {
                if (LeaderElection.this.nodeConfig.isEnableLeaderElector()) {
                    LeaderElection.this.refreshIntervals(nodeConfig);
                    LeaderElection.this.maintainState();
                }
                sleep(10);
            } catch (Throwable t) {
                LOG.error("Error in heartbeat");
            }
        }
    }

    public interface RoleChangeHandler extends LifeCycle {
        void handle(long term, MemberState.Role role);
    }
}
