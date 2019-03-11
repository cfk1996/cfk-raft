package com.github.chenfeikun.raft;

import com.github.chenfeikun.raft.concurrent.RaftExecutor;
import com.github.chenfeikun.raft.concurrent.ThreadNameFactory;
import com.github.chenfeikun.raft.consensus.Consensus;
import com.github.chenfeikun.raft.consensus.DefaultConsensus;
import com.github.chenfeikun.raft.entity.*;
import com.github.chenfeikun.raft.logModule.DefaultLogManage;
import com.github.chenfeikun.raft.logModule.LogEntry;
import com.github.chenfeikun.raft.logModule.LogManage;
import com.github.chenfeikun.raft.rpc.*;
import com.github.chenfeikun.raft.stateMachine.DefaultStateMachine;
import com.github.chenfeikun.raft.stateMachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @desciption: RaftNode
 * @CreateTime: 2019-03-05
 * @author: chenfeikun
 */
public class RaftNode implements Node{

    private static final Logger LOG = LoggerFactory.getLogger(RaftNode.class);

    private enum State {
        FOLLEWER, CANDIDATE, LEADER
    }

    /**
     *  =============================static variable==============================
     */

    /**
     * 选举最低时间 150ms
     */
    private static final int ELECTION_BASE_TIME = 150;
    /**
     * 选举浮动时间, 选举时间为[150, 300]ms
     */
    private static final int ELECTION_RANDOM_TIME = 150;
    /**
     * 心跳超时时间 500ms
     */
    private static final int HEART_BEAT_TIMEOUT = 500;

    /**
     * ==========================================================================
     */
    // 是否初始化标志，只允许初始化一次
    private volatile boolean started = false;
    /* raft　config*/
    private RaftConfig config;
    /* 服务器状态*/
    private volatile State state = State.FOLLEWER;
    /* 当前任期，初值为０，单调递增*/
    private AtomicLong currentTerm;
    /* 当前任期下投票的服务器id,可为null*/
    private Integer voteFor;
    /* 将被提交的日志索引，初值为0，递增*/
    private int commitIndex;
    /* 已经被提交到状态机的最后一个日志索引，初值0,递增*/
    private int lastApplied;
    //　leader拥有的状态
    /* 每台机器在数组上占一个元素，值为下一条发送出的日志索引，初值为leader最新日志索引+1*/
    private Map<Server, Integer> nextIndex;
    /* 每台机器一个，值为将要复制给该机器的日志索引*/
    private Map<Server, Integer> matchIndex;
    /* 一致性模块*/
    private Consensus consensus;
    /* 日志模块，*/
    private LogManage logManage;
    // 状态机
    private StateMachine stateMachine;
    // local server
    private Server localServer;
    // peers ; other servers
    private Peer peerSet;

    // Raft thread pool and executors
    private RaftExecutor raftExecutor;
    // rpc
    private RpcClient rpcClient;
    private RpcServer rpcServer;
    // raft rpc
    private AppendEntriesTask appendEntriesTask;
    private ElectionTask electionTask;
    private ScheduledExecutorService appendEntriesExecutor;
    private ScheduledExecutorService electionExecutor;

    private long preElectionTime;
    private long preAppendEntriesTime;

    public RaftNode(Server[] servers, Server localServer) {
        currentTerm = new AtomicLong(0);
        voteFor = null;
        commitIndex = 0;
        lastApplied = 0;
        this.localServer = localServer;
        this.peerSet = new Peer(servers, localServer);
        nextIndex = new ConcurrentHashMap<>(servers.length); // only leader need
        matchIndex = new ConcurrentHashMap<>(servers.length); // Only leader need
        state  = State.FOLLEWER; // initial at follower
    }

    @Override
    public void init() {
        if (started) {
            return;
        }
        synchronized (this) {
            if (started) {
                return;
            }
            started = true;
            consensus = DefaultConsensus.getInstance();
            stateMachine = DefaultStateMachine.getInstance();
            logManage = DefaultLogManage.getInstance();
            rpcClient = new DefaultRpcClient();
            rpcServer = new DefaultRpcServer(localServer.getPort(), this);
            rpcServer.start();
            // initial rpc tasks and schedule them in thread pool
            appendEntriesTask = new AppendEntriesTask();
            electionTask = new ElectionTask();
            raftExecutor = new RaftExecutor();
            raftExecutor.executeAppendEntries(appendEntriesTask, 0, 500, TimeUnit.MILLISECONDS);
            raftExecutor.executeEletion(electionTask, 6000, 500, TimeUnit.MILLISECONDS);
        }

    }

    @Override
    public void destory() {
        rpcServer.stop();
    }

    public void setConfig(RaftConfig config) {
        this.config = config;
    }

    @Override
    public VoteResult handleRequestVote(VoteParam param) {
        return null;
    }

    @Override
    public EntriesResult handleAppendEntries(EntriesParam param) {
        return null;
    }

    @Override
    public void handlerClientRequest() {

    }

    @Override
    public void redirect() {

    }

    /**
     * init nextIndex and matchIndex after localServer selected as leader
     */
    private void preBecomeLeader() {
        List<Server> otherServers = peerSet.getServerListExceptLocal();
        int lastIndex = logManage.getLastIndex();
        for (Server s : otherServers) {
            nextIndex.put(s, lastIndex+1);
            matchIndex.put(s, 0);
        }
    }

    /**
     * send empty append rpc to other server after localServer selected as leader
     */
    public void sendHeartBeat() {
        List<Server> otherServers = peerSet.getServerListExceptLocal();
        EntriesParam entriesParam = EntriesParam.newBuilder()
                .term(currentTerm.get())
                .leaderId(localServer.getServerId())
                .prevLogIndex(logManage.getLastIndex())
                .prevLogTerm(logManage.getLastEntry().getTerm())
                .entries(null)
                .leaderCommit(commitIndex)
                .build();
        for (Server s : otherServers) {
            raftExecutor.execute(() -> {
                Request<EntriesParam> request = Request.<EntriesParam>newBuilder()
                        .type(Request.A_ENTRIES)
                        .url(s.getUrl())
                        .obj(entriesParam)
                        .build();
                try {
                    Response<EntriesResult> response = rpcClient.invoke(request);
                    long term = response.getResult().getTerm();
                    if (term > currentTerm.get()) {
                        currentTerm.set(term);
                        state = State.FOLLEWER;
                        voteFor = null;
                        LOG.info("leader term is expire, url = " + s.getUrl());
                    }
                } catch (Exception e) {
                    LOG.error("send heartbeat error, url = " + s.getUrl());
                }
            });
        }
    }

    /**
     * used to process append entries rpc request
     */
    private class AppendEntriesTask implements Runnable {
        @Override
        public void run() {
            if (state != State.LEADER) {
                return;
            }

        }
    }

    /**
     * used to process election rpc request
     */
    private class ElectionTask implements Runnable {
        @Override
        public void run() {
            if (state == State.LEADER) {
                return;
            }
            if (System.currentTimeMillis() - preAppendEntriesTime < ELECTION_BASE_TIME) {
                return;
            }
            Random random = new Random();
            try {
                preAppendEntriesTime = System.currentTimeMillis() + random.nextInt(ELECTION_RANDOM_TIME);
                sendVote();
            } catch (Exception e) {
                LOG.error("send vote error at : " + preAppendEntriesTime);
            }
        }

        public void sendVote() {
            // modify the node state before send vote request
            currentTerm.getAndIncrement();
            state = State.CANDIDATE;
            voteFor = localServer.getServerId();

            // send vote request in parallel, and put the result in future lists
            List<Future<Response<VoteResult>>> futures = new ArrayList<>(peerSet.getServerSizeWithoutLocal());
            VoteParam voteParam = VoteParam.newBuilder()
                    .term(currentTerm.get())
                    .candidateId(localServer.getServerId())
                    .lastLogIndex(logManage.getLastIndex())
                    .lastLogTerm(logManage.getLastEntry().getTerm())
                    .build();
            for (Server s : peerSet.getServerListExceptLocal()) {
                futures.add(raftExecutor.submit(() -> {
                    Request<VoteParam> request = Request.<VoteParam>newBuilder()
                            .type(Request.R_VOTE)
                            .obj(voteParam)
                            .url(s.getUrl())
                            .build();
                    try {
                        Response<VoteResult> response = rpcClient.invoke(request);
                        return response;
                    } catch (Exception e) {
                        LOG.error("Vote rpc error, url = " + s.getUrl());
                        return null;
                    }
                }));
            }

            // get the vote request responses to decide the next step
            AtomicInteger success = new AtomicInteger(1);
            CountDownLatch counts = new CountDownLatch(futures.size());
            for (Future<Response<VoteResult>> future : futures) {
                raftExecutor.execute(() -> {
                    try {
                        Response<VoteResult> response = future.get(3000, TimeUnit.MILLISECONDS);
                        if (response == null) {
                            return;
                        }
                        if (response.getResult().getTerm() > currentTerm.get()) {
                            currentTerm.set(response.getResult().getTerm());
                            voteFor = null;
                            return;
                        }
                        if (response.getResult().isVoteGranted()) {
                            success.getAndIncrement();
                        }
                    } catch (Exception e) {
                        LOG.error("receive vote response error, exception : " + e);
                    } finally {
                        counts.countDown();
                    }
                });
            }
            // wait for the result
            try {
                counts.await(3500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.error("interrupted by election");
            }
            if (state == State.FOLLEWER) {
                return;
            }
            // 获取大于半数的投票，成为Leader
            if (success.get() >= peerSet.getServerSize() / 2 + 1) {
                LOG.info(localServer.getUrl() + "become leader");
                preBecomeLeader();
                state = State.LEADER;
                sendHeartBeat();
            }
        }
    }

}
