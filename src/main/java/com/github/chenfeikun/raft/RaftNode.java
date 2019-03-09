package com.github.chenfeikun.raft;

import com.github.chenfeikun.raft.consensus.Consensus;
import com.github.chenfeikun.raft.consensus.DefaultConsensus;
import com.github.chenfeikun.raft.entity.RaftConfig;
import com.github.chenfeikun.raft.entity.Server;
import com.github.chenfeikun.raft.logModule.DefaultLogManage;
import com.github.chenfeikun.raft.logModule.LogEntry;
import com.github.chenfeikun.raft.logModule.LogManage;
import com.github.chenfeikun.raft.stateMachine.DefaultStateMachine;
import com.github.chenfeikun.raft.stateMachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

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

    /* raft　config*/
    private RaftConfig config;
    /* 服务器状态*/
    private State state = State.FOLLEWER;
    /* 当前任期，初值为０，单调递增*/
    private long currentTerm;
    /* 当前任期下投票的服务器id,可为null*/
    private Integer voteFor;
    /* 将被提交的日志索引，初值为0，递增*/
    private int commitIndex;
    /* 已经被提交到状态机的最后一个日志索引，初值0,递增*/
    private int lastApplied;
    //　leader拥有的状态
    /* 每台机器在数组上占一个元素，值为下一条发送出的日志索引，初值为leader最新日志索引+1*/
    private int[] nextIndex;
    /* 每台机器一个，值为将要复制给该机器的日志索引*/
    private int[] matchIndex;
    /* 一致性模块*/
    private Consensus consensus;
    /* 日志模块，*/
    private LogManage logManage;
    // 状态机
    private StateMachine stateMachine;

    public RaftNode(Server[] servers, Server localServer) {
        currentTerm = 0;
        voteFor = null;
        commitIndex = 0;
        lastApplied = 0;
        nextIndex = new int[servers.length]; // only leader need
        matchIndex = new int[servers.length]; // Only leader need
        state  = State.FOLLEWER; // initial at follower
    }

    @Override
    public void init() {
        consensus = DefaultConsensus.getInstance();
        stateMachine = DefaultStateMachine.getInstance();
        logManage = DefaultLogManage.getInstance();
    }

    @Override
    public void destory() {

    }

    public void setConfig(RaftConfig config) {
        this.config = config;
    }

    @Override
    public void handleRequestVote() {

    }

    @Override
    public void handleAppendEntries() {

    }

    @Override
    public void handlerClientRequest() {

    }

    @Override
    public void redirect() {

    }
}
