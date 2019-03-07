package com.github.chenfeikun.raft;

import java.util.ArrayList;

/**
 * @desciption: RaftNode
 * @CreateTime: 2019-03-05
 * @author: chenfeikun
 */
public class RaftNode {

    private enum State {
        FOLLEWER, CANDIDATE, LEADER
    }

    /* 服务器状态*/
    private State state = State.FOLLEWER;
    /* 当前任期，初值为０，单调递增*/
    private long currentTerm;
    /* 当前任期下投票的服务器id,可为null*/
    private Integer voteFor;
    /* 日志记录，包括命令＋任期，索引初始值为1*/
    private ArrayList<LogEntry> logs;
    /* 将被提交的日志索引，初值为0，递增*/
    private int commitIndex;
    /* 已经被提交到状态机的最后一个日志索引，初值0,递增*/
    private int lastApplied;
    //　leader拥有的状态
    /* 每台机器在数组上占一个元素，值为下一条发送出的日志索引，初值为leader最新日志索引+1*/
    private int[] nextIndex;
    /* 每台机器一个，值为将要复制给该机器的日志索引*/
    private int[] matchIndex;
    // 状态机
    private StateMachine stateMachine;

    public RaftNode(Server[] servers, Server localServer, StateMachine stateMachine) {
        this.stateMachine = stateMachine;
        init(servers, localServer);
    }

    public void init(Server[] servers, Server localServer) {
        currentTerm = 0;
        voteFor = null;
        logs = new ArrayList<>();
        logs.add(LogEntry.ZERO_LOG);  //　日志记录索引值初始值为1,0处占位
        commitIndex = 0;
        lastApplied = 0;
        nextIndex = new int[servers.length]; // only leader need
        matchIndex = new int[servers.length]; // Only leader need
        state  = State.FOLLEWER; // initial at follower
        resetElectionTime();
    }


    private void resetElectionTime() {

    }





}
