package com.github.chenfeikun.raft;

/**
 * @desciption: Node
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public interface Node extends LifeCycle{

    void handleRequestVote();

    void handleAppendEntries();

    void handlerClientRequest();

    void redirect();
}
