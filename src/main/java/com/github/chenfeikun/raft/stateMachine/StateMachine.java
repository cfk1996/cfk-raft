package com.github.chenfeikun.raft.stateMachine;

import com.github.chenfeikun.raft.logModule.LogEntry;

/**
 * @desciption: StateMachine
 * @CreateTime: 2019-03-06
 * @author: chenfeikun
 */
public interface StateMachine {

    void apply(LogEntry entry);

    LogEntry read(int index);

    LogEntry read(String key);
}
