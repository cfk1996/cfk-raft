package com.github.chenfeikun.raft.stateMachine;

import com.github.chenfeikun.raft.logModule.LogEntry;

/**
 * @desciption: DefaultStateMachine
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public class DefaultStateMachine implements StateMachine {

    @Override
    public void apply(LogEntry entry) {

    }

    @Override
    public LogEntry read(int index) {
        return null;
    }

    @Override
    public LogEntry read(String key) {
        return null;
    }
}
