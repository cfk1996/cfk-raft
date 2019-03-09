package com.github.chenfeikun.raft.stateMachine;

import com.github.chenfeikun.raft.logModule.LogEntry;

/**
 * @desciption: DefaultStateMachine
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public class DefaultStateMachine implements StateMachine {


    private DefaultStateMachine() {}

    public static final DefaultStateMachine getInstance() {
        return DefaultStateMachineLazyHolder.INSTANCE;
    }

    private static final class DefaultStateMachineLazyHolder {
        private static final DefaultStateMachine INSTANCE = new DefaultStateMachine();
    }

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
