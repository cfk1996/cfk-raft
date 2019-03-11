package com.github.chenfeikun.raft.concurrent;

import java.util.concurrent.*;

/**
 * @desciption: RaftExecutor
 * @CreateTime: 2019-03-11
 * @author: chenfeikun
 */
public class RaftExecutor {

    private static final ScheduledExecutorService APPEND_ENTRY_EXECUTOR = Executors.newScheduledThreadPool(2,
            new ThreadNameFactory("appendEntries"));

    private static final ScheduledExecutorService ELECTION_EXECUTOR = Executors.newScheduledThreadPool(2,
            new ThreadNameFactory("election"));

    private static final ExecutorService raftPool = Executors.newFixedThreadPool(5);

    public void executeAppendEntries(Runnable command, long initialDelay, long period, TimeUnit unit) {
        APPEND_ENTRY_EXECUTOR.scheduleWithFixedDelay(command, initialDelay, period, unit);
    }

    public void executeEletion(Runnable r, long initialDelay, long period, TimeUnit unit) {
        ELECTION_EXECUTOR.scheduleAtFixedRate(r, initialDelay, period, unit);
    }

    public <T> Future<T> submit(Callable r) {
        return (Future<T>) raftPool.submit(r);
    }

    public void execute(Runnable r) {
        raftPool.execute(r);
    }

}
