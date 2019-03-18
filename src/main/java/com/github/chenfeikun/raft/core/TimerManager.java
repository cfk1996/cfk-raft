package com.github.chenfeikun.raft.core;

import com.github.chenfeikun.raft.LifeCycle;
import com.github.chenfeikun.raft.utils.NamedThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @desciption: TimerManager
 * @CreateTime: 2019-03-17
 * @author: chenfeikun
 */
public class TimerManager implements LifeCycle<Integer> {

    private ScheduledExecutorService executor;

    @Override
    public boolean init(Integer coreSize) {
        executor = Executors.newScheduledThreadPool(coreSize, new NamedThreadFactory("JRaft-Node-ScheduleThreadPool-"));
        return true;
    }

    @Override
    public void shutdown() {
        if (null != executor) {
            executor.shutdown();
            executor = null;
        }
    }

    private void checkStarted() {
        if (null == executor) {
            throw new IllegalStateException("Please init timer manager");
        }
    }

    public ScheduledFuture<?> schedule(Runnable r, long delay, TimeUnit unit) {
        checkStarted();
        return executor.schedule(r, delay, unit);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(Runnable r, long initDelay, long period, TimeUnit unit) {
        checkStarted();
        return executor.scheduleAtFixedRate(r, initDelay, period, unit);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable r, long initDelay, long delay, TimeUnit unit) {
        checkStarted();
        return executor.scheduleWithFixedDelay(r, initDelay, delay, unit);
    }
}
