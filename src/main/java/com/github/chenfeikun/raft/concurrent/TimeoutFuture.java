package com.github.chenfeikun.raft.concurrent;

import java.util.concurrent.CompletableFuture;

/**
 * @desciption: TimeoutFuture
 * @CreateTime: 2019-03-21
 * @author: chenfeikun
 */
public class TimeoutFuture<T> extends CompletableFuture<T> {

    protected long createTimeMs = System.currentTimeMillis();

    protected long timeoutMs = 1000;

    public TimeoutFuture() {

    }

    public TimeoutFuture(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public void setCreateTimeMs(long createTimeMs) {
        this.createTimeMs = createTimeMs;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public boolean isTimeOut() {
        return System.currentTimeMillis() - createTimeMs >= timeoutMs;
    }
}
