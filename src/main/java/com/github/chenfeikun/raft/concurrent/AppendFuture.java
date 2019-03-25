package com.github.chenfeikun.raft.concurrent;

/**
 * @desciption: AppendFuture
 * @CreateTime: 2019-03-25
 * @author: chenfeikun
 */
public class AppendFuture<T> extends TimeoutFuture<T> {

    private long pos = -1;

    public AppendFuture() {

    }

    public AppendFuture(long timeOutMs) {
        this.timeoutMs = timeOutMs;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public static <T> AppendFuture<T> newCompletedFuture(long pos, T value) {
        AppendFuture<T> future = new AppendFuture<T>();
        future.setPos(pos);
        future.complete(value);
        return future;
    }
}
