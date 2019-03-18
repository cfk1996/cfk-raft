package com.github.chenfeikun.raft.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @desciption: NamedThreadFactory with given prefix.
 * @CreateTime: 2019-03-17
 * @author: chenfeikun
 */
public class NamedThreadFactory implements ThreadFactory {

    private static final Logger LOG = LoggerFactory.getLogger(NamedThreadFactory.class);

    private static final LogUnCaughtExceptionHandler UN_CAUGHT_EXCEPTION_HANDLER = new LogUnCaughtExceptionHandler();

    private static final class LogUnCaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            LOG.error("Uncaught exception in thread {}, {}", t, e);
        }
    }

    private final String prefix;
    private final boolean daemon;

    private final AtomicInteger counter = new AtomicInteger(0);

    public NamedThreadFactory(String prefix) {
        this(prefix, false);
    }

    public NamedThreadFactory(String prefix, boolean daemon) {
        this.prefix = prefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(daemon);
        t.setUncaughtExceptionHandler(UN_CAUGHT_EXCEPTION_HANDLER);
        t.setName(prefix + counter.getAndIncrement());
        return t;
    }
}
