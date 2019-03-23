package com.github.chenfeikun.raft.utils;

import org.slf4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @desciption: ShutdownableThread
 * @CreateTime: 2019-03-20
 * @author: chenfeikun
 */
public abstract class ShutdownableThread extends Thread {

    protected final ResettableCountDownLatch waitPoint = new ResettableCountDownLatch(1);
    protected Logger LOG;
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    private AtomicBoolean running = new AtomicBoolean(true);
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public ShutdownableThread(String name, Logger LOG) {
        super(name);
        this.LOG = LOG;
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            try {
                wakeup();
                countDownLatch.await(10, TimeUnit.SECONDS);
            } catch (Throwable t) {
                if (LOG != null) {
                    LOG.error("Unexpected exception in shutdown");
                }
            }
            if (countDownLatch.getCount() != 0) {
                if (LOG != null) {
                    LOG.error("The {} failed to shutdown in 10 seconds", getName());
                }
            }
        }
    }

    private void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown();
        }
    }

    public void waitForRunning(long interval) throws InterruptedException {
        if (hasNotified.compareAndSet(true, false)) {
            return;
        }
        waitPoint.reset();
        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw e;
        } finally {
            hasNotified.set(false);
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                dowork();
            } catch (Throwable t) {
                if (LOG != null) {
                    LOG.error("Unexpected error in running {}", getName(), t);
                }
            }
        }
        countDownLatch.countDown();
    }

    public abstract void dowork();
}
