package com.github.chenfeikun.raft.logModule;

import org.omg.Messaging.SYNC_WITH_TRANSPORT;

/**
 * @desciption: Main
 * @CreateTime: 2019-03-13
 * @author: chenfeikun
 */
public class Main {

    public static void main(String[] args) {
        LogManage logManage = DefaultLogManage.getInstance();
        LogEntry logEntry = new LogEntry(1, "s");
        logManage.write(logEntry);
        LogEntry entry = logManage.read(0);
        if (entry == LogEntry.ZERO_LOG) {
            System.out.println("the same");
        } else {
            System.out.println("not same");
        }
    }
}
