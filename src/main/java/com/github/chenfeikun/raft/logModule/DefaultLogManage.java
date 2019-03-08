package com.github.chenfeikun.raft.logModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @desciption: DefaultLogManage
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public class DefaultLogManage implements LogManage {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogManage.class);
    /* log entries*/
    private ArrayList<LogEntry> logEntries;

    private DefaultLogManage() {
        logEntries = new ArrayList<>();
        logEntries.add(LogEntry.ZERO_LOG); // 日志索引初始为1
    }

    public static final DefaultLogManage getInstance() {


    }

    private static final class DefaultLogManageLazyHolder {
        private static final DefaultLogManage INSTANCE = new DefaultLogManage();
    }

    @Override
    public void write(LogEntry logEntry) {

    }

    @Override
    public LogEntry read(int index) {
        return null;
    }

    @Override
    public int getLastIndex() {
        return 0;
    }

    @Override
    public LogEntry getLastEntry() {
        return null;
    }
}
