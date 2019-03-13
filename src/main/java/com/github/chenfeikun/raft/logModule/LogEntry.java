package com.github.chenfeikun.raft.logModule;

/**
 * @desciption: LogEntry
 * @CreateTime: 2019-03-05
 * @author: chenfeikun
 */
public class LogEntry {
    public static LogEntry ZERO_LOG = new LogEntry(-1, null);

    private long term;
    private String cmd;
    private int index;

    public LogEntry(long term, String cmd) {
        this.term = term;
        this.cmd = cmd;
    }

    public long getTerm() {
        return term;
    }

    public String getCmd() {
        return cmd;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
