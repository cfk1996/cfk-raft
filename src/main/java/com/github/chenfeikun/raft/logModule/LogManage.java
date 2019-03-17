package com.github.chenfeikun.raft.logModule;

import com.github.chenfeikun.raft.logModule.LogEntry;

/**
 * @desciption: LogManage
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public interface LogManage {

    void write(LogEntry logEntry);

    LogEntry read(int index);

    int getLastIndex();

    LogEntry getLastEntry();

    /**
     * 日志不匹配时，删除prevLogIndex及之后的日志
     * @param index　prevLogIndex
     */
    void removeToEnd(int index);
}
