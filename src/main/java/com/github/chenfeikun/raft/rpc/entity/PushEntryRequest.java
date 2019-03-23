package com.github.chenfeikun.raft.rpc.entity;

import com.github.chenfeikun.raft.core.Entry;

/**
 * @desciption: PushEntryRequest
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class PushEntryRequest extends RequestOrResponse {
    private long commitIndex = -1;
    private Type type = Type.APPEND;
    private Entry entry;

    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public enum Type {
        APPEND,
        COMMIT,
        COMPARE,
        TRUNCATE;
    }
}
