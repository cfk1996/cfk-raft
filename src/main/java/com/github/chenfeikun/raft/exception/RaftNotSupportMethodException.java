package com.github.chenfeikun.raft.exception;

/**
 * @desciption: RaftNotSupportMethodException
 * @CreateTime: 2019-03-09
 * @author: chenfeikun
 */
public class RaftNotSupportMethodException extends RuntimeException {

    public RaftNotSupportMethodException(String message) {
        super(message);
    }
}
