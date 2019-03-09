package com.github.chenfeikun.raft.exception;

/**
 * @desciption: RaftRemotingException
 * @CreateTime: 2019-03-09
 * @author: chenfeikun
 */
public class RaftRemotingException extends RuntimeException {

    public RaftRemotingException(Exception e) {
        super(e);
    }
}
