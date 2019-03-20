package com.github.chenfeikun.raft.exception;

import com.github.chenfeikun.raft.rpc.entity.ResponseCode;

/**
 * @desciption: RaftException
 * @CreateTime: 2019-03-19
 * @author: chenfeikun
 */
public class RaftException extends RuntimeException{

    private final ResponseCode code;

    public RaftException(ResponseCode code, String message) {
        super(message);
        this.code = code;
    }

    public RaftException(ResponseCode code, String format, Object... args) {
        super(String.format(format, args));
        this.code = code;
    }

    public ResponseCode getCode() {
        return code;
    }
}
