package com.github.chenfeikun.raft.utils;

import com.github.chenfeikun.raft.exception.RaftException;
import com.github.chenfeikun.raft.rpc.entity.ResponseCode;

/**
 * @desciption: PreConditions
 * @CreateTime: 2019-03-19
 * @author: chenfeikun
 */
public class PreConditions {

    public static void check(boolean expression, ResponseCode code) throws RaftException {
        check(expression, code, null);
    }

    public static void check(boolean expression, ResponseCode code, String message) throws RaftException {
        if (!expression) {
            message = message == null ? code.toString()
                    : code.toString() + " " + message;
            throw new RaftException(code, message);
        }
    }

    public static void check(boolean expression, ResponseCode code, String format,
                             Object... args) throws RaftException {
        if (!expression) {
            String message = code.toString() + " " + String.format(format, args);
            throw new RaftException(code, message);
        }
    }
}
