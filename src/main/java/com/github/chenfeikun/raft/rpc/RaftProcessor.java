package com.github.chenfeikun.raft.rpc;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AbstractUserProcessor;

import com.github.chenfeikun.raft.exception.RaftNotSupportMethodException;
import com.github.chenfeikun.raft.exception.RaftRemotingException;
/**
 * @desciption: RaftProcessor
 * @CreateTime: 2019-03-09
 * @author: chenfeikun
 */
public abstract class RaftProcessor<T> extends AbstractUserProcessor<T> {
    @Override
    public void handleRequest(BizContext bizCtx, AsyncContext asyncCtx, T request) {
        throw new RaftNotSupportMethodException("not support async handle request");
    }

    @Override
    public String interest() {
        return Request.class.getName();
    }
}
