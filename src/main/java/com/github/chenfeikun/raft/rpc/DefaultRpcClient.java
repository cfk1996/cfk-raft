package com.github.chenfeikun.raft.rpc;

import com.alipay.remoting.exception.RemotingException;
import com.github.chenfeikun.raft.exception.RaftRemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desciption: DefaultRpcClient
 * @CreateTime: 2019-03-09
 * @author: chenfeikun
 */
public class DefaultRpcClient implements RpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRpcClient.class);

    private static final com.alipay.remoting.rpc.RpcClient CLIENT = new com.alipay.remoting.rpc.RpcClient();
    static {
        CLIENT.init();
    }

    @Override
    public Response invoke(Request request) {
        Response response = null;
        try {
            response = (Response) CLIENT.invokeSync(request.getUrl(), request, 20000);
        } catch (RemotingException e) {
            LOG.error("rpc remoting exception; request" + request);
            throw new RaftRemotingException(e);
        } catch (InterruptedException e) {
            LOG.error("rpc interrupt exception; request = " + request);
        }
        return response;
    }
}
