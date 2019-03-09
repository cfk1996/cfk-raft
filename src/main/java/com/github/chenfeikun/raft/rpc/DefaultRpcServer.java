package com.github.chenfeikun.raft.rpc;

import com.alipay.remoting.BizContext;
import com.github.chenfeikun.raft.RaftNode;

/**
 * @desciption: DefaultRpcServer
 * @CreateTime: 2019-03-09
 * @author: chenfeikun
 */
public class DefaultRpcServer implements RpcServer {

    private com.alipay.remoting.rpc.RpcServer rpcServer;
    private RaftNode raftNode;

    public DefaultRpcServer(int port, RaftNode raftNode) {
        rpcServer = new com.alipay.remoting.rpc.RpcServer(port);
        rpcServer.registerUserProcessor(new RaftProcessor<Request>() {
            @Override
            public Object handleRequest(BizContext bizCtx, Request request) throws Exception {
                return DefaultRpcServer.this.handleRequest(request);
            }
        });
        this.raftNode = raftNode;
    }

    @Override
    public void start() {
        rpcServer.start();
    }

    @Override
    public void stop() {
        rpcServer.stop();
    }

    @Override
    public Response handleRequest(Request request) {
        if (request.getType() == Request.R_VOTE) {
            return new Response(node.handlerRequestVote((RvoteParam) request.getObj()));
        } else if (request.getType() == Request.A_ENTRIES) {
            return new Response(node.handlerAppendEntries((AentryParam) request.getObj()));
        } else if (request.getType() == Request.CLIENT_REQ) {
            return new Response(node.handlerClientRequest((ClientKVReq) request.getObj()));
        } else if (request.getType() == Request.CHANGE_CONFIG_REMOVE) {
            return new Response(((ClusterMembershipChanges) node).removePeer((Peer) request.getObj()));
        } else if (request.getType() == Request.CHANGE_CONFIG_ADD) {
            return new Response(((ClusterMembershipChanges) node).addPeer((Peer) request.getObj()));
        }
        return null;
    }
}
