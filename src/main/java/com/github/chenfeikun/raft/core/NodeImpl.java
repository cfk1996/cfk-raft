package com.github.chenfeikun.raft.core;

import com.github.chenfeikun.raft.Closure;
import com.github.chenfeikun.raft.Node;
import com.github.chenfeikun.raft.entity.PeerId;
import com.github.chenfeikun.raft.options.NodeOptions;
import com.github.chenfeikun.raft.entity.Task;
import com.github.chenfeikun.raft.options.RaftOptions;
import com.github.chenfeikun.raft.utils.RepeatedTimer;
import com.github.chenfeikun.raft.utils.Requires;
import com.github.chenfeikun.raft.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desciption: Raft node implementation.
 * @CreateTime: 2019-03-17
 * @author: chenfeikun
 */
public class NodeImpl implements Node {

    private static final Logger LOG = LoggerFactory.getLogger(NodeImpl.class);

    /** internal states*/
    private PeerId serverId;
    private String groupId;
    private State state;
    private long currTerm;
    private volatile long lastLeaderTimestamp;
    /** options*/
    private NodeOptions options;
    private RaftOptions raftOptions;
    /** timer */
    private TimerManager timerManager;
    private RepeatedTimer voteTimer;
    private RepeatedTimer electionTimer;
    private RepeatedTimer stepdownTimer;

    public NodeImpl(String groupId, PeerId serverId) {
        super();
        this.groupId = groupId;
        this.serverId = serverId != null ? serverId.copy() : null;
        this.state = State.STATE_UNINITIALIZED;
        this.currTerm = 0;
        this.updateLastLeaderTimestamp(Utils.monotonicMs());
//        this.confCtx = new ConfigurationCtx(this);
//        this.wakingCandidate = null;
//        GLOBAL_NUM_NODES.incrementAndGet();
    }

    private void updateLastLeaderTimestamp(long lastLeaderTimestamp) {
        this.lastLeaderTimestamp = lastLeaderTimestamp;
    }

    @Override
    public void apply(Task task) {

    }

    @Override
    public PeerId getLeaderID() {
        return null;
    }

    @Override
    public void join() {

    }

    @Override
    public void snapshot(Closure done) {

    }

    private void handleVoteTimeout() {}

    private int randomTimeout(int timeoutMs) {
        return -1;
    }

    private void handleElectionTimeout() {}

    private void handleStepDownTimeout() {}

    @Override
    public boolean init(NodeOptions opts) {
        Requires.requireNonNull(opts, "Null node options");
        Requires.requireNonNull(opts.getRaftOptions(), "Null raft options");
        this.options = opts;
        this.raftOptions = options.getRaftOptions();

        if (this.serverId.getIp().equals(Utils.IP_ANY)) {
            LOG.error("Node can't started from IP_ANY");
            return false;
        }

//        if (!NodeManager.getInstance().serverExists(this.serverId.getEndpoint())) {
//            LOG.error("No RPC server attached to, did you forget to call addService?");
//            return false;
//        }
        this.timerManager = new TimerManager();
        if (!this.timerManager.init(opts.getTimerPoolSize())) {
            LOG.error("failed to init executor");
            return false;
        }
        // init timer
        this.voteTimer = new RepeatedTimer("Vote-Timer", this.options.getElectionTimeoutMs()) {
            @Override
            protected void onTrigger() {
                handleVoteTimeout();
            }

            @Override
            protected int adjustTimeout(int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };
        this.electionTimer = new RepeatedTimer("Election-Timer", this.options.getElectionTimeoutMs()) {
            @Override
            protected void onTrigger() {
                handleElectionTimeout();
            }

            @Override
            protected int adjustTimeout(int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };
        this.stepdownTimer = new RepeatedTimer("StepDown-Timer", this.options.getElectionTimeoutMs()>>1) {
            @Override
            protected void onTrigger() {
                handleStepDownTimeout();
            }
        };
        // TODO: this.snapshot = new RepeatedTimer(){...}
        // TODO: applyDisruptor
        // TODO: state machine



    }

    @Override
    public void shutdown() {

    }
}
