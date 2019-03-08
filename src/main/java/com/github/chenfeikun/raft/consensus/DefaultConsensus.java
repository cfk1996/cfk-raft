package com.github.chenfeikun.raft.consensus;

import com.github.chenfeikun.raft.consensus.Consensus;
import com.github.chenfeikun.raft.entity.EntriesParam;
import com.github.chenfeikun.raft.entity.EntriesResult;
import com.github.chenfeikun.raft.entity.VoteParam;
import com.github.chenfeikun.raft.entity.VoteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desciption: DefaultConsensus
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public class DefaultConsensus implements Consensus {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultConsensus.class);

    private DefaultConsensus() {}

    public static final DefaultConsensus getInstance() {
        return DefaultConsensusLazyHolder.INSTANCE;
    }

    private static final class DefaultConsensusLazyHolder {
        private static final DefaultConsensus INSTANCE = new DefaultConsensus();
    }

    @Override
    public VoteResult requestVote(VoteParam voteParam) {
        return null;
    }

    @Override
    public EntriesResult appendEntries(EntriesParam entriesParam) {
        return null;
    }
}
