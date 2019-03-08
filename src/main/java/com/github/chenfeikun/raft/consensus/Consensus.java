package com.github.chenfeikun.raft.consensus;

import com.github.chenfeikun.raft.entity.EntriesParam;
import com.github.chenfeikun.raft.entity.EntriesResult;
import com.github.chenfeikun.raft.entity.VoteParam;
import com.github.chenfeikun.raft.entity.VoteResult;

/**
 * @desciption: Consensus
 * @CreateTime: 2019-03-07
 * @author: chenfeikun
 */
public interface Consensus {

    // vote rpc
    VoteResult requestVote(VoteParam voteParam);

    // rpc for leader to append entries or send heartbeat
    EntriesResult appendEntries(EntriesParam entriesParam);
}
