package com.github.chenfeikun.raft;

import com.github.chenfeikun.raft.entity.EntriesParam;
import com.github.chenfeikun.raft.entity.EntriesResult;
import com.github.chenfeikun.raft.entity.VoteParam;
import com.github.chenfeikun.raft.entity.VoteResult;

/**
 * @desciption: Node
 * @CreateTime: 2019-03-08
 * @author: chenfeikun
 */
public interface Node extends LifeCycle{

    VoteResult handleRequestVote(VoteParam param);

    EntriesResult handleAppendEntries(EntriesParam param);

    void handlerClientRequest();

    void redirect();
}
