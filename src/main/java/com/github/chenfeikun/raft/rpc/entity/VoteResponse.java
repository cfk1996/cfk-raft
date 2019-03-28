package com.github.chenfeikun.raft.rpc.entity;

/**
 * @desciption: VoteResponse
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class VoteResponse extends RequestOrResponse {

    public RESULT voteResult = RESULT.UNKNOWN;

    public VoteResponse() {

    }

    public VoteResponse(VoteRequest request) {
        copyBaseInfo(request);
    }

    public RESULT getVoteResult() {
        return voteResult;
    }

    public void setVoteResult(RESULT voteResult) {
        this.voteResult = voteResult;
    }

    public VoteResponse voteResult(RESULT voteResult) {
        this.voteResult = voteResult;
        return this;
    }

    public VoteResponse term(long term) {
        this.term = term;
        return this;
    }

    public enum RESULT {
        UNKNOWN,
        ACCEPT,
        REJECT_UNKNOWN_LEADER,
        REJECT_UNEXPECTED_LEADER,
        REJECT_EXPIRED_VOTE_TERM,
        REJECT_ALREADY_VOTED,
        REJECT_ALREADY_HAS_LEADER,
        REJECT_TERM_NOT_READY,
        REJECT_TERM_SMALL_THAN_LEDGER,
        REJECT_EXPIRED_TERM,
        REJECT_SMALL_LEDGER_END_INDEX;
    }

    public enum ParseResult {
        WAIT_TO_REVOTE,
        REVOTE_IMMEDIATELY,
        PASSED,
        WAIT_TO_VOTE_NEXT;
    }
}
