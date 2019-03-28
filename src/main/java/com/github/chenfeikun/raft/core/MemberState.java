package com.github.chenfeikun.raft.core;

import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.rpc.entity.ResponseCode;
import com.github.chenfeikun.raft.utils.IOUtils;
import com.github.chenfeikun.raft.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * @desciption: MemberState
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class MemberState {

    private static final Logger LOG = LoggerFactory.getLogger(MemberState.class);

    /** static variable*/
    public static final String TERM_PERSIST_FILE = "currterm";
    public static final String TERM_PERSIST_KEY_TERM = "currTerm";
    public static final String TERM_PERSIST_KEY_VOTE_FOR = "voteLeader";

    /** internal variable*/
    private NodeConfig nodeConfig;

    private long currTerm;
    private String currVoteFor;

    private String group;
    private String selfId;
    private String peers;
    private Map<String, String> peerMap;
    private long knownMaxTermInGroup = -1;

    private long endTerm = -1;
    private long endIndex = -1;

    private Role role = Role.CANDIDATE;
    private String leaderId;

    public MemberState(NodeConfig config) {
        this.group = config.getGroup();
        this.selfId = config.getSelfId();
        this.peers = config.getPeers();
        for (String peerInfo : this.peers.split(";")) {
            peerMap.put(peerInfo.split("-")[0], peerInfo.split("-")[1]);
        }
        this.nodeConfig = config;
        loadTerm();
    }

    private void loadTerm() {
        try {
            String data = IOUtils.file2String(nodeConfig.getDefaultPath() + File.separator + TERM_PERSIST_FILE);
            Properties properties = IOUtils.string2Properties(data);
            if (properties.containsKey(TERM_PERSIST_KEY_TERM)) {
                currTerm = Long.valueOf(String.valueOf(properties.getProperty(TERM_PERSIST_KEY_TERM)));
            }
            if (properties.containsKey(TERM_PERSIST_KEY_VOTE_FOR)) {
                currVoteFor = String.valueOf(properties.getProperty(TERM_PERSIST_KEY_VOTE_FOR));
                if (currVoteFor.length() == 0) {
                    currVoteFor = null;
                }
            }
        } catch (IOException e) {
            LOG.error("Load current term failed");
        }
    }

    public synchronized long nextTerm() {
        PreConditions.check(role == Role.CANDIDATE, ResponseCode.ILLEGAL_MEMBER_STATE, "%s != %s", role,
                Role.CANDIDATE);
        if (knownMaxTermInGroup > currTerm) {
            currTerm = knownMaxTermInGroup;
        } else {
            currTerm++;
        }
        currVoteFor = null;
        persistTerm();
        return currTerm;
    }

    public boolean isPeerMember(String id) {
        return id != null && peerMap.containsKey(id);
    }

    private void persistTerm() {
        try {
            Properties properties = new Properties();
            properties.put(TERM_PERSIST_KEY_TERM, currTerm);
            properties.put(TERM_PERSIST_KEY_VOTE_FOR, currVoteFor == null ? "" : currVoteFor);
            String data = IOUtils.properties2String(properties);
            IOUtils.string2File(data, nodeConfig.getDefaultPath() + File.separator + TERM_PERSIST_FILE);
        } catch (Throwable t) {
            LOG.error("persist curr term failed", t);
        }
    }

    public int peerSize() {
        return peerMap.size();
    }

    public synchronized void changeToLeader(long term) {
        PreConditions.check(currTerm == term, ResponseCode.ILLEGAL_MEMBER_STATE);
        this.role = Role.LEADER;
        this.leaderId = selfId;
    }

    public synchronized void changeToCandidate(long term) {
        assert term >= currTerm;
        PreConditions.check(term >= currTerm, ResponseCode.ILLEGAL_MEMBER_STATE, "should %d >= %d", term, currTerm);
        if (term > knownMaxTermInGroup) {
            knownMaxTermInGroup = term;
        }
        //the currTerm should be promoted in handleVote thread
        this.role = Role.CANDIDATE;
        this.leaderId = null;
    }

    public boolean isQuorum(int num) {
        return num >= ((peerSize() / 2) + 1);
    }

    public String getSelfAddr() {
        return peerMap.get(selfId);
    }

    public long getCurrTerm() {
        return currTerm;
    }

    public void setCurrTerm(long currTerm) {
        this.currTerm = currTerm;
    }

    public String getCurrVoteFor() {
        return currVoteFor;
    }

    public void setCurrVoteFor(String currVoteFor) {
        this.currVoteFor = currVoteFor;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getSelfId() {
        return selfId;
    }

    public void setSelfId(String selfId) {
        this.selfId = selfId;
    }

    public String getPeers() {
        return peers;
    }

    public void setPeers(String peers) {
        this.peers = peers;
    }

    public Map<String, String> getPeerMap() {
        return peerMap;
    }

    public void setPeerMap(Map<String, String> peerMap) {
        this.peerMap = peerMap;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public boolean isFollower() {
        return role == Role.FOLLOWER;
    }

    public boolean isCandidate() {
        return role == Role.CANDIDATE;
    }

    public long getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(long endIndex) {
        this.endIndex = endIndex;
    }

    public long getEndTerm() {
        return endTerm;
    }

    public void setEndTerm(long endTerm) {
        this.endTerm = endTerm;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public enum Role {
        UNKNOWN,
        CANDIDATE,
        LEADER,
        FOLLOWER;
    }
}
