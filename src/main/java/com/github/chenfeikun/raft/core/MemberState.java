package com.github.chenfeikun.raft.core;

import com.github.chenfeikun.raft.NodeConfig;
import com.github.chenfeikun.raft.utils.IOUtils;
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

    public enum Role {
        UNKNOWN,
        CANDIDATE,
        LEADER,
        FOLLOWER;
    }
}
