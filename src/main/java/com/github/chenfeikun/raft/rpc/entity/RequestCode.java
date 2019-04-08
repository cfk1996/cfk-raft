package com.github.chenfeikun.raft.rpc.entity;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashMap;
import java.util.Map;

/**
 * @desciption: RequestCode
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public enum RequestCode {

    UNKNOWN(-1, ""),
    METADATA(50000, ""),
    APPEND(50001, ""),
    GET(50002, ""),
    GET_SINGLE(50003, ""),
    VOTE(51001, ""),
    HEART_BEAT(51002, ""),
    PULL(51003, ""),
    PUSH(51004, "");

    private static Map<Integer, RequestCode> codeMap = new HashMap<>();

    static {
        for (RequestCode requestCode : RequestCode.values()) {
            codeMap.put(requestCode.code, requestCode);
        }
    }

    private int code;
    private String desc;

    RequestCode(int code, String msg) {
        this.code = code;
        this.desc = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public static RequestCode valueOf(int code) {
        RequestCode tmp = codeMap.get(code);
        if (tmp != null) {
            return tmp;
        } else {
            return UNKNOWN;
        }

    }
}
