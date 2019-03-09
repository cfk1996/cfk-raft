package com.github.chenfeikun.raft.rpc;

import java.io.Serializable;

/**
 * @desciption: Request
 * @CreateTime: 2019-03-09
 * @author: chenfeikun
 */
public class Request<T> implements Serializable {

    /** 请求投票 */
    public static final int R_VOTE = 0;
    /** 附加日志 */
    public static final int A_ENTRIES = 1;
    /** 客户端 */
    public static final int CLIENT_REQ = 2;
    /** 配置变更. add*/
    public static final int CHANGE_CONFIG_ADD = 3;
    /** 配置变更. remove*/
    public static final int CHANGE_CONFIG_REMOVE = 4;

    /* 请求类型*/
    private int type;
    /* 请求体*/
    private T obj;
    /* server地址*/
    private String url;

    public Request(int type, T obj, String url) {
        this.type = type;
        this.obj = obj;
        this.url = url;
    }

    private Request(Builder<T> builder) {
        this.type = builder.type;
        this.obj = builder.obj;
        this.url = builder.url;
    }

    public static final <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    public static final class Builder<T> {

        private int type;
        private T obj;
        private String url;

        private Builder() {}

        public Builder<T> type(int type) {
            this.type = type;
            return this;
        }

        public Builder<T> obj(T obj) {
            this.obj = obj;
            return this;
        }

        public Builder<T> url(String url) {
            this.url = url;
            return this;
        }

        public Request<T> build() {
            return new Request<>(this);
        }
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public T getObj() {
        return obj;
    }

    public void setObj(T obj) {
        this.obj = obj;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public static void main(String[] args) {
        Builder<String> builder = Request.newBuilder();
        Request<String> request = builder.url("12.2.").type(2).obj("sd").build();
        System.out.println(request.getObj());
    }
}
