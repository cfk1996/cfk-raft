package com.github.chenfeikun.raft.rpc;

/**
 * @desciption: Response
 * @CreateTime: 2019-03-09
 * @author: chenfeikun
 */
public class Response<T> {

    private T result;

    public Response(T result) {
        this.result = result;
    }

    private Response(Builder<T> builder) {
        this.result = builder.result;
    }

    public static final class Builder<T> {
        private T result;

        private Builder() {}

        public Builder<T> result(T result) {
            this.result = result;
            return this;
        }

        public Response<T> build() {
            return new Response<>(this);
        }
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }
}
