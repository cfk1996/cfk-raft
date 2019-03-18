package com.github.chenfeikun.raft.common;

/**
 * @desciption: Copiable mark interface
 * @CreateTime: 2019-03-17
 * @author: chenfeikun
 */
public interface Copiable<T> {

    /**
     * deep clone
     * @return
     */
    T copy();
}
