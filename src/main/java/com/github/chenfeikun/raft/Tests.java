package com.github.chenfeikun.raft;

/**
 * @desciption: Tests
 * @CreateTime: 2019-04-04
 * @author: chenfeikun
 */
public class Tests {

    private Object o = new Object();

    public void printa() {
        synchronized (o) {
            System.out.println("----a----");
            printb();
            System.out.println("------------------");
        }
    }

    public void printb() {
        synchronized (o) {
            System.out.println("--------b-----------");
        }
    }

    public static void main(String[] args) {
        Tests t = new Tests();
        t.printa();
    }
}
