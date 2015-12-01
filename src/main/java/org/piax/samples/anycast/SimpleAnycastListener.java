package org.piax.samples.anycast;

import java.io.Serializable;

/**
 * Anycast された際の callback
 * @param <T> 引数の型
 * @param <R> 返り値の型
 */
// Java8
// @FunctionInterface
public interface SimpleAnycastListener<T extends Serializable, R extends Serializable> {
    /**
     * Anycast を受けた際に callback される
     * @param groupid Anycast された Group ID
     * @param obj {@link SimpleAnycast#anycast(String, Serializable)} に与えられた引数
     * @return {@link SimpleAnycast#anycast(String, Serializable)} に返す値
     */
    public R onReceive(String groupid, T obj);
}