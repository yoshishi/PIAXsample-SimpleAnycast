package org.piax.samples.anycast;

import java.io.IOException;
import java.io.Serializable;

/**
 * 被 Anycast 側で保持するハンドルクラス
 * 
 * このハンドルを介して被探索の有無効を切り換えることができる
 *
 * @param <T> Anycast 時の引数の型
 * @param <R> Anycast 時の返り値の型
 */
public class SimpleAnycastHandle<T extends Serializable, R extends Serializable> {
    private final SimpleAnycast<T, R> simpleAnyCast;
    private SimpleAnycast.LTKey myKey;
    private volatile SimpleAnycastListener<T, R> listener;

    private volatile boolean discoverable = false;  // 被探索フラグ true:有効 false:無効
    private volatile boolean avalable = true;       // 破棄フラグ dispose 後は false

    SimpleAnycastHandle(SimpleAnycast<T, R> simpleAnyCast, SimpleAnycast.LTKey handlekey,
            SimpleAnycastListener<T, R> listener) {
        if (simpleAnyCast == null)
            throw new IllegalArgumentException("simpleAnyCast should not be null");
        if (handlekey == null)
            throw new IllegalArgumentException("handlekey should not be null");

        this.simpleAnyCast = simpleAnyCast;
        this.myKey = handlekey;
        this.listener = listener;
    }

    /**
     * Anycast Group ID を取得する
     * @return
     */
    public String getGroupId() {
        return myKey.getPrefix();
    }

    /**
     * 被探索時の callback を取得する
     * @return
     */
    public SimpleAnycastListener<T, R> getListener() {
        return this.listener;
    }

    /**
     * 被探索時の callback を設定する
     * @param new_listener 新たな callback (null可)
     * @return これまで設定されていた callback
     */
    public SimpleAnycastListener<T, R> setListener(SimpleAnycastListener<T, R> new_listener) {
        if (!avalable)
            throw new IllegalStateException("This handle is already disposed");
        SimpleAnycastListener<T, R> prev_listener = this.listener;
        this.listener = new_listener;
        return prev_listener;
    }

    /**
     * 被探索可能とする
     * @throws IOException
     */
    public synchronized void setDiscoverable() throws IOException {
        if (!avalable)
            throw new IllegalStateException("This handle is already disposed");
        if (discoverable)
            return;
        this.simpleAnyCast.sg.addKey(this.simpleAnyCast.serviceId, myKey);
        discoverable = true;
    }

    /**
     * 探索されないようにする
     * @throws IOException
     */
    public synchronized void setUndiscoverable() throws IOException {
        if (!avalable)
            throw new IllegalStateException("This handle is already disposed");
        if (!discoverable)
            return;
        this.simpleAnyCast.sg.removeKey(this.simpleAnyCast.serviceId, myKey);
        discoverable = false;
    }

    /**
     * このハンドルを破棄する
     * dispose 呼び出し後、このハンドルの状態変更が生じるメソッドを呼ぶと IllegalStateException が生じる
     * {@link SimpleAnycast#unregister(SimpleAnycastHandle)} により呼び出される
     * @throws IOException 
     */
    synchronized void dispose() throws IOException {
        if (!discoverable)
            return;
        this.simpleAnyCast.sg.removeKey(this.simpleAnyCast.serviceId, myKey);
        discoverable = false;
        avalable = false;
    }
}