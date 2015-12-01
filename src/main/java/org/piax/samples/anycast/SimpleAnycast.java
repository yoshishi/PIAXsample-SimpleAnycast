package org.piax.samples.anycast;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Id;
import org.piax.common.ServiceId;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.LowerUpper;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.NetworkTimeoutException;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.sg.MSkipGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 単純な Anycast 実装
 * 
 * Group ID で識別される Anycast グループを単位として擬似 anycast を実現する。
 * 被 Anycast 側は register によりハンドルを取得し、 unregister によりハンドルを解放する。
 * ハンドルを介して被探索状態の有無効を切り換えることができる。
 * Anycast 側は search により指定された Group ID に属するノードに Anycast を行う。
 * 
 * 【実装】
 * 被 Anycast 側は Group ID に、ランダムに生成した suffix を付加したキーを SkipGraph に登録し、
 * 被探索状態となる。
 * Anycast 側は指定された Group ID にランダムに生成した suffix を付加したキーを探索キーとして
 * SkipGraph 上を LessThan 探索を行うことで Anycast を実現している。
 * 
 * @param <T> Anycast 時の引数の型
 * @param <R> Anycast 時の返り値の型
 */
public class SimpleAnycast<T extends Serializable, R extends Serializable> {
    private static final Logger logger = LoggerFactory.getLogger(SimpleAnycast.class);

    public static final ServiceId DEFAULT_SERVICE_ID = new ServiceId(SimpleAnycast.class.getSimpleName());
    public static int suffixLength = 16;    /** suffix のバイト長 */

    final ServiceId serviceId;

    final MSkipGraph<Destination, ComparableKey<?>> sg;

    /**
     * Group ID と対応するハンドラの Map
     */
    protected final ConcurrentHashMap<String, SimpleAnycastHandle<T, R>> am = new ConcurrentHashMap<>();

    /**
     * FutureQueue タイムアウト時間
     */
    public static int FUTUREQUEUE_GETNEXT_TIMEOUT = 10 * 1000;


    /**
     *  request に用いるクエリクラス
     */
    static class QueryPack<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        enum QPMethod {
            DISCOVER,
        }
        final QPMethod method;
        final SimpleAnycast.LTKey targetKey;
        final T value;

        /**
         * 
         * @param method
         * @param id 探索対象キー
         * @param value SimpleAnycastListener#onReceive に渡す任意のオブジェクト
         */
        QueryPack(QPMethod method, SimpleAnycast.LTKey id, T value) {
            if (method == null)
                throw new IllegalArgumentException("method should not be null");
            if (id == null)
                throw new IllegalArgumentException("id should not be null");

            this.method = method;
            this.targetKey = id;
            this.value = value;
        }

        @Override
        public String toString() {
            return "QueryPack [method=" + method + ", targetId=" + targetKey
                    + ", value=" + value + "]";
        }
    }

    /**
     * SkipGraph 上に登録するキー
     * prefix + suffix の組により順序が定義される
     */
    static class LTKey implements ComparableKey<LTKey> {
        private static final long serialVersionUID = 1L;

        private final String prefix;  // Prefix of key. not null not empty
        private final Id suffix;    // if null, this instance is an infinity key
        private boolean infinitePlus = true;    // false means infinite minus

        public LTKey(String prefix, Id suffix) {
            if (prefix == null || prefix.isEmpty())
                throw new IllegalArgumentException("prefix should not be null or empty");
            if (suffix == null)
                throw new IllegalArgumentException("suffix should not be null");

            this.prefix = prefix;
            this.suffix = suffix;
        }

        // for infinity key
        LTKey(String prefix, boolean plus) {
            if (prefix == null || prefix.isEmpty())
                throw new IllegalArgumentException("prefix should not be null or empty");
            this.prefix = prefix;
            this.suffix = null;     // infinity key
            this.infinitePlus = plus;
        }

        /**
         * get +infinity key for prefix
         * @param prefix
         * @return
         */
        public static LTKey getPlusInfinity(String prefix) {
            return new LTKey(prefix, true);
        }

        /**
         * get -infinity key for prefix
         * @param prefix
         * @return
         */
        public static LTKey getMinusInfinity(String prefix) {
            return new LTKey(prefix, false);
        }

        /**
         * get +infinity key of the instance
         * @return
         */
        public LTKey getPlusInfinity() {
            return new LTKey(prefix, true);
        }

        /**
         * get -infinity key of the instance
         * @return
         */
        public LTKey getMinusInfinity() {
            return new LTKey(prefix, false);
        }

        /**
         * getter for prefix
         * @return
         */
        public String getPrefix() {
            return prefix;
        }

        /**
         * getter for suffix
         * @return suffix or null. if null returns, it means this key is infinite key
         */
        public Id getSuffix() {
            return suffix;
        }

        public boolean isSamePrefix(LTKey val) {
            return this.prefix.equals(val.prefix);
        }

        @Override
        public int compareTo(LTKey val) {
            if (this == val)
                return 0;
            if (val == null)
                return 1;

            if (isSamePrefix(val)) {
                if (this.suffix == null) {
                    if (val.suffix == null) {
                        // 無限大小 vs 無限大小
                        return (this.infinitePlus ? 1 : 0) - (val.infinitePlus ? 1 : 0);
                    } else {
                        // 無限大小 vs その他
                        return (this.infinitePlus ? 1 : -1);
                    }
                } else {
                    if (val.suffix == null) {
                        // その他 vs 無限大小
                        return (val.infinitePlus ? -1 : 1);
                    } else {
                        // その他 vs その他
                        return this.suffix.compareTo(val.suffix);
                    }
                }
            } else {
                return this.prefix.compareTo(val.prefix);
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((prefix == null) ? 0 : prefix.hashCode());
            if (suffix == null) {
                result = result + (infinitePlus ? 1 : 0);
            } else {
                result = prime * result
                        + ((suffix == null) ? 0 : suffix.hashCode());
            }
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            LTKey other = (LTKey) obj;
            if (prefix == null) {
                if (other.prefix != null)
                    return false;
            } else if (!prefix.equals(other.prefix))
                return false;
            if (suffix == null) {
                if (other.suffix == null) {
                    return (this.infinitePlus == other.infinitePlus);
                } else {
                    return false;
                }
            } else if (!suffix.equals(other.suffix))
                return false;
            return true;
        }

        @Override
        public String toString() {
            if (suffix != null) {
                return "LTKey [prefix=" + prefix + ", suffix=" + suffix + "]";
            } else {
                return "LTKey [prefix=" + prefix + (this.infinitePlus ? " +inf]" : "-inf]");
            }
        }
    }


    /**
     * コンストラクタ
     * Service ID はデフォルト値が使用される
     * @param sg 基盤になる SkipGraph
     */
    public SimpleAnycast(MSkipGraph<Destination, ComparableKey<?>> sg) {
        this(sg, DEFAULT_SERVICE_ID);
    }

    /**
     * Service ID を指定するコンストラクタ
     * @param sg 基盤になる SkipGraph
     * @param sid sg 基盤になる SkipGraph
     */
    public SimpleAnycast(MSkipGraph<Destination, ComparableKey<?>> sg, ServiceId sid) {
        if (sg == null)
            throw new IllegalArgumentException("sg should not be null");
        if (sid == null)
            throw new IllegalArgumentException("sid should not be null");

        this.sg = sg;
        this.serviceId = sid;
        this.sg.setListener(serviceId, new OverlayListenerImpl());
    }


    /**
     * groupid の Anycast グループに登録し Anycast を受け付ける
     * @param groupid Anycast グループの ID (Group ID)
     * @param listener Anycast を受けた場合の callback nullable
     * @return
     * @throws IOException
     */
    public SimpleAnycastHandle<T, R> register(String groupid, SimpleAnycastListener<T, R> listener) throws IOException {
        if (groupid == null || groupid.isEmpty())
            throw new IllegalArgumentException("groupid should not be null or empty");

        synchronized (am) {
            if (am.containsKey(groupid))
                throw new IllegalStateException("groupid is already registered");

            LTKey handlekey = newRandomKey(groupid);
            SimpleAnycastHandle<T, R> result = new SimpleAnycastHandle<T, R>(this, handlekey, listener);
            result.setDiscoverable();   // may fail
            am.put(groupid, result);
            return result;
        }
    }

    /**
     * SimpleAnycastHandle h に対応する Anycast グループから離脱する
     * @param h
     * @throws IOException
     */
    public void unregister(SimpleAnycastHandle<T, R> h) throws IOException {
        if (h == null)
            throw new IllegalArgumentException("h should not be null");

        synchronized (am) {
            if (am.containsValue(h)) {
                h.setUndiscoverable();
                h.dispose();
                am.remove(h.getGroupId());
            }
        }
    }

    /**
     * Group ID を prefix とし、ランダムな suffix を持つキーの生成
     * @param groupid Group ID
     * @return
     */
    private LTKey newRandomKey(String groupid) {
        return new LTKey(groupid, Id.newId(suffixLength));
    }

    /**
     * 指定された GroupID に Anycast する
     * @param groupid Anycast 対象の Group ID
     * @param obj {@link SimpleAnycastListener#onReceive(String, Object)} に引数として渡される任意のオブジェクト
     * @return {@link SimpleAnycastListener#onReceive(String, Object)} の返り値が返る null 時はノード未発見
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    public R anycast(String groupid, T obj) throws ProtocolUnsupportedException, IOException {
        if (groupid == null || groupid.isEmpty())
            throw new IllegalArgumentException("groupid should not be null or empty");

        LTKey searchkey = newRandomKey(groupid);
        QueryPack<T> query = new QueryPack<>(QueryPack.QPMethod.DISCOVER, searchkey, obj);

        // (MIN, serachkey) の区間を探索する
        // KeyComparator.getMinusInfinity ではクラス単位の最大値最小値となるため wrap around 時に
        // LTKey の最大値が hit することになり不具合となる。
        // ここでは groupid 単位で端点を与える
        @SuppressWarnings({ "unchecked", "rawtypes" })
        KeyRange<?> range = new KeyRange(searchkey.getMinusInfinity(), false,
                searchkey, true);
        LowerUpper dst = new LowerUpper(range, false, 1);   // serachkey を越えない最大のキーを探索するための Destination
        FutureQueue<?> fq = sg.request(serviceId, serviceId, dst,
                query, 100);
        if (fq == null) {
            logger.warn("null FutureQueue");
            return null;
        }

        fq.setGetNextTimeout(FUTUREQUEUE_GETNEXT_TIMEOUT);
        boolean found = false;
        List<R> nodes = new ArrayList<>();
        for (RemoteValue<?> rv : fq) {
            if (rv == null) {
                // getNextでtimeoutした場合
                if (nodes.isEmpty()) {
                    fq.cancel();
                    throw new NetworkTimeoutException();
                }
            } else {
                found = true;
                @SuppressWarnings("unchecked")
                R info = (R) rv.getValue();
                if (info != null) {
                    nodes.add(info);
                }
            }
        }
        if (!found) {
            /*
             * 見つからない場合は、wraparoundさせたLowerUpper
             * をセットし、requestを再発行する。
             */
            logger.debug("do wraparound");

            // (serachkey, MAX) の区間を探索する
            // KeyComparator.getMinusInfinity ではクラス単位の最大値最小値となるため wrap around 時に
            // LTKey の最大値が hit することになり不具合となる。
            // ここでは groupid 単位で端点を与える
            @SuppressWarnings({ "unchecked", "rawtypes" })
            KeyRange<?> range2 = new KeyRange(searchkey, false, 
                    searchkey.getPlusInfinity(), false);
            dst = new LowerUpper(range2, false, 1);     // MAX を越えない最大のキーを探索するための Destination
            fq = sg.request(serviceId, serviceId, dst,
                    query, 100);
            if (fq == null) {
                logger.warn("null FutureQueue");
                return null;
            }

            fq.setGetNextTimeout(FUTUREQUEUE_GETNEXT_TIMEOUT);
            for (RemoteValue<?> rv : fq) {
                if (rv == null) {
                    // getNextでtimeoutした場合
                    if (nodes.isEmpty()) {
                        fq.cancel();
                        throw new NetworkTimeoutException();
                    }
                } else {
                    found = true;
                    @SuppressWarnings("unchecked")
                    R info = (R) rv.getValue();
                    if (info != null) {
                        nodes.add(info);
                    }
                }
            }
        }
        if (nodes.isEmpty()) {
            logger.warn("No avalable result");
            return null;
        }

        logger.debug("discoverPrevious returns {}", nodes.get(0));
        return nodes.get(0);
    }

    /**
     * OverlayListener impl
     */
    private class OverlayListenerImpl implements OverlayListener<Destination, ComparableKey<?>> {
        @Override
        public FutureQueue<?> onReceiveRequest(Overlay<Destination, ComparableKey<?>> ov,
                OverlayReceivedMessage<ComparableKey<?>> rmsg) {

            QueryPack<T> query = (QueryPack<T>) rmsg.getMessage();
            logger.debug("onReceiveRequest peerId:{} {}", sg.getPeerId(), query);
            assert query != null;

            if (query.method == QueryPack.QPMethod.DISCOVER) {
                R info = null;
                logger.debug("onReceiveRequest discovered:{}", query.targetKey);
                for (ComparableKey<?> c : rmsg.getMatchedKeys()) {
                    LTKey matchedkey = (LTKey) c;
                    logger.debug("onReceiveRequest discover matched:{}", matchedkey);

                    String groupid = matchedkey.getPrefix();
                    SimpleAnycastHandle<T, R> h = am.get(groupid);
                    if (h != null) {
                        try {
                            SimpleAnycastListener<T, R> listener = h.getListener();
                            if (listener != null)
                                info = listener.onReceive(groupid, (T) query.value);
                        } catch (Exception e) {
                            logger.error("", e);
                        }
                    } else {
                        // SkipGraph 上にキーがあるが、対応する Handle がない状態
                        // 通常は生じない
                        logger.warn("No handled key : {}", groupid);
                    }
                }
                logger.debug("onReceiveRequest discover result :{}", info.toString());

                RemoteValue<R> val = new RemoteValue<>(sg.getPeerId(), info);
                return FutureQueue.singletonQueue(val);
            } else {
                // サポート外クエリ
                assert false;
                return FutureQueue.emptyQueue();
            }
        }

        @Override
        public void onReceive(Transport<Destination> trans, ReceivedMessage rmsg) {
            logger.warn("Unexpected call : onReceive(Transport<LowerUpper> trans, ReceivedMessage rmsg)");
            throw new UnsupportedOperationException();
        }

        @Override
        public void onReceive(Overlay<Destination, ComparableKey<?>> ov,
                OverlayReceivedMessage<ComparableKey<?>> rmsg) {
            logger.warn("Unexpected call : onReceive(Overlay<Destination, ComparableKey<?>> ov, OverlayReceivedMessage<ComparableKey<?>> rmsg)");
            throw new UnsupportedOperationException();
        }
    }
}
