package org.piax.samples.anycast.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.HashMap;

import org.piax.agent.AgentPeer;
import org.piax.agent.AgentTransportManager;
import org.piax.agent.DefaultAgentTransportManager;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.PeerLocator;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.samples.anycast.SimpleAnycast;
import org.piax.samples.anycast.SimpleAnycastHandle;
import org.piax.samples.anycast.SimpleAnycastListener;
import org.piax.samples.util.Util;
import org.piax.util.LocalInetAddrs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Anycast テスト用 shell
 * 
 * 起動オプション
 * -i <addr> Peer locator の設定
 * -s <addr> Seed locator の設定
 *
 * シェルコマンド
 * reg <groupid> groupid で指定された Anycast グループに入る
 * ureg <groupid> groupid で指定された Anycast グループから離脱する
 * cast <groupid> <msg> groupid で指定された Anycast グループに msg を anycast する
 * disable <groupid> groupid で指定された Anycast グループから一時離脱する
 * enable <groupid> groupid で指定された Anycast グループに復帰する
 * bye
 */
public class Shell {
    private static final Logger logger = LoggerFactory.getLogger(Shell.class);

    // 各種デフォルト値
    private static final int DEFAULT_MY_LOCATORPORT = 12367;   // PIAX ポート番号
    private static final InetAddress DEFAULT_MY_LOCATORADDR = LocalInetAddrs.choice();
    private static final String DEFAULT_PEER_LOCATOR = DEFAULT_MY_LOCATORADDR.getHostAddress() + ":" + DEFAULT_MY_LOCATORPORT;

    private PeerLocator peerLocator = null;
    private PeerLocator seedLocator = null;

    private boolean active = true;

    private AgentPeer peer = null;
    private SimpleAnycast<String, String> ha = null;

    public static void main(String[] args) {
        final Shell ss = new Shell();

        if (!ss.initSetting(args)) {
            printUsage();
            return;
        }

        try {
            // サーバ起動
            ss.start();

            // 終了処理登録
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        if (ss.isActive()) {
                            ss.stop();
                            logger.warn("Shell stopped in shutdown hook");
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(),e);
                    }
                }
            });

            HashMap<String, SimpleAnycastHandle<String, String>> handles = new HashMap<>();
            // コマンド入力待ち
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                try {
                    System.out.print("Input Command >");
                    String line = reader.readLine();
                    // バックグラウンド動作対策
                    if (line == null) {
                        Thread.sleep(1000);
                        continue;
                    }

                    if ("".equals(line.trim()))
                        continue;

                    String[] cmds = line.split("\\s+");

                    if ("bye".equals(cmds[0])) {
                        // bye : 終了
                        break;
                    } else if ("reg".equals(cmds[0])) {
                        // register
                        String gid = cmds[1];
                        SimpleAnycastHandle<String, String> h = ss.ha.register(gid, new SimpleAnycastListener<String, String>() {
                            @Override
                            public String onReceive(String groupid, String obj) {
                                String msg = "Received anycast for " + groupid + ", " + obj;
                                System.out.println(msg);
                                return ss.peer.getPeerId() + ":" + msg;
                            }
                        });
                        handles.put(gid, h);
                        System.out.println("registered for " + gid);
                    } else if ("ureg".equals(cmds[0])) {
                        // unregister
                        String gid = cmds[1];
                        SimpleAnycastHandle<String, String> h = handles.get(gid);
                        ss.ha.unregister(h);
                        System.out.println("unregistered for " + gid);
                    } else if ("cast".equals(cmds[0])) {
                        // anycast
                        String gid = cmds[1];
                        String arg = cmds[2];
                        System.out.println("anycast for "+ gid);
                        String result = ss.ha.anycast(gid, arg);
                        System.out.println("Result: " + result);
                    } else if ("disable".equals(cmds[0])) {
                        // undiscoverable
                        String gid = cmds[1];
                        SimpleAnycastHandle<String, String> h = handles.get(gid);
                        h.setUndiscoverable();
                        System.out.println("disable receiving for " + gid);
                    } else if ("enable".equals(cmds[0])) {
                        // discoverable
                        String gid = cmds[1];
                        SimpleAnycastHandle<String, String> h = handles.get(gid);
                        h.setDiscoverable();
                        System.out.println("enable receiving for " + gid);
                    } else {
                        System.out.println(" reg <groupid> groupid で指定された Anycast グループに入る");
                        System.out.println(" ureg <groupid> groupid で指定された Anycast グループから離脱する");
                        System.out.println(" cast <groupid> <msg> groupid で指定された Anycast グループに msg を anycast する");
                        System.out.println(" disable <groupid> groupid で指定された Anycast グループから一時離脱する");
                        System.out.println(" enable <groupid> groupid で指定された Anycast グループに復帰する");
                        System.out.println(" bye");
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                // サーバ停止
                ss.stop();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 設定の読み込みと内部パラメータのセットアップ
     * @param args コマンドライン引数
     * @return true 正常にセットアップ終了 false パラメータに異常あり
     */
    public synchronized boolean initSetting(String[] args) {
        try {
            String tmp_peer_locator = "";
            String tmp_seed_locator = "";

            boolean isFault = false;

            // コマンドライン引数の処理
            for (int i = 0; i < args.length; i++) {
                String arg = args[i].trim();
                if (args[i].startsWith("-")) {
                    switch (arg.charAt(1)) {
                    case 'i':
                        i++;
                        if (i < args.length) {
                            tmp_peer_locator = args[i];
                        }
                        break;
                    case 's':
                        i++;
                        if (i < args.length) {
                            tmp_seed_locator = args[i];
                        }
                        break;
                    case '?':
                    case 'h':
                        return false;
                    default:
                        logger.error("Found an undefined option : " + args[i]);
                        return false;
                    }
                }
            }

            // Setup instance fields.
            if (tmp_peer_locator.equals("")) {
                logger.warn("Peer locator is not specified. Choose appropriate address.");
                tmp_peer_locator = DEFAULT_PEER_LOCATOR;
            }
            if (tmp_seed_locator.equals("")) {
                logger.warn("Seed locator is not specified. Use my peerlocator.");
                tmp_seed_locator = tmp_peer_locator;
            }

            seedLocator = Util.parseLocator(tmp_seed_locator);
            peerLocator = Util.parseLocator(tmp_peer_locator);

            logger.info("Seed locator         : {}", seedLocator);
            logger.info("Peer locator         : {}", peerLocator);

            return !isFault;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }

    }

    private static void printUsage() {
        System.out.println("Usage: ReceiverShell [options]");
        System.out.println("  -i <addr> sets Peer locator");
        System.out.println("  -s <addr> sets Seed locator");
    }

    public synchronized void start() throws Exception {
        String peername = peerLocator.toString();
        AgentTransportManager tm = new DefaultAgentTransportManager(
                peername, peerLocator, seedLocator);

        peer = new AgentPeer(peername, tm, new File("."));

        MSkipGraph<Destination, ComparableKey<?>> sg = (MSkipGraph<Destination, ComparableKey<?>>) tm.getOverlay("MSG");

        ha = new SimpleAnycast<>(sg);

        logger.info("Peer ID       : {}", peer.getPeerId().toString());

        // crate agent
        peer.join();

        active = true;
    }

    public synchronized void stop() throws Exception {
        logger.info("Offline peer");
        peer.leave();
        logger.info("Finalize PIAX");
        peer.fin();

        active = false;
    }

    public synchronized boolean isActive() {
        return active;
    }
}
