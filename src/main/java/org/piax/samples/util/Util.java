package org.piax.samples.util;

import java.net.InetSocketAddress;

import org.piax.common.PeerLocator;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.util.LocalInetAddrs;

public class Util {
    /**
     * 文字列からLocatorを生成する
     * 
     * (addr):(port)  TcpLocator  ex) 192.168.0.10:12368
     * t(addr):(port) TcpLocator  ex) t192.168.0.10:12368
     * u(addr):(port) UdpLocator  ex) u192.168.0.10:12368
     * e(pprt) EmuLocator         ex) e12368
     * 
     * @param locator [e|u|t][host:]port
     * @return
     */
    public static PeerLocator parseLocator(String locator) {
        if (locator == null)
            throw new NullPointerException("locator should not be null");
        if (locator.isEmpty())
            throw new IllegalArgumentException("locator should not be empty");

        Net net = Net.TCP;
        String host = "";
        int portno = -1;

        locator = locator.trim().toLowerCase();
        if (locator.startsWith("e")) {
            net = Net.EMU;
            locator = locator.substring(1);
        } else if (locator.startsWith("u")) {
            net = Net.UDP;
            locator = locator.substring(1);
        } else if (locator.startsWith("t")) {
            net = Net.TCP;
            locator = locator.substring(1);
        }
        String[] locEle = locator.split(":");
        host = locEle[0];
        portno = Integer.parseInt(locEle[locEle.length - 1]);
        if (locEle.length == 1) {
            host = LocalInetAddrs.choice().getHostAddress();
        }

        if (net == Net.EMU) {
            return new EmuLocator(portno);
        } else if (net == Net.TCP) {
            return new TcpLocator(new InetSocketAddress(host, portno));
        } else if (net == Net.UDP) {
            return new UdpLocator(new InetSocketAddress(host, portno));
        } else {
            throw new IllegalArgumentException("Invalid locator type : " + net);
        }
    }

    public enum Net {
        EMU,
        UDP,
        TCP
    }
}

