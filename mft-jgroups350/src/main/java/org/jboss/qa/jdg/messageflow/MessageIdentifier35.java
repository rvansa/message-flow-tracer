package org.jboss.qa.jdg.messageflow;


import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.FD_SOCK;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.protocols.pbcast.STABLE;

import org.jgroups.Message;
import org.jgroups.util.MessageBatch;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by rmacor on 10/21/14.
 */


public class MessageIdentifier35 extends MessageIdentifier33 {


    // type field was public in earlier version of jgroups
    public static String getMessageIdentifier(Message msg) {
        Map<Short, Header> headers = msg.getHeaders();
        UNICAST2.Unicast2Header unicast2header = (UNICAST2.Unicast2Header) headers.get((short) 40);
        if (unicast2header != null) {
            return String.format("%s|%s|U%d:%d", msg.getSrc(), msg.getDest(), unicast2header.getType(), unicast2header.getSeqno());
        }
        NakAckHeader2 nakAckHeader2 = (NakAckHeader2) headers.get((short) 57);
        if (nakAckHeader2 != null) {
            return String.format("%s|M%d:%d", msg.getSrc(), nakAckHeader2.getType(), nakAckHeader2.getSeqno());
        }
        FD_ALL.HeartbeatHeader fdHeader = (FD_ALL.HeartbeatHeader) headers.get((short) 29);
        if (fdHeader != null) {
            return String.format("%s|FD_ALL%d", msg.getSrc(), System.currentTimeMillis() / 1000);
        }
        STABLE.StableHeader stableHeader = (STABLE.StableHeader) headers.get((short) 16);
        if (stableHeader != null) {
            return String.format("%s|STABLE%d", msg.getSrc(), System.currentTimeMillis() / 1000);
        }
        PingHeader pingHeader = (PingHeader) headers.get((short) 6);
        if (pingHeader != null) {
            return String.format("%s|PING%d:%d", msg.getSrc(), pingHeader.type(), System.currentTimeMillis() / 1000);
        }
        FD_SOCK.FdHeader fdSockHeader = (FD_SOCK.FdHeader) headers.get((short) 3);
        if (fdSockHeader != null) {
            return String.format("%s|%s|FD_SOCK:%d", msg.getSrc(), msg.getDest(), System.currentTimeMillis() / 1000);
        }
        return String.format("%s|%s|%s", msg.getSrc(), msg.getDest(), msg.printHeaders());
    }

    //is using getMessageIdentifier
    public static String getEventIdentifier(Object e) {
        //Should be type checked ?
        Event event = (Event) e;
        switch (event.getType()) {
            case Event.MSG:
                return getMessageIdentifier((Message) event.getArg());
            default:
                return "Event:" + event.getType();
        }
    }
    public static List<String> getMessageIdentifiers(MessageBatch batch) {
        List<String> list = new ArrayList<String>(batch.size());
        for (Message m : batch.array()) {
            if (m != null) {
                list.add(getMessageIdentifier(m));
            }
        }
        return list;
    }
    public static List<String> identifyMyHandler(Runnable r, Class<?> clazz) throws NoSuchFieldException, IllegalAccessException {
        Field msgField = clazz.getDeclaredField("msg");
        msgField.setAccessible(true);
        Message msg = (Message) msgField.get(r);
        return Collections.singletonList(getMessageIdentifier(msg));
    }

    public static String getBatchIdentifier(MessageBatch batch) {
        StringBuilder sb = new StringBuilder();
        Message[] messages = batch.array();
        int i = 0;
        for (; i < messages.length; ++i) {
            if (messages[i] != null) {
                sb.append(getMessageIdentifier(messages[i]));
                ++i;
                break;
            }
        }
        for (; i < messages.length; ++i) {
            if (messages[i] != null) {
                sb.append("||");
                sb.append(getMessageIdentifier(messages[i]));
            }
        }
        return sb.toString();
    }
    public static List<String> getDataIdentifiers(Runnable r) {
        try {
            Class<?> clazz = r.getClass();
            if (clazz.getSimpleName().equals("MyHandler")) {
                return identifyMyHandler(r, clazz);
            } else if (clazz.getSimpleName().equals("BatchHandler")) {
                return identifyBatchHandler(r, clazz);
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        return org.jboss.qa.jdg.messageflow.MessageIdentifier32.getDataIdentifiers(r);
    }
}
