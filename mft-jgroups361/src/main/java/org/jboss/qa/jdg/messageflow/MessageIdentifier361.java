package org.jboss.qa.jdg.messageflow;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by rmacor on 3/10/15.
 */
public class MessageIdentifier361 extends MessageIdentifier35{

    //Unicast3 doesn't have type Header.type in JGroups 3.5...
    public static String getMessageIdentifier(Message msg) {
        Map<Short, Header> headers = msg.getHeaders();

        UNICAST3.Header unicast3header = (UNICAST3.Header) headers.get((short) 64);
        if (unicast3header != null) {
            return String.format("%s|%s|U%d:%d", msg.getSrc(), msg.getDest(), unicast3header.type(), unicast3header.seqno());
        }
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
    //Replacing ExposedInputStream with ByteArrayStream
    public static List<String> getDataIdentifiers(Runnable r) {
        try {
            Class<?> clazz = r.getClass();
            if (clazz.getSimpleName().equals("MyHandler")) {
                return identifyMyHandler(r, clazz);
            } else if (clazz.getSimpleName().equals("BatchHandler")) {
                return identifyBatchHandler(r, clazz);
            } else if (clazz.getSimpleName().equals("IncomingPacket")) {
                return identifyIncomingPacket(r, clazz);
            }

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<String> identifyIncomingPacket(Runnable r, Class<?> clazz) throws NoSuchFieldException, IllegalAccessException {
        Field bufField = clazz.getDeclaredField("buf");
        bufField.setAccessible(true);
        byte[] buf = (byte[]) bufField.get(r);
        Field offsetField = clazz.getDeclaredField("offset");
        offsetField.setAccessible(true);
        int offset = (Integer) offsetField.get(r);
        Field lengthField = clazz.getDeclaredField("length");
        lengthField.setAccessible(true);
        int length = (Integer) lengthField.get(r);
        return unmarshall(buf, offset, length);
    }
    private static List<String> unmarshall(byte[] buf, int offset, int length) {
        short                        version;
        byte                         flags;
        DataInput in_stream;// = null;
        try {
            in_stream=new ByteArrayDataInputStream(buf, offset, length);
            version=in_stream.readShort();
            flags=in_stream.readByte();
            boolean isMessageList=(flags & LIST) == LIST;
            boolean multicast=(flags & MULTICAST) == MULTICAST;
            if(isMessageList) { // used if message bundling is enabled
                return readMessageList(in_stream);
            }
            else {
                return readMessage(in_stream);
            }
        }
        catch(Throwable t) {
            System.err.println(t);
            t.printStackTrace(System.err);
            return null;
        }
    }
    protected static List<String> readMessage(DataInput instream) throws Exception {
        Message msg = new Message(false); // don't create headers, readFrom() will do this
        msg.readFrom(instream);
        return Collections.singletonList(getMessageIdentifier(msg));
    }
    protected static List<String> readMessageList(DataInput in) throws Exception {
        List<String> list=new ArrayList<String>();
        Address dest = Util.readAddress(in);
        Address src = Util.readAddress(in);
        while(in.readBoolean()) {
            Message msg = new Message(false);
            msg.readFrom(in);
            msg.setDest(dest);
            if(msg.getSrc() == null)
                msg.setSrc(src);
            list.add(getMessageIdentifier(msg));
        }
        return list;
    }
}
