/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.qa.jdg.messageflow;

import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.FD_SOCK;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.ExposedByteArrayInputStream;
import org.jgroups.util.Util;

/**
 * Retrieves message types from binary data and provides short (but hopefully unique) identifiers of the messages.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class MessageUnmarshaller {
   private static final byte LIST=1; // we have a list of messages rather than a single message when set
   private static final byte MULTICAST=2; // message is a multicast (versus a unicast) message when set
   private static final byte OOB=4; // message has OOB flag set (Message.OOB)

   public static List<String> getDataIdentifiers(Runnable r) {
      try {
         Class<?> clazz = r.getClass();
         if (!clazz.getSimpleName().equals("IncomingPacket")) {
            return null;
         }
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
      } catch (NoSuchFieldException e) {
         throw new IllegalArgumentException("NoSuchField", e);
      } catch (IllegalAccessException e) {
         throw new RuntimeException(e);
      }
   }

   public static byte[] getDataBuffer(Runnable r) {
      try {
         Class<?> clazz = r.getClass();
         if (!clazz.getSimpleName().equals("IncomingPacket")) {
            return null;
         }
         Field bufField = clazz.getDeclaredField("buf");
         bufField.setAccessible(true);
         return (byte[]) bufField.get(r);
      } catch (NoSuchFieldException e) {
         throw new IllegalArgumentException("NoSuchField", e);
      } catch (IllegalAccessException e) {
         throw new RuntimeException(e);
      }
   }

   private static List<String> unmarshall(byte[] buf, int offset, int length) {
      short                        version;
      byte                         flags;
      ExposedByteArrayInputStream in_stream;
      DataInputStream dis=null;

      try {
         in_stream=new ExposedByteArrayInputStream(buf, offset, length);
         dis=new DataInputStream(in_stream);
         version=dis.readShort();

         flags=dis.readByte();
         boolean isMessageList=(flags & LIST) == LIST;
         boolean multicast=(flags & MULTICAST) == MULTICAST;

         if(isMessageList) { // used if message bundling is enabled
            return readMessageList(dis);
         }
         else {
            return readMessage(dis);
         }
      }
      catch(Throwable t) {
         System.err.println(t);
         t.printStackTrace(System.err);
         return null;
      }
      finally {
         Util.close(dis);
      }
   }

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
         return String.format("%s|PING%d:%d", msg.getSrc(), pingHeader.type, System.currentTimeMillis() / 1000);
      }
      FD_SOCK.FdHeader fdSockHeader = (FD_SOCK.FdHeader) headers.get((short) 3);
      if (fdSockHeader != null) {
         return String.format("%s|%s|FD_SOCK:%d", msg.getSrc(), msg.getDest(), System.currentTimeMillis() / 1000);
      }
      return String.format("%s|%s|%s", msg.getSrc(), msg.getDest(), msg.printHeaders());
   }

   protected static List<String> readMessageList(DataInputStream in) throws Exception {
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

   protected static List<String> readMessage(DataInputStream instream) throws Exception {
      Message msg = new Message(false); // don't create headers, readFrom() will do this
      msg.readFrom(instream);
      return Collections.singletonList(getMessageIdentifier(msg));
   }
}
