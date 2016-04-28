package org.mft;

import org.mft.objects.MessageId;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Identifier extends Protocol {
   static final short PROTOCOL_ID = 0x1DE1;
   private static short nodeId;
   private static AtomicInteger counter = new AtomicInteger();

   static {
      Integer nodeId = Integer.getInteger("org.mft.nodeId");
      if (nodeId == null) {
         throw new IllegalStateException("This node does not have identifier set.");
      }
      Identifier.nodeId = nodeId.shortValue();
      ClassConfigurator.add(PROTOCOL_ID, Header.class);
   }

   public static void install(ProtocolStack protocolStack) throws Exception {
      protocolStack.insertProtocol(new Identifier(), ProtocolStack.ABOVE, TP.class);
   }

   public Identifier() {
      id = PROTOCOL_ID;
   }

   @Override
   public Object down(Event evt) {
      if (evt.getType() == Event.MSG) {
         Message msg = (Message) evt.getArg();
         msg.putHeader(id, new Header(nodeId, counter.getAndIncrement()));
      }
      return super.down(evt);
   }

   @Override
   protected boolean accept(Message msg) {
      return false;
   }

   public static class Header extends org.jgroups.Header implements MessageId {
      private short node;
      private int id;

      public Header() {}

      public Header(short node, int id) {
         this.node = node;
         this.id = id;
      }

      @Override
      public int size() {
         return 10;
      }

      @Override
      public void writeTo(DataOutput out) throws Exception {
         out.writeShort(nodeId);
         out.writeInt(id);
      }

      @Override
      public void readFrom(DataInput in) throws Exception {
         node = in.readShort();
         id = in.readInt();
      }

      @Override
      public String toString() {
         final StringBuilder sb = new StringBuilder();
         sb.append(node).append(':').append(id);
         return sb.toString();
      }

      @Override
      public short from() {
         return node;
      }

      @Override
      public int id() {
         return id;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Header header = (Header) o;

         if (node != header.node) return false;
         return id == header.id;

      }

      @Override
      public int hashCode() {
         int result = (int) node;
         result = 31 * result + id;
         return result;
      }
   }
}
