package org.mft.objects;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Message {
   private final int identityHashCode;
   private final MessageId messageId;

   public Message(MessageId messageId, int identityHashCode) {
      this.identityHashCode = identityHashCode;
      this.messageId = messageId;
   }

   public int identityHashCode() {
      return identityHashCode;
   }

   public MessageId id() {
      return messageId;
   }

   @Override
   public String toString() {
      return String.format("%d:%d, %08x", messageId.from(), messageId.id(), identityHashCode);
   }
}
