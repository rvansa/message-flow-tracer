package org.mft.objects;

import java.util.Optional;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface MessageId {
   short NODE_ID = Optional.ofNullable(Integer.getInteger("org.mft.nodeId"))
      .orElseThrow(() -> new IllegalStateException("This node does not have identifier set.")).shortValue();

   short from();

   /**
    * Span files with >2G messages are too huge to be processed anyway.
    */
   int id();

   class Impl implements MessageId{
      private final short from;
      private final int id;

      public Impl(int id) {
         this.from = NODE_ID;
         this.id = id;
      }

      public Impl(short from, int id) {
         this.from = from;
         this.id = id;
      }

      // hack when id is not unique across destinations
      public Impl(short from, short to, int id) {
         this.from = (short) ((from << 5) ^ (to << 10));
         this.id = id;
      }

      @Override
      public short from() {
         return from;
      }

      @Override
      public int id() {
         return id;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Impl impl = (Impl) o;

         if (from != impl.from) return false;
         return id == impl.id;

      }

      @Override
      public int hashCode() {
         int result = (int) from;
         result = 31 * result + id;
         return result;
      }

      @Override
      public String toString() {
         return new StringBuilder().append(from).append(':').append(id).toString();
      }
   }
}
