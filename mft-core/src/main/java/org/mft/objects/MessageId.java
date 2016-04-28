package org.mft.objects;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface MessageId {
   short from();

   /**
    * Span files with >2G messages are too huge to be processed anyway.
    */
   int id();

   class Impl implements MessageId{
      private final short from;
      private final int id;

      public Impl(short from, int id) {
         this.from = from;
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
