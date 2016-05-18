package org.mft.objects;

/**
 * Wrapper with reference equality operations.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public final class Annotation {
   private final Object wrapped;

   public static Annotation of(Object o) {
      return new Annotation(o);
   }

   private Annotation(Object wrapped) {
      this.wrapped = wrapped;
   }

   @Override
   public int hashCode() {
      return System.identityHashCode(wrapped);
   }

   @Override
   public boolean equals(Object obj) {
      if (obj instanceof Annotation) {
         return ((Annotation) obj).wrapped == wrapped;
      } else {
         return false;
      }
   }

   @Override
   public String toString() {
      return wrapped.toString();
   }

   public Object unwrap() {
      return wrapped;
   }
}
