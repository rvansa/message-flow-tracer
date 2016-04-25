package org.jboss.qa.jdg.messageflow.persistence;

import org.jboss.qa.jdg.messageflow.objects.Span;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class Persister {
   private long nanoTime, unixTime;

   protected void setTimes(long nanoTime, long unixTime) {
      this.nanoTime = nanoTime;
      this.unixTime = unixTime;
   }

   public long getNanoTime() {
      return nanoTime;
   }

   public long getUnixTime() {
      return unixTime;
   }

   public abstract void open(String path, long nanoTime, long unixTime) throws IOException;

   public abstract void write(Span span, boolean sort) throws IOException;

   public abstract void close() throws IOException;

   public abstract void read(Consumer<Span> spanConsumer, boolean loadEvents) throws IOException;

   public abstract int getPosition();
}
