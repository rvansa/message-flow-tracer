package org.mft.persistence;

import org.mft.objects.Header;
import org.mft.objects.Span;
import org.mft.objects.ThreadChange;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class Persister {
   protected final Input input;
   protected Consumer<Span> spanConsumer;
   protected boolean loadEvents;
   protected Consumer<ThreadChange> threadChangeConsumer;

   protected Persister(Input input) {
      this.input = input;
   }

   protected Persister() {
      input = null;
   }

   public Input getInput() {
      return input;
   }

   public void setSpanConsumer(Consumer<Span> spanConsumer, boolean loadEvents) {
      this.spanConsumer = spanConsumer;
      this.loadEvents = loadEvents;
   }

   public void setThreadChangeConsumer(Consumer<ThreadChange> threadChangeConsumer) {
      this.threadChangeConsumer = threadChangeConsumer;
   }

   public abstract void openForWrite(String path, Header header) throws IOException;

   public abstract Header openForRead() throws IOException;

   public abstract void write(Span span, boolean sort) throws IOException;

   public abstract void close() throws IOException;

   public abstract void read() throws IOException;

   public abstract int getPosition();

   public abstract void write(ThreadChange threadChange) throws IOException;
}
