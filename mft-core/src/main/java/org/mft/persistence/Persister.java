package org.mft.persistence;

import org.mft.logic.Input;
import org.mft.objects.Header;
import org.mft.objects.Span;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class Persister {
   protected final Input input;

   protected Persister(Input input) {
      this.input = input;
   }

   protected Persister() {
      input = null;
   }

   public Input getInput() {
      return input;
   }

   public abstract void openForWrite(String path, Header header) throws IOException;

   public abstract Header openForRead() throws IOException;

   public abstract void write(Span span, boolean sort) throws IOException;

   public abstract void close() throws IOException;

   public abstract void read(Consumer<Span> spanConsumer, boolean loadEvents) throws IOException;

   public abstract int getPosition();
}
