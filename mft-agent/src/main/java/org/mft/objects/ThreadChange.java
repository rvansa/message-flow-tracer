package org.mft.objects;

import org.mft.persistence.Persistable;
import org.mft.persistence.Persister;

import java.io.IOException;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ThreadChange implements Persistable {
   private final String threadName;
   private final long nanoTime;
   private final long id;

   public ThreadChange(String threadName, long nanoTime, long id) {
      this.threadName = threadName;
      this.nanoTime = nanoTime;
      this.id = id;
   }

   public String getThreadName() {
      return threadName;
   }

   public long getNanoTime() {
      return nanoTime;
   }

   public long getId() {
      return id;
   }

   @Override
   public void accept(Persister persister) throws IOException {
      persister.write(this);
   }
}
