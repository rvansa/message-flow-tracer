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

package org.jboss.qa.jdg.messageflow.processors;

import java.io.PrintStream;
import java.util.*;

import org.jboss.qa.jdg.messageflow.objects.Event;
import org.jboss.qa.jdg.messageflow.objects.Header;
import org.jboss.qa.jdg.messageflow.objects.Trace;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class AnalyseLocks implements Processor {

   static final String[] NO_KEYS = new String[0];
   private PrintStream out = System.out;

   private AvgMinMax lockingTraces = new AvgMinMax();
   private long nonLockingTraces;
   private long successfulLocks;
   private long failedLocks;

   private AvgMinMax succAcquireTime = new AvgMinMax();
   private AvgMinMax failAcquireTime = new AvgMinMax();
   private AvgMinMax lockHeldTime = new AvgMinMax();

    //private Map<String, Locking> lockings = new HashMap<String, Locking>();

    private List<Trace> traces = new ArrayList<Trace>();
   private static class Locking {
      Event lockAttempt;
      Event lockOk;
      Event lockFail;
      Event unlock;
   }
   private String inspectedKey = "";

   @Override
   /*public void process(Trace trace, long traceCounter){
        traces.add(trace);
   }
   */

   public void process(Trace trace, long traceCounter) {
      Map<String, Locking> lockings = new HashMap<String, Locking>();
      Set<Locking> finished = new HashSet<Locking>();
      Event[] events = trace.events.toArray(new Event[0]);
      for (int i = 0; i < events.length; ++i) {
         Event e = events[i];
         if (e.type != Event.Type.CHECKPOINT || e.payload == null) continue;
         String text = (String) e.payload;
         if (text.startsWith("LOCK ")) {
            addLock(lockings, e, parseKey(text));
         } else if (text.startsWith("LOCK_ALL ")) {
            for (String key : parseKeys(text)) {
               addLock(lockings, e, key);
            }
         } else if (text.startsWith("LOCK_OK") || text.startsWith("LOCK_FAIL")) {
            for (int j = i - 1; j >= 0; --j) {
               Event other = events[j];
               if (other.type == Event.Type.CHECKPOINT && other.source == e.source && other.threadName.equals(e.threadName)
                  && other.payload != null && ((String) other.payload).startsWith("LOCK ")) {
                  String key = parseKey((String) other.payload);
                  boolean successful = text.startsWith("LOCK_OK");
                  setLockCompleted(lockings, finished, e, key, successful);
                  break;
               }
            }
         } else if (text.startsWith("LOCK_ALL_OK")) {
            for (int j = i - 1; j >= 0; --j) {
               Event other = events[j];
               if (other.type == Event.Type.CHECKPOINT && other.source == e.source && other.threadName.equals(e.threadName)
                  && other.payload != null && ((String) other.payload).startsWith("LOCK_ALL ")) {
                  for (String key : parseKeys(((String) other.payload))) {
                     setLockCompleted(lockings, finished, e, key, true);
                     break;
                  }
               }
            }
         } else if (text.startsWith("UNLOCK")) {
            for (String key : parseKeys(text)) {
               Locking locking = lockings.remove(key.trim());
                if (key.equals(inspectedKey)){
                    System.out.println("Removing key from lockings key (UNLOCK): " + key + " timestamp:" + e.nanoTime + " corrected timestamp: " + e.correctedTimestamp);
                }
               if (locking == null) {
                  throw new IllegalStateException("Trying to unlock: The key isn't in the lockings " + key + " time: " + e.nanoTime);
               }
               locking.unlock = e;
               finished.add(locking);
            }
         }
      }
      for (Locking l : finished) {
         if (l.lockOk != null) {
            succAcquireTime.add(l.lockOk.nanoTime - l.lockAttempt.nanoTime);
            lockHeldTime.add(l.unlock.nanoTime - l.lockOk.nanoTime);
            successfulLocks++;
         } else {
            failAcquireTime.add(l.lockFail.nanoTime - l.lockAttempt.nanoTime);
            failedLocks++;
         }
      }
      if (finished.isEmpty()) {
         nonLockingTraces++;
      } else {
         lockingTraces.add(finished.size());
      }
      if (!lockings.isEmpty()) {
         //throw new IllegalStateException("Not unlocked: " + lockings.keySet());
         System.err.println("Lockings is not empty at the end of the trace");
      }

   }

   @Override
   public void processHeader(Header header) {
   }

   public void setLockCompleted(Map<String, Locking> lockings, Set<Locking> finished, Event e, String key, boolean successful) {
      Locking locking = lockings.get(key);
      if (locking == null)
         throw new IllegalStateException("The key does not exists in lockings. key: " + key);
      if (successful) {
         locking.lockOk = e;
      } else {
         locking.lockFail = e;
         lockings.remove(key);
         if (key.equals(inspectedKey)) {
            System.out.println("Removing key from lockings key (LOCK FAILED): " + key + " timestamp:" + e.nanoTime + " corrected timestamp: " + e.correctedTimestamp);
         }
         finished.add(locking);
      }
   }

   public void addLock(Map<String, Locking> lockings, Event e, String key) {
      Locking locking = lockings.get(key);
      if (locking != null)
         throw new IllegalStateException("The key already exists in lockings. key: " + key + " time: " + e.nanoTime);
      locking = new Locking();
      lockings.put(key, locking);
      if (key.equals(inspectedKey)) {
         System.out.println("Putting key into lockings key: " + key + " timestamp:" + e.nanoTime + " corrected timestamp: " + e.correctedTimestamp);
      }
      locking.lockAttempt = e;
   }

   public String[] parseKeys(String string) {
      int openBracket = string.indexOf('[');
      int closeBracket = string.indexOf(']');
      if (openBracket < 0 || closeBracket < 0 || openBracket >= closeBracket) {
         throw new IllegalStateException("brackets don't match");
      }
      if (openBracket + 1 == closeBracket) return NO_KEYS;

      String keys = string.substring(openBracket + 1, closeBracket);
      return keys.split(",");
   }

   private String parseKey(String string) {
      return string.substring(5).trim();
   }

   @Override
   public void finish() {
  /*      sortTraces();
        for (int i = 0; i < traces.size(); i++){
            actual_process(traces.get(i),0);
        }
*/
    //   if (!lockings.isEmpty()) {
      //     throw new IllegalStateException("Not unlocked: " + lockings.keySet());
     //  }
      out.println("\n*********");
      out.println("* LOCKS *");
      out.println("*********");
      out.printf("%d locking traces with %.2f locks (%d - %d), %d non-locking traces\n",
                 lockingTraces.count(), lockingTraces.avg(), lockingTraces.min(), lockingTraces.max(), nonLockingTraces);
      double succPercent = (100d * successfulLocks) / (successfulLocks + failedLocks);
      double failPercent = (100d * failedLocks) / (successfulLocks + failedLocks);
      out.printf("%d successful locks (%.2f%%), %d failed attempts (%.2f%%)\n",
                 successfulLocks, succPercent, failedLocks, failPercent);
      out.printf("Acquiring lock takes %.2f us (%.2f - %.2f)\n",
                 succAcquireTime.avg() / 1000d, succAcquireTime.min() / 1000d, succAcquireTime.max() / 1000d);
      if (failedLocks > 0) {
         out.printf("Failing to acquire lock takes %.2f us (%.2f - %.2f)\n",
                    failAcquireTime.avg() / 1000d, failAcquireTime.min() / 1000d, failAcquireTime.max() / 1000d);
      }
      out.printf("Lock is held for %.2f us (%.2f - %.2f)\n",
                 lockHeldTime.avg() / 1000d, lockHeldTime.min() / 1000d, lockHeldTime.max() / 1000d);
   }

    private void sortTraces() {
        Collections.sort(traces, new Comparator<Trace>() {
            @Override
            public int compare(Trace o1, Trace o2) {
                return o1.events.get(0).timestamp.compareTo(o2.events.get(0).timestamp);
            }
        });
    }
}
