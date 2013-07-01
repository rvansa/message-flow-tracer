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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jboss.qa.jdg.messageflow.objects.Event;
import org.jboss.qa.jdg.messageflow.objects.Trace;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class AnalyseLocks implements Processor {

   private PrintStream out = System.out;

   private AvgMinMax lockingTraces = new AvgMinMax();
   private long nonLockingTraces;
   private long successfulLocks;
   private long failedLocks;

   private AvgMinMax succAcquireTime = new AvgMinMax();
   private AvgMinMax failAcquireTime = new AvgMinMax();
   private AvgMinMax lockHeldTime = new AvgMinMax();

   private static class Locking {
      Event lockAttempt;
      Event lockOk;
      Event lockFail;
      Event unlock;
   }

   @Override
   public void process(Trace trace, long traceCounter) {
      Map<String, Locking> lockings = new HashMap<String, Locking>();
      Set<Locking> finished = new HashSet<Locking>();
      Event[] events = trace.events.toArray(new Event[0]);
      for (int i = 0; i < events.length; ++i) {
         Event e = events[i];
         if (e.type != Event.Type.CHECKPOINT || e.text == null) continue;
         if (e.text.startsWith("LOCK ")) {
            String key = getLockKey(e);
            Locking locking = lockings.get(key);
            if (locking != null) throw new IllegalStateException();
            locking = new Locking();
            lockings.put(key, locking);
            locking.lockAttempt = e;
         } else if (e.text.startsWith("LOCK_OK") || e.text.startsWith("LOCK_FAIL")) {
            for (int j = i - 1; j >= 0; --j) {
               Event other = events[j];
               if (other.type == Event.Type.CHECKPOINT && other.source == e.source && other.threadName.equals(e.threadName)
                     && other.text != null && other.text.startsWith("LOCK ")) {
                  String key = getLockKey(other);
                  Locking locking = lockings.get(key);
                  if (locking == null) throw new IllegalStateException();
                  if (e.text.startsWith("LOCK_OK")) {
                     locking.lockOk = e;
                  } else {
                     locking.lockFail = e;
                     lockings.remove(key);
                     finished.add(locking);
                  }
                  break;
               }
            }
         } else if (e.text.startsWith("UNLOCK")) {
            int openBracket = e.text.indexOf('[');
            int closeBracket = e.text.indexOf(']');
            if (openBracket < 0 || closeBracket < 0 || openBracket >= closeBracket) {
               throw new IllegalStateException();
            }
            if (openBracket + 1 == closeBracket) continue;

            String keys = e.text.substring(openBracket + 1, closeBracket);
            for (String key : keys.split(",")) {
               Locking locking = lockings.remove(key.trim());
               if (locking == null) {
                  throw new IllegalStateException(keys + ": " + key);
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
         throw new IllegalStateException("Not unlocked: " + lockings.keySet());
      }
   }

   private String getLockKey(Event e) {
      return e.text.substring(5).trim();
   }

   @Override
   public void finish() {
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
}
