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

package org.jboss.qa.jdg.messageflow;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
* Sequence of operations on one node. The control flow may span multiple threads,
* may execute operations in parallel in multiple threads but never leaves the node.
*
* @author Radim Vansa &lt;rvansa@redhat.com&gt;
*/
class Span {
   private final Span parent;
   private String incoming;
   private Set<String> outcoming;
   private Set<LocalEvent> events = new HashSet<LocalEvent>();
   private List<Span> children = new ArrayList<Span>();

   private int counter = 1;
   private boolean nonCausal;
   private boolean threadLocalOnly = true;

//   private static HashSet<Span> debugSpans = new HashSet<Span>();

   public static void debugPrintUnfinished() {
//      synchronized (debugSpans) {
//         for (Span span : debugSpans) {
//            System.err.printf("%08x = ", span.hashCode());
//            span.writeTo(System.err);
//         }
//      }
   }

   public Span() {
//      synchronized (debugSpans) {
//         debugSpans.add(this);
//      }
      parent = null;
   }

   public Span(Span parent) {
//      synchronized (debugSpans) {
//         debugSpans.add(this);
//      }
      this.parent = parent;
      synchronized (parent) {
         parent.children.add(this);
      }
   }

   public synchronized void addOutcoming(String identifier) {
      if (outcoming == null) {
         outcoming = new HashSet<String>();
      }
      outcoming.add(identifier);
   }

   public void incrementRefCount() {
      if (parent != null) {
         parent.incrementRefCount();
         return;
      }
      synchronized (this) {
         counter++;
         //System.err.printf("INC %08x -> %d\n", this.hashCode(), counter);
      }
   }

   public void decrementRefCount(Queue<Span> finishedSpans) {
      if (parent != null) {
         parent.decrementRefCount(finishedSpans);
         return;
      }
      synchronized (this) {
         counter--;
         //System.err.printf("DEC %08x -> %d\n", this.hashCode(), counter);
         if (counter == 0) {
            passToFinished(finishedSpans);
         } else if (counter < 0) {
            writeWithChildren(System.err);
            throw new IllegalStateException();
         }
      }
   }

   private void writeWithChildren(PrintStream out) {
      out.printf("%08x", this.hashCode());
      writeTo(out);
      out.println("<Begin children>");
      for (Span s : children) {
         s.writeWithChildren(out);
      }
      out.println("<End children>");
   }

   public void decrementOrRetire(Queue<Span> finishedSpans) {
      if (threadLocalOnly) {
//         synchronized (debugSpans) {
//            debugSpans.remove(this);
//         }
         finishedSpans.add(this);
      } else {
         decrementRefCount(finishedSpans);
      }
   }

   private void passToFinished(Queue<Span> finishedSpans) {
//      synchronized (debugSpans) {
//         debugSpans.remove(this);
//      }
      if (parent != null) {
         for (LocalEvent e : parent.events) {
            events.add(e);
         }
         if (parent.incoming != null) {
            setIncoming(parent.incoming);
         }
         if (parent.outcoming != null) {
            for (String msg : parent.outcoming) {
               addOutcoming(msg);
            }
         }
      }
      if (children.isEmpty()) {
         //System.err.printf("%08x finished\n", this.hashCode());
         finishedSpans.add(this);
      } else {
         boolean causalChildren = false;
         for (Span child : children) {
            if (!child.isNonCausal()) {
               causalChildren = true;
            }
            child.passToFinished(finishedSpans);
         }
         if (!causalChildren) {
            finishedSpans.add(this);
         }
      }
   }

   public synchronized void addEvent(Event.Type type, String text) {
      events.add(new LocalEvent(type, text));
   }

   public void setNonCausal(boolean retransmission) {
      this.nonCausal = retransmission;
   }

   public boolean isNonCausal() {
      return nonCausal;
   }

   public void setIncoming(String incoming) {
      if (this.incoming != null) {
         throw new IllegalArgumentException("Cannot have two incoming messages!");
      }
      this.incoming = incoming;
   }

   public synchronized void writeTo(PrintStream stream) {
      if (isNonCausal()) {
         stream.print(Trace.NON_CAUSAL);
      } else {
         stream.print(Trace.SPAN);
      }
      stream.print(';');
      stream.print(incoming);
      if (outcoming != null) {
         for (String msg : outcoming) {
            stream.print(';');
            stream.print(msg);
         }
      }
      stream.println();
      for (LocalEvent evt : events) {
         stream.print(Trace.EVENT);
         stream.print(';');
         stream.print(evt.timestamp);
         stream.print(';');
         stream.print(evt.threadName);
         stream.print(';');
         stream.print(evt.type);
         stream.print(';');
         stream.println(evt.text == null ? "" : evt.text);
      }
   }

   public Span getParent() {
      return parent;
   }

   /* Debugging only */
   public String getLastMsgTag() {
      LocalEvent lastTag = null;
      for (LocalEvent event : events) {
         if (event.type == Event.Type.MESSAGE_TAG && (lastTag == null || event.timestamp > lastTag.timestamp)) {
            lastTag = event;
         }
      }
      return lastTag == null ? null : lastTag.text;
   }

   /* Debugging only */
   public String getTraceTag() {
      for (LocalEvent event : events) {
         if (event.type == Event.Type.TRACE_TAG) {
            return event.text;
         }
      }
      return "-no-trace-tag-";
   }


   public synchronized boolean isThreadLocalOnly() {
      return threadLocalOnly;
   }

   public void setThreadLocalOnly(boolean threadLocalOnly) {
      this.threadLocalOnly = threadLocalOnly;
   }

   private static class LocalEvent {
      public long timestamp;
      public String threadName;
      public Event.Type type;
      public String text;


      private LocalEvent(Event.Type type, String text) {
         this.timestamp = System.nanoTime();
         this.threadName = Thread.currentThread().getName();
         this.type = type;
         this.text = text;
      }
   }
}
