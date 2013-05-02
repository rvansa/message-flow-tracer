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
class ControlFlow {
   private final ControlFlow parent;
   private String incoming;
   private Set<String> outcoming;
   private Set<LocalEvent> events = new HashSet<LocalEvent>();
   private List<ControlFlow> children = new ArrayList<ControlFlow>();

   private int counter = 1;
   private boolean retransmission;
   private boolean threadLocalOnly = true;

   public ControlFlow() {
      parent = null;
   }

   public ControlFlow(ControlFlow parent) {
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

   public synchronized void incrementRefCount() {
      counter++;
      //System.err.printf("INC %08x -> %d\n", this.hashCode(), counter);
   }

   public void decrementRefCount(Queue<ControlFlow> finishedFlows) {
      if (parent != null) {
         parent.decrementRefCount(finishedFlows);
         return;
      }
      synchronized (this) {
         counter--;
         //System.err.printf("DEC %08x -> %d\n", this.hashCode(), counter);
         if (counter == 0) {
            passToFinished(finishedFlows);
         }
      }
   }

   private void passToFinished(Queue<ControlFlow> finishedFlows) {
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
         finishedFlows.add(this);
      } else {
         for (ControlFlow child : children) {
            child.passToFinished(finishedFlows);
         }
      }
   }

   public synchronized void addEvent(Event.Type type, String text) {
      events.add(new LocalEvent(type, text));
   }

   public void setNonCausal(boolean retransmission) {
      this.retransmission = retransmission;
   }

   public boolean isRetransmission() {
      return retransmission;
   }

   public void setIncoming(String incoming) {
      if (this.incoming != null) {
         throw new IllegalArgumentException("Cannot have two incoming messages!");
      }
      this.incoming = incoming;
   }

   public synchronized void writeTo(PrintStream stream) {
      if (isRetransmission()) {
         stream.print(MessageFlow.NON_CAUSAL);
      } else {
         stream.print(MessageFlow.CONTROL_FLOW);
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
         stream.print(MessageFlow.EVENT);
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

   public ControlFlow getParent() {
      return parent;
   }

   public String getLastMsgTag() {
      LocalEvent lastTag = null;
      for (LocalEvent event : events) {
         if (event.type == Event.Type.MESSAGE_TAG && (lastTag == null || event.timestamp > lastTag.timestamp)) {
            lastTag = event;
         }
      }
      return lastTag == null ? null : lastTag.text;
   }

   public void setThreadLocalOnly(boolean threadLocalOnly) {
      this.threadLocalOnly = threadLocalOnly;
   }

   public boolean isThreadLocalOnly() {
      return threadLocalOnly;
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
