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

package org.mft.objects;

import org.mft.persistence.Persistable;
import org.mft.persistence.Persister;
import org.mft.persistence.TextPersister;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
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
public class Span implements Serializable, Persistable {
   private final transient Span parent;
   private MessageId incoming;
   private List<MessageId> outcoming;
   private List<LocalEvent> events = new ArrayList<>();
   protected transient List<Span> children = new ArrayList<Span>();

   private int counter = 1;
   private boolean nonCausal;

//   private static HashSet<Span> debugSpans = new HashSet<Span>();

   public static void debugPrintUnfinished() {
//      System.err.println("DEBUG unfinished");
//      synchronized (debugSpans) {
//         for (Span span : debugSpans) {
//            if (span.getParent() == null || !debugSpans.contains(span.getParent())) {
//               span.print(System.err, "");
//            }
//         }
//      }
//      System.err.println("DEBUG unfinished end");
   }

   public Span() {
//      synchronized (debugSpans) {
//         debugSpans.add(this);
//      }
      this.parent = null;
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

   public MessageId getIncoming() {
      return incoming;
   }

   public synchronized void addOutcoming(MessageId identifier) {
      if (outcoming == null) {
         outcoming = new ArrayList<>();
      }
      outcoming.add(identifier);
   }

    /**
     * Increase counter in parent span, if it doesn't have a parent increase counter it this span.
     */
   public void incrementRefCount() {
      if (parent != null) {
         parent.incrementRefCount();
         return;
      }
      synchronized (this) {
         counter++;
      }
   }
    /**
     * Decrement counter in parent span, if it doesn't have a parent decrement counter it this span.
     * If counter is zero pass to finished
     */
   public void decrementRefCount(Queue<Persistable> persistenceQueue) {
      if (parent != null) {
         parent.decrementRefCount(persistenceQueue);
         return;
      }
      synchronized (this) {
         counter--;
         if (counter == 0) {
            persist(persistenceQueue);
         } else if (counter < 0) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            print(new PrintStream(stream), "");
            throw new IllegalStateException(new String(stream.toByteArray()));
         }
      }
   }

   public void print(PrintStream out, String prefix) {
      out.printf("%08x (parent %08x)", this.hashCode(), this.parent == null ? 0 : this.parent.hashCode());
      new TextPersister(out, prefix).write(this, true);
      if (!children.isEmpty()) {
         out.println(prefix + "<Begin children>");
         for (Span s : children) {
            s.print(out, prefix + "\t");
         }
         out.println(prefix + "<End children>");
      }
   }

    /**
     * add events, incoming and outcomming messages from parent, pass children to finished spans
     * @param persistenceQueue
     */
   private void persist(Queue<Persistable> persistenceQueue) {
//      synchronized (debugSpans) {
//         debugSpans.remove(this);
//      }
      if (parent != null) {
         if (parent == this) {
            throw new IllegalStateException();
         }
         for (LocalEvent e : parent.events) {
            events.add(e);
         }
         if (parent.incoming != null) {
            setIncoming(parent.incoming);
         }
         if (parent.outcoming != null) {
            for (MessageId msg : parent.outcoming) {
               addOutcoming(msg);
            }
         }
      }
      if (children.isEmpty()) {
//         System.err.printf("%08x finished\n", this.hashCode());
         persistenceQueue.add(this);
      } else {
         boolean causalChildren = false;
         for (Span child : children) {
            if (!child.isNonCausal()) {
               causalChildren = true;
            }
            child.persist(persistenceQueue);
         }
         if (!causalChildren) {
            persistenceQueue.add(this);
//            System.err.printf("%08x finished\n", this.hashCode());
         }
      }
   }

   public synchronized void addEvent(Event.Type type, Object payload) {
      events.add(new LocalEvent(type, payload));
   }
   public void addEvent(LocalEvent event){
      events.add(event);
   }

   public void setNonCausal() {
      this.nonCausal = true;
   }

   public boolean isNonCausal() {
      return nonCausal;
   }

   public void setIncoming(MessageId incoming) {
      if (this.incoming != null) {
         //throw new IllegalArgumentException("Cannot have two incoming messages!");
          System.err.println("Cannot have two incoming messages. The current incoming message is " + this.incoming + ", span is " + this);
          new Throwable().fillInStackTrace().printStackTrace();
          return;
      }
      this.incoming = incoming;
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
      return lastTag == null ? null : String.valueOf(lastTag.payload);
   }

   public Set<MessageId> getMessages() {
      Set<MessageId> messages = new HashSet<>();
      if (incoming != null)
         messages.add(incoming);
      if (outcoming != null){
         for (MessageId msg : outcoming) {
            if (outcoming != null)
               messages.add(msg);
         }
      }
      return messages;
   }

//   public Set<LocalEvent> getEvents() {
//      return events;
//   }

   /* Debugging only */
   public String getTraceTag() {
      for (LocalEvent event : events) {
         if (event.type == Event.Type.TRACE_TAG) {
            return String.valueOf(event.payload);
         }
      }
      return "-no-trace-tag-";
   }

   public Span getCurrent() {
      return this;
   }

   @Override
   public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append(nonCausal ? "SPAN: nonCasual" : "SPAN: causal");
      sb.append(" Incoming: " + incoming);
      if (outcoming != null){
         for (MessageId message : outcoming){
            sb.append(" Outcomming: " + message);
         }
      }
      if (events != null){
         sb.append(System.lineSeparator());
         sb.append("Events:");
         sb.append(System.lineSeparator());
         for (LocalEvent event : events){
            sb.append(" timestamp: " + event.timestamp);
            sb.append(" threadId: " + event.threadId);
            sb.append(" type: " + event.type);
            sb.append(" payload: " + event.payload);
            sb.append(System.lineSeparator());
         }
      }

      return sb.toString();
   }

   public List<LocalEvent> getEvents() {
      return events;
   }

   public List<MessageId> getOutcoming() {
      return outcoming;
   }

   public List<LocalEvent> getSortedEvents() {
      Span.LocalEvent[] array = events.toArray(new Span.LocalEvent[events.size()]);
      Arrays.sort(array);
      return Arrays.asList(array);
   }

   @Override
   public void accept(Persister persister) throws IOException {
      persister.write(this, false);
   }

   public static class LocalEvent implements Comparable<LocalEvent>, Serializable {
      public long timestamp;
      public long threadId;
      public Event.Type type;
      public Object payload;

      public LocalEvent(){};

      public LocalEvent(long timestamp, long threadId, Event.Type type, Object payload) {
         this.timestamp = timestamp;
         this.threadId = threadId;
         this.type = type;
         this.payload = payload;
      }

      private LocalEvent(Event.Type type, Object payload) {
         this.timestamp = System.nanoTime();
         this.threadId = Thread.currentThread().getId();
         this.type = type;
         this.payload = payload;
      }

      //@Override
      public int compareTo(LocalEvent o) {
         return Long.compare(timestamp, o.timestamp);
      }

   }

}
