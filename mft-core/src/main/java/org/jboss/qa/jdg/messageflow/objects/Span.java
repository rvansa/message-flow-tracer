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

package org.jboss.qa.jdg.messageflow.objects;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
public class Span implements Serializable {
   private final transient Span parent;
   private String incoming;
   private List<String> outcoming;
   private Set<LocalEvent> events = new HashSet<LocalEvent>();
   protected transient List<Span> children = new ArrayList<Span>();

   private int counter = 1;
   private boolean nonCausal;

//   private static HashSet<Span> debugSpans = new HashSet<Span>();

   public static void debugPrintUnfinished() {
//      synchronized (debugSpans) {
//         for (Span span : debugSpans) {
//            if (span.getParent() == null || !debugSpans.contains(span.getParent())) {
//               span.writeWithChildren(System.err);
//            }
//         }
//      }
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

   public synchronized void addOutcoming(String identifier) {
      if (outcoming == null) {
         outcoming = new ArrayList<String>();
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
   public void decrementRefCount(Queue<Span> finishedSpans) {
      if (parent != null) {
         parent.decrementRefCount(finishedSpans);
         return;
      }
      synchronized (this) {
         counter--;
         if (counter == 0) {
            passToFinished(finishedSpans);
         } else if (counter < 0) {
            throw new IllegalStateException();
         }
      }
   }

   public void writeWithChildren(PrintStream out) {
      out.printf("%08x (parent %08x)", this.hashCode(), this.parent == null ? 0 : this.parent.hashCode());
      writeTo(out, true);
      out.println("<Begin children>");
      for (Span s : children) {
         s.writeWithChildren(out);
      }
      out.println("<End children>");
   }

    /**
     * add events, incoming and outcomming messages from parent, pass children to finished spans
     * @param finishedSpans
     */
   private void passToFinished(Queue<Span> finishedSpans) {
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
   public void addEvent(LocalEvent event){
      events.add(event);
   }

   public void setNonCausal() {
      this.nonCausal = true;
   }

   public boolean isNonCausal() {
      return nonCausal;
   }

   public void setIncoming(String incoming) {
      if (this.incoming != null) {
         //throw new IllegalArgumentException("Cannot have two incoming messages!");
          System.err.println("Cannot have two incoming messages. The current incoming message is " + this.incoming);
          return;
      }
      this.incoming = incoming;
   }

   /**
    * Writes span to output stream in binary format rather than plain text for performance improvement
    * It uses Java serialization which is inefficient
    * Sort is not implemented
    * @param stream output stream
    * @param sort not implemented
    */
   public synchronized void binaryWriteTo(ObjectOutputStream stream, boolean sort){
         try {
           stream.writeObject(this);
         } catch (IOException e) {
            e.printStackTrace();
         }
   }

   /**
    * Writes span to output stream in binary format rather than plain text for performance improvement
    * @param stream output stream
    * @param sort whether to sort events
    */
   public synchronized void binaryWriteTo(DataOutputStream stream, boolean sort){

      try {
         stream.writeBoolean(isNonCausal());
         if (incoming != null){
            stream.writeUTF(incoming);
         }else{
            stream.writeUTF("");
         }

         int outcommingCount = outcoming != null ? outcoming.size() : 0;
         stream.writeShort(outcommingCount);
         if (outcoming != null){
            for (String message : outcoming){
               stream.writeUTF(message);
            }
         }
         int eventCount = events != null ? events.size() : 0;
         stream.writeShort(eventCount);
         if (events != null){
            Collection<LocalEvent> events = this.events;
            if (sort) {
               LocalEvent[] array = events.toArray(new LocalEvent[events.size()]);
               Arrays.sort(array);
               events = Arrays.asList(array);
            }
            for (LocalEvent event : events){
               stream.writeLong(event.timestamp);
               stream.writeUTF(event.threadName);
               stream.writeShort(event.type.ordinal());
               stream.writeUTF(event.text == null ? "" : event.text);
            }

         }

      } catch (IOException e) {
         e.printStackTrace();
      }
   }
   /**
    * Writes span to output stream in text format.
    * @param stream output stream
    * @param sort whether to sort events
    */
   public synchronized void writeTo(PrintStream stream, boolean sort) {
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
      Collection<LocalEvent> events = this.events;
      if (sort) {
         LocalEvent[] array = events.toArray(new LocalEvent[events.size()]);
         Arrays.sort(array);
         events = Arrays.asList(array);
      }
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

   public Set<String> getMessages() {
      Set<String> messages = new HashSet<>();
      if (incoming != null)
         messages.add(incoming);
      if (outcoming != null){
         for (String msg : outcoming) {
            if (outcoming != null)
               messages.add(msg);
         }
      }
      return messages;
   }

   public Set<LocalEvent> getEvents() {
      return events;
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

   public Span getCurrent() {
      return this;
   }
   @Override
   public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append(nonCausal ? "SPAN: nonCasual" : "SPAN: casual");
      sb.append(" Incoming: " + incoming);
      if (outcoming != null){
         for (String message : outcoming){
            sb.append(" Outcomming: " + message);
         }
      }
      if (events != null){
         sb.append(System.lineSeparator());
         sb.append("Events:");
         sb.append(System.lineSeparator());
         for (LocalEvent event : events){
            sb.append(" timestamp: " + event.timestamp);
            sb.append(" threadName: " + event.threadName);
            sb.append(" type: " + event.type);
            sb.append(" text: " + event.text);
            sb.append(System.lineSeparator());
         }
      }

      return sb.toString();
   }
   public static class LocalEvent implements Comparable<LocalEvent>, Serializable {
      public long timestamp;
      public String threadName;
      public Event.Type type;
      public String text;


      public LocalEvent(){};
      private LocalEvent(Event.Type type, String text) {
         this.timestamp = System.nanoTime();
         this.threadName = Thread.currentThread().getName();
         this.type = type;
         this.text = text;
      }

      //@Override
      public int compareTo(LocalEvent o) {
         return Long.compare(timestamp, o.timestamp);
      }

   }

}
