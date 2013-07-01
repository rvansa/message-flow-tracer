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

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
* @author Radim Vansa &lt;rvansa@redhat.com&gt;
*/
public class Event {
   public enum Type {
      INCOMING_DATA("Incoming"),
      NEW_BATCH("NewBatch"),
      THREAD_HANDOVER_STARTED("THStarted"),
      THREAD_HANDOVER_SUCCESS("THSuccess"),
      THREAD_HANDOVER_FAILURE("THFailure"),
      THREAD_PROCESSING_COMPLETE("TPComplete"),
      OUTCOMING_DATA_STARTED("ODStarted"),
      OUTCOMING_DATA_FINISHED("ODFinished"),
      CHECKPOINT("Checkpoint"),
      MESSAGE_TAG("MsgTag"),
      TRACE_TAG("FlowTag"),
      STACKPOINT("Stackpoint"),
      MSG_PROCESSING_START("MsgProcStart"),
      MSG_PROCESSING_END("MsgProcEnd"),
      BATCH_PROCESSING_START("ProcessingBatch"),
      BATCH_PROCESSING_END("ProcessedBatch"),
      CONTAINS("Contains"),
      DISCARD("Discard"),
      RETRANSMISSION("Retransmission");

      private String name;

      Type(String name) {
         this.name = name;
      }

      @Override
      public String toString() {
         return name;
      }

      static Map<String, Type> nameToEnum = new HashMap<String, Type>();

      static {
         for (Type value : Type.values()) {
            nameToEnum.put(value.toString(), value);
         }
      }

      static Type get(String name) {
         return nameToEnum.get(name);
      }
   }

   private static SimpleDateFormat FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");

   public Date timestamp;
   public long nanoTime;
   public long roundingError;
   public String source;
   public int span;
   public String threadName;
   public Type type;
   public String text;
   //public DagVertex causalOrder = null;
   public long correctedTimestamp = 0;

   public Event(long originNanoTime, long originUnixTime, long nanoTime, String source, int span, String threadName, String type, String text) {
      this.type = Type.get(type);
      this.timestamp = new Date((nanoTime - originNanoTime) / 1000000 + originUnixTime);
      this.roundingError = (nanoTime - originNanoTime) % 1000000;
      this.nanoTime = nanoTime;
      this.source = source;
      this.span = span;
      this.threadName = threadName;//fromDictionary(threadName);
      this.text = text;//fromDictionary(message);
   }

   public static class GlobalTimestampComparator implements Comparator<Event> {
      @Override
      public int compare(Event o1, Event o2) {
         return o1.timestamp.compareTo(o2.timestamp);
      }
   }

   public static class LocalTimestampComparator implements Comparator<Event> {
      @Override
      public int compare(Event o1, Event o2) {
         return Long.compare(o1.nanoTime, o2.nanoTime);
      }
   }

   public static class CorrectedTimestampComparator implements Comparator<Event> {
      @Override
      public int compare(Event o1, Event o2) {
         return Long.compare(o1.correctedTimestamp, o2.correctedTimestamp);
      }
   }

   @Override
   public String toString() {
      return String.format("%s|%s|%s|%s|%s", FORMAT.format(timestamp), source, threadName, type, text);
   }
}
