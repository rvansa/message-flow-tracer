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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.jboss.qa.jdg.messageflow.objects.Event;
import org.jboss.qa.jdg.messageflow.objects.MessageId;
import org.jboss.qa.jdg.messageflow.objects.Trace;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class AnalyseTraces implements Processor {

   private static final String NO_TAG = "-no-tag-";
   private Map<String, TraceStats> traceStats = new TreeMap<String, TraceStats>();
   private PrintStream out = System.out;

   private static class TraceStats {
      final String tag;
      TreeMap<String, Integer> msgOccurrences = new TreeMap<String, Integer>();
      int traces;
      int sumNodes;

      AvgMinMax messages = new AvgMinMax();
      AvgMinMax threads = new AvgMinMax();
      AvgMinMax nanoTime = new AvgMinMax();
      AvgMinMax wallTime = new AvgMinMax();

      private TraceStats(String tag) {
         this.tag = tag;
      }
   }

   @Override
   public void process(Trace trace, long traceCounter) {
      String traceTag = NO_TAG;
      for (Event event : trace.events) {
         if (event.type == Event.Type.TRACE_TAG) {
            traceTag = (String) event.payload;
            break;
         }
      }
      TraceStats stats = traceStats.get(traceTag);
      if (stats == null) {
         stats = new TraceStats(traceTag);
         traceStats.put(traceTag, stats);
      }
      stats.messages.add(trace.messages.size());
      stats.traces++;
      for (MessageId message : trace.messages) {
         TreeSet<String> msgTags = new TreeSet<String>();
         for (Event event : trace.events) {
            if (event.type == Event.Type.MSG_PROCESSING_START && event.payload.equals(message)) {
               for (Event e : trace.events) {
                  if (e.type == Event.Type.MESSAGE_TAG && e.source == event.source && e.span == event.span) {
                     msgTags.add((String) e.payload);
                  }
               }
            }
         }
         String msgTag;
         if (msgTags.isEmpty()) {
            msgTag = NO_TAG;
         } else {
            StringBuilder allTags = new StringBuilder();
            for (Iterator<String> iterator = msgTags.iterator(); iterator.hasNext(); ) {
               allTags.append(iterator.next());
               if (iterator.hasNext()) allTags.append(", ");
            }
            msgTag = allTags.toString();
         }
         Integer occurrences = stats.msgOccurrences.get(msgTag);
         if (occurrences == null) {
            stats.msgOccurrences.put(msgTag, 1);
         } else {
            stats.msgOccurrences.put(msgTag, occurrences + 1);
         }
      }
      Map<String, Long> startTimestamps = new HashMap<String, Long>();
      Map<String, Long> lastTimestamps = new HashMap<String, Long>();
      int threads = 0;
      long nanoTime = 0;
      Set<String> nodes = new HashSet<String>();
      for (Event e : trace.events) {
         nodes.add(e.source);
         String nodeThread = e.source + "|" + e.threadName;
         Long timestamp = startTimestamps.get(nodeThread);
         if (timestamp == null) {
            startTimestamps.put(nodeThread, e.nanoTime);
         }
         if (e.type == Event.Type.THREAD_PROCESSING_COMPLETE) {
            Long start = startTimestamps.remove(nodeThread);
            if (start != null) {
               nanoTime += e.nanoTime - start;
               threads++;
            }
         }
         lastTimestamps.put(nodeThread, e.nanoTime);
      }
      for (Map.Entry<String, Long> entry : startTimestamps.entrySet()) {
         Long last = lastTimestamps.get(entry.getKey());
         if (last != null) {
            nanoTime += last - entry.getValue();
            threads++;
         }
      }
      stats.sumNodes += nodes.size();
      stats.nanoTime.add(nanoTime);
      stats.wallTime.add(trace.events.get(trace.events.size() - 1).timestamp.getTime() - trace.events.get(0).timestamp.getTime());
      stats.threads.add(threads);

   }

   @Override
   public void finish() {
      out.println("\n**********");
      out.println("* TRACES *");
      out.println("**********");
      for (TraceStats stats : traceStats.values()) {
         out.printf("%s:\t%d traces, avg %.2f msg (%d - %d)\n", stats.tag, stats.traces,
                    stats.messages.avg(), stats.messages.min(), stats.messages.max());
         for (Map.Entry<String, Integer> message : stats.msgOccurrences.entrySet()) {
            out.printf("\t%1.2fx: %s\n", (double) message.getValue() / stats.traces, message.getKey());
         }
         out.printf("\tavg %1.2f nodes, avg %.2f threads (%d - %d)\n", (double) stats.sumNodes / stats.traces,
                    stats.threads.avg(), stats.threads.min(), stats.threads.max());
         out.printf("\tProcessing took avg %.2f us (%.2f - %.2f)\n",
                    stats.nanoTime.avg() / 1000d, stats.nanoTime.min() / 1000d, stats.nanoTime.max() / 1000d);
         out.printf("\tWall time delta %.2f ms (%d - %d)\n\n",  stats.wallTime.avg(), stats.wallTime.min(), stats.wallTime.max());
      }
   }
}
