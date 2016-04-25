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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.jboss.qa.jdg.messageflow.objects.Event;
import org.jboss.qa.jdg.messageflow.objects.MessageId;
import org.jboss.qa.jdg.messageflow.objects.Trace;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class AnalyseMessages implements Processor {

   private static final String NO_TAG = "-no-tag-";
   private Map<String, TaggedStats> stats = new TreeMap<String, TaggedStats>();
   private int notSent;
   private int notReceived;
   private PrintStream out = System.out;

   private static class TaggedStats {

      public String tag;
      public boolean merged;
      public Map<String, Average> processingDelay = new TreeMap<String, Average>();
      public Map<String, RouteStats> routeStats = new TreeMap<String, RouteStats>();

      public TaggedStats(String tag) {
         this.tag = tag;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         TaggedStats that = (TaggedStats) o;
         return mapEquals(processingDelay, that.processingDelay) && mapEquals(routeStats, that.routeStats);
      }

      private static <TKey, TValue> boolean mapEquals(Map<TKey, TValue> m1, Map<TKey, TValue> m2) {
         if (m1.size() != m2.size()) return false;
         for (Map.Entry<TKey, TValue> entry : m1.entrySet()) {
            if (!entry.getValue().equals(m2.get(entry.getKey()))) return false;
         }
         return true;
      }
   }

   private static class RouteStats {
      /*
       * The partial results have no actual meaning, the real average difference is only average of the route
       * from first node to second and from second to first (we assume symmetric latency).
       */
      Average transportDiffs = new Average();
      /*
       * Those that were received but not sent
       */
      int duplicates;
      /*
       * Received, but also received before
       */
      int ignored;
      /*
       * Discarded during thread change
       */
      int discarded;
      /*
       * Sent but never received.
       */
      int lost;
      /*
       * Processed (and tagged)
       */
      int uniqueMessages = 0;

      int totalSent = 0;
      int totalReceived = 0;

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         RouteStats that = (RouteStats) o;

         if (discarded != that.discarded) return false;
         if (duplicates != that.duplicates) return false;
         if (ignored != that.ignored) return false;
         if (lost != that.lost) return false;
         if (uniqueMessages != that.uniqueMessages) return false;
         if (!transportDiffs.equals(that.transportDiffs)) return false;

         return true;
      }

      public void printTo(PrintStream out) {
         out.printf("%9d unique, %9d total sent, %9d total received, %5d discarded, %5d ignored, %5d lost, %5d duplicate",
                    uniqueMessages, totalSent, totalReceived, discarded, ignored, lost, duplicates);
      }
   }

   private static class Average {
      private long sum = 0;
      private int count = 0;

      public void add(long value) {
         sum += value;
         ++count;
      }

      public long get() {
         return sum/count;
      }

      public double getFloat() {
         return (double) sum / (double) count;
      }

      public long getSum() {
         return sum;
      }

      public int getCount() {
         return count;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         Average average = (Average) o;
         return count == average.count && sum == average.sum;
      }
   }

   @Override
   public void process(Trace trace, long traceCounter) {
      for (MessageId message : trace.messages) {
         ArrayList<Event> sent = new ArrayList<Event>();
         Map<String, ArrayList<Event>> incoming = new HashMap<String, ArrayList<Event>>();
         Map<String, TaggedStats> stats = new HashMap<String, TaggedStats>();
         // iterating in the time order
         for (Event event : trace.events) {
            if (event.payload == null || !event.payload.equals(message)) continue;
            if (event.type == Event.Type.OUTCOMING_DATA_STARTED || event.type == Event.Type.RETRANSMISSION) {
               sent.add(event);
            } else if (event.type == Event.Type.MSG_PROCESSING_START || event.type == Event.Type.DISCARD) {
               ArrayList<Event> incomingForSource = incoming.get(event.source);
               if (incomingForSource == null) {
                  incomingForSource = new ArrayList<Event>();
                  incoming.put(event.source, incomingForSource);
               }
               Event e = event;
               //TODO: document why not for discard (currently I don't remember)
               if (event.type == Event.Type.MSG_PROCESSING_START) {
                  e = findIncoming(trace.events, event.source, event.span);
               }
               incomingForSource.add(e);
               stats.putAll(findTaggedStats(trace.events, event.source, event.span));
            }
         }
         if (stats.isEmpty()) {
            stats.put(NO_TAG, getTaggedStats(NO_TAG));
         }
         for (Event event : trace.events) {
            if (event.payload == null || !event.payload.equals(message)) continue;
            if (event.type == Event.Type.MSG_PROCESSING_START) {
               Event rx = findIncoming(trace.events, event.source, event.span);
               for (TaggedStats ts : stats.values()) {
                  getAverage(ts.processingDelay, rx.source).add(event.nanoTime - rx.nanoTime);
               }
            }
         }
         // let's assume that if the message was really lost, it was the first message, if there are
         // more receptions, these are in the end and combination of these two is rare
         for (ArrayList<Event> incomingForSource : incoming.values()) {
            int shiftIndex = Math.max(sent.size() - incomingForSource.size(), 0);
            if (sent.isEmpty()) {
               // we don't know who sent that, cannot produce any stats
               continue;
            }

            if (incomingForSource.get(0) == null) {
                 incomingForSource.remove(0);
                 sent.remove(0);
                 // cannot produce any stats
                 continue;
            }
            String route = sent.get(0).source + "|" + incomingForSource.get(0).source;
            for (int i = 0; i + shiftIndex < sent.size(); ++i) {
               Event tx = sent.get(i + shiftIndex);
               Event rx = incomingForSource.get(i);
               long diff = rx.nanoTime - tx.nanoTime;
               for (TaggedStats ts : stats.values()) {
                  getRouteStats(ts.routeStats, route).transportDiffs.add(diff);
               }
            }
            int discarded = 0;
            for (Event event : incomingForSource) {
               if (event.type == Event.Type.DISCARD) {
                  discarded++;
               }
            }
            for (TaggedStats ts : stats.values()) {
               RouteStats routeStats = getRouteStats(ts.routeStats, route);
               routeStats.duplicates += Math.max(incomingForSource.size() - sent.size(), 0);
               routeStats.lost += shiftIndex;
               routeStats.ignored += incomingForSource.size() - discarded - 1;
               routeStats.discarded += discarded;
               routeStats.uniqueMessages++;
               routeStats.totalSent = sent.size();
               routeStats.totalReceived = incomingForSource.size();
            }
         }
         if (incoming.isEmpty()) {
            notReceived++;
         }
         if (sent.isEmpty()) {
            notSent++;
         }
      }
   }

   private Average getAverage(Map<String, Average> map, String key) {
      Average avg = map.get(key);
      if (avg == null) {
         avg = new Average();
         map.put(key, avg);
      }
      return avg;
   }

   private RouteStats getRouteStats(Map<String, RouteStats> map, String key) {
      RouteStats avg = map.get(key);
      if (avg == null) {
         avg = new RouteStats();
         map.put(key, avg);
      }
      return avg;
   }

   private Map<String, TaggedStats> findTaggedStats(Collection<Event> events, String source, int span) {
      Map<String, TaggedStats> tags = new HashMap<String, TaggedStats>();
      for (Event event : events) {
         if (event.source == source && event.span == span && event.type == Event.Type.MESSAGE_TAG) {
            TaggedStats stats = getTaggedStats((String) event.payload);
            tags.put((String) event.payload, stats);
         }
      }
      return tags;
   }

   private TaggedStats getTaggedStats(String tag) {
      TaggedStats stats = this.stats.get(tag);
      if (stats == null) {
         stats = new TaggedStats(tag);
         this.stats.put(tag, stats);
      }
      return stats;
   }

   private Event findIncoming(Collection<Event> events, String source, int span) {
      for (Event event : events) {
         if (event.type == Event.Type.INCOMING_DATA && event.source == source && event.span == span) {
            return event;
         }
      }
      return null;
   }

   private Event findSent(Collection<Event> events, String message) {
      for (Event event : events) {
         if (event.type == Event.Type.OUTCOMING_DATA_STARTED && event.payload.equals(message)) {
            return event;
         }
      }
      return null;
   }

   @Override
   public void finish() {
      out.println("\n************");
      out.println("* MESSAGES *");
      out.println("************");
      for (TaggedStats ts1 : this.stats.values()) {
         if (ts1.merged) continue;
         for (TaggedStats ts2 : this.stats.values()) {
            if (ts1 != ts2 && ts1.equals(ts2)) {
               ts1.tag = ts1.tag + ", " + ts2.tag;
               ts2.merged = true;
            }
         }
      }
      for (TaggedStats stats : this.stats.values()) {
         if (stats.merged) continue;
         out.println(stats.tag);
         out.println("\tIncoming to message processing times:");
         for (Map.Entry<String, Average> entry : stats.processingDelay.entrySet()) {
            out.printf("\t\t%s:\t%.2f us\n", entry.getKey(), entry.getValue().getFloat() / 1000);
         }
         out.println("\tTransport:");
         double averageLatency = 0;
         int unique = 0;
         int totalSent = 0;
         int totalReceived = 0;
         int latencyCount = 0;
         for (Map.Entry<String, RouteStats> entry : stats.routeStats.entrySet()) {
            String[] parts = entry.getKey().split("\\|");
            String routeBack = String.format("%s|%s", parts[1], parts[0]);
            RouteStats forward = entry.getValue();
            unique += forward.uniqueMessages;
            totalSent += forward.totalSent;
            totalReceived += forward.totalReceived;
            RouteStats back = stats.routeStats.get(routeBack);
            if (back == null) {
               out.printf("\t\t%s\t<-> %s:\tlatency unknown\n", parts[0], parts[1]);
               out.printf("\t\t%s\t -> %s:\t", parts[0], parts[1]);
               forward.printTo(out);
               out.println();
            } else if (parts[0].compareTo(parts[1]) < 0) {
               double latency = (forward.transportDiffs.getFloat() + back.transportDiffs.getFloat()) / 2000;
               averageLatency += latency;
               latencyCount++;
               out.printf("\t\t%s\t<-> %s:\tlatency %5.2f us\n", parts[0], parts[1], latency);
               out.printf("\t\t%s\t -> %s:\t", parts[0], parts[1]);
               forward.printTo(out);
               out.println();
               out.printf("\t\t%s\t<-  %s:\t", parts[0], parts[1]);
               back.printTo(out);
               out.println();
               unique += back.uniqueMessages;
               totalSent += back.totalSent;
               totalReceived += back.totalReceived;
            }
         }
         out.printf("\t\tAverage latency:\t%5.2f us\tUnique messages: %9d\tSent messages: %9d\tReceived messages: %9d\n\n",
                           averageLatency / latencyCount, unique, totalSent, totalReceived);
      }
      out.printf("Transmission of %d messages not detected.\n", notSent);
      out.printf("%d messages have not been received at all.\n", notReceived);
   }
}
