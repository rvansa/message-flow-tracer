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

package org.mft.processors;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.SimpleFormatter;

import org.mft.objects.Event;
import org.mft.objects.Header;
import org.mft.objects.Message;
import org.mft.objects.MessageId;
import org.mft.objects.Trace;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class AnalyseMessages implements Processor {

   private static final String NO_TAG = "-no-tag-";
   private Map<String, TaggedStats> stats = new TreeMap<>();
   private int notSent;
   private int notReceived;
   private PrintStream out = System.out;
   private long startUnixTime = Long.MAX_VALUE;

   private static class TaggedStats {

      public String tag;
      public boolean merged;
      public Map<String, LongSummaryStatistics> processingDelay = new TreeMap<>();
      public Map<String, LongSummaryStatistics> bundlerDelay = new TreeMap<>();
      public Map<String, RouteStats> routeStats = new TreeMap<>();

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
      ArrayList<LongSummaryStatistics> transportDiffs = new ArrayList<>();
      ArrayList<LongSummaryStatistics> transportWallClock = new ArrayList<>();
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

   @Override
   public void process(Trace trace, long traceCounter) {
      for (MessageId msgId : trace.messages) {
         ArrayList<Event> sent = new ArrayList<Event>();
         Map<String, ArrayList<Event>> incoming = new HashMap<>();
         Map<String, TaggedStats> stats = new HashMap<>();
         // iterating in the time order
         for (Event event : trace.events) {
            if (event.type == Event.Type.OUTCOMING_DATA_STARTED || event.type == Event.Type.RETRANSMISSION) {
               if (!((Message) event.payload).id().equals(msgId)) {
                  continue;
               }
               sent.add(event);
            } else if (event.type == Event.Type.MSG_PROCESSING_START || event.type == Event.Type.DISCARD) {
               if (!event.payload.equals(msgId)) {
                  continue;
               }
               Event rx = null;
               //TODO: document why not for discard (currently I don't remember)
               if (event.type == Event.Type.MSG_PROCESSING_START) {
                  rx = findIncoming(trace.events, event.source, event.span);
               }
               if (rx != null) {
                  incoming.computeIfAbsent(event.source, a -> new ArrayList<>()).add(rx);
               }
               stats.putAll(findTaggedStats(trace.events, event.source, event.span));
            }
         }
         if (stats.isEmpty()) {
            stats.put(NO_TAG, this.stats.computeIfAbsent(NO_TAG, t -> new TaggedStats(t)));
         }
         for (Event event : trace.events) {
            if (event.type == Event.Type.OUTCOMING_DATA_STARTED) {
               Message msg = (Message) event.payload;
               if (!msg.id().equals(msgId) || msg.identityHashCode() == 0) {
                  continue;
               }
               Optional<Event> oth = trace.events.stream().filter(
                  e -> e.type == Event.Type.THREAD_HANDOVER_STARTED &&
                     e.span == event.span && getAnnotationHash(e) == msg.identityHashCode()).findFirst();
               oth.ifPresent(th -> {
                  for (TaggedStats ts : stats.values()) {
                     ts.bundlerDelay.computeIfAbsent(event.source, a -> new LongSummaryStatistics()).accept(event.nanoTime - th.nanoTime);
                  }
               });
            } else if (event.type == Event.Type.MSG_PROCESSING_START) {
               if (!msgId.equals(event.payload)) {
                  continue;
               }
               Event rx = findIncoming(trace.events, event.source, event.span);
               if (rx == null) {
                  // message was not sent over wire, it's a broadcast
               } else {
                  for (TaggedStats ts : stats.values()) {
                     ts.processingDelay.computeIfAbsent(rx.source, a -> new LongSummaryStatistics()).accept(event.nanoTime - rx.nanoTime);
                  }
               }
            }
         }
         // let's assume that if the message was really lost, it was the first message, if there are
         // more receptions, these are in the end and combination of these two is rare
         for (ArrayList<Event> incomingForSource : incoming.values()) {
            if (sent.isEmpty()) {
               // we don't know who sent that, cannot produce any stats
               continue;
            }
            int shiftIndex = Math.max(sent.size() - incomingForSource.size(), 0);

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
                  int index = (int) TimeUnit.MILLISECONDS.toSeconds(tx.timestamp.getTime() - startUnixTime);
                  RouteStats routeStats = ts.routeStats.computeIfAbsent(route, r -> new RouteStats());
                  getOrAdd(routeStats.transportDiffs, index, LongSummaryStatistics::new).accept(diff);
                  getOrAdd(routeStats.transportWallClock, index, LongSummaryStatistics::new).accept(rx.timestamp.getTime() - tx.timestamp.getTime());
               }
            }
            int discarded = 0;
            for (Event event : incomingForSource) {
               if (event.type == Event.Type.DISCARD) {
                  discarded++;
               }
            }
            for (TaggedStats ts : stats.values()) {
               RouteStats routeStats = ts.routeStats.get(route);
               routeStats.duplicates += Math.max(incomingForSource.size() - sent.size(), 0);
               routeStats.lost += shiftIndex;
               routeStats.ignored += incomingForSource.size() - discarded - 1;
               routeStats.discarded += discarded;
               routeStats.uniqueMessages++;
               routeStats.totalSent += sent.size();
               routeStats.totalReceived += incomingForSource.size();
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

   public int getAnnotationHash(Event e) {
      if (e.payload instanceof Integer) return (Integer) e.payload;
      if (e.payload instanceof String) {
         String str = (String) e.payload;
         int index = str.lastIndexOf(':');
         return (int) Long.parseLong(index < 0 ? str: str.substring(index + 1), 16);
      }
      throw new IllegalArgumentException(String.valueOf(e.payload));
   }

   private <T> T getOrAdd(List<T> list, int index, Supplier<T> supplier) {
      int size = list.size();
      if (index < size) {
         return list.get(index);
      } else {
         for (int i = size; i < index; ++i) {
            list.add(supplier.get());;
         }
         T last = supplier.get();
         list.add(last);
         return last;
      }
   }

   @Override
   public synchronized void processHeader(Header header) {
      startUnixTime = Math.min(header.getUnixTime(), startUnixTime);
   }

   private Map<String, TaggedStats> findTaggedStats(Collection<Event> events, String source, int span) {
      Map<String, TaggedStats> tags = new HashMap<String, TaggedStats>();
      for (Event event : events) {
         if (event.source == source && event.span == span && event.type == Event.Type.MESSAGE_TAG) {
            String tag = (String) event.payload;
            TaggedStats stats = this.stats.computeIfAbsent(tag, t -> new TaggedStats(t));
            tags.put(tag, stats);
         }
      }
      return tags;
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
         long totalSentOnAllNodes = stats.routeStats.values().stream().mapToLong(rs -> rs.totalSent).sum();
         if (totalSentOnAllNodes < 30) {
            out.println(stats.tag + ": " + totalSentOnAllNodes + " messages");
            continue;
         }
         out.println(stats.tag);
         out.println("\tMessage processing times:");
         for (Map.Entry<String, LongSummaryStatistics> entry : stats.processingDelay.entrySet()) {
            LongSummaryStatistics bundlerDelay = stats.bundlerDelay.get(entry.getKey());
            out.printf("\t\t%s:\tIncoming: %.2f us\tBundler: %.2f us\n", entry.getKey(), entry.getValue().getAverage() / 1000, bundlerDelay == null ? 0 : bundlerDelay.getAverage() / 1000);
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
               double latency = (forward.transportDiffs.stream().reduce(new LongSummaryStatistics(), this::merge).getAverage()
                  + back.transportDiffs.stream().reduce(new LongSummaryStatistics(), this::merge).getAverage()) / 2000;
               double wcLatency = (forward.transportWallClock.stream().reduce(new LongSummaryStatistics(), this::merge).getAverage()
                  + back.transportWallClock.stream().reduce(new LongSummaryStatistics(), this::merge).getAverage()) / 2;
               averageLatency += latency;
               latencyCount++;
               out.printf("\t\t%s\t<-> %s:\tlatency %.2f us\twall-clock latency: %.2f ms", parts[0], parts[1], latency, wcLatency);
               int minDiffs = Math.min(forward.transportDiffs.size(), back.transportDiffs.size());
               for (int i = 0; i < minDiffs; i += 4) {
                  out.println();
                  for (int j = 0; j < 4 && i + j < minDiffs; ++j) {
                     double nanoTimeLatency = (forward.transportDiffs.get(i + j).getAverage() + back.transportDiffs.get(i + j).getAverage()) / 2000;
                     long forwardCount = forward.transportDiffs.get(i + j).getCount();
                     long backCount = back.transportDiffs.get(i + j).getCount();
                     double wallTimeLatency = forward.transportWallClock.get(i + j).getAverage() + back.transportWallClock.get(i + j).getAverage();
                     if (forwardCount > 0 && backCount > 0) wallTimeLatency /= 2;
                     out.printf("%s %8.2f us|%8.2f ms(%5d,%5d)\t", new SimpleDateFormat("HH:mm:ss").format(new Date(startUnixTime + (i + j) * 1000)),
                        forwardCount > 0 && backCount > 0 ? nanoTimeLatency : 0,
                         wallTimeLatency, forwardCount, backCount);
                  }
               }
               out.printf("\n\t\t%s\t -> %s:\t", parts[0], parts[1]);
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

   private LongSummaryStatistics merge(LongSummaryStatistics s1, LongSummaryStatistics s2) {
      LongSummaryStatistics sum = new LongSummaryStatistics();
      sum.combine(s1);
      sum.combine(s2);
      return sum;
   }
}
