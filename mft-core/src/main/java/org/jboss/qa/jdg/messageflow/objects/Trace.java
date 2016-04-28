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

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
* @author Radim Vansa &lt;rvansa@redhat.com&gt;
*/
public class Trace {

   public List<Event> events = new ArrayList<>();
   public Set<MessageId> messages = new HashSet<>();
   public int negativeCycles = 0;
   public volatile boolean retired = false;
   public int mergeCounter = 0;
   public Trace mergedInto = null;

   // this lock both against internal manipulation AND removing/replacing in messageFlows
   public Lock lock = new ReentrantLock();

   public void addMessage(MessageId msg) {
      messages.add(msg);
   }

   public void addEvent(Event e) {
      events.add(e);
   }

   public void sortCausally() {
      /* Identify handling events */
      Set<MessageId> availableTransmissions = new HashSet<>();
      Map<MessageId, Set<Event>> incomingEvents = new HashMap<>(messages.size());
      Map<String, List<Event>> eventsBySpan = getEventsBySpan();
      for (List<Event> spe : eventsBySpan.values()) {
         for (Event e : spe) {
            if (e.type != Event.Type.MSG_PROCESSING_START && e.type != Event.Type.CONTAINS) continue;
            Set<Event> incoming = incomingEvents.get(e.payload);
            if (incoming == null) {
               incoming = new HashSet<>();
               incomingEvents.put((MessageId) e.payload, incoming);
            }
            for (Event inc : spe) {
               if (inc.type == Event.Type.INCOMING_DATA) {
//                  inc.payload = inc.payload == null ? e.payload : inc.payload + ", " + e.payload;
                  inc.payload = e.payload;
                  incoming.add(e);
                  break;
               }
            }
         }
      }
      /* Find first moment when the message was sent - in fact retransmissions may hapen even BEFORE the message
         was sent the first time in a regular way */
      Map<MessageId, Event> outcomings = new HashMap<>();
      for (Event e : events) {
         if (e.type == Event.Type.OUTCOMING_DATA_STARTED || e.type == Event.Type.RETRANSMISSION) {
            Event out = outcomings.get(e.payload);
            if (out == null || out.nanoTime > e.nanoTime) {
               outcomings.put((MessageId) e.payload, e);
            }
         }
      }
      // TODO: maps with strings are not very effective
      /* Setup the graph */
      Map<String, Long> graph = new HashMap<String, Long>();
      Set<String> vertices = new HashSet<String>();
      for (Event e : outcomings.values()) {
         availableTransmissions.add((MessageId) e.payload);
         Set<Event> incoming = incomingEvents.get(e.payload);
         if (incoming != null) {
            for (Event inc : incoming) {
               if (inc.source == e.source && inc.nanoTime < e.nanoTime) {
                  System.err.printf("Message %s was received prior to first transmission, trace cannot be sorted causally.\n", e.payload);
                  sortByTimestamps();
                  return;
               }
               String edge = edge(e.source, inc.source);
               Long currDistance = graph.get(edge);//node.edges.get(e2.source);
               long distance = inc.timestamp.getTime() - e.timestamp.getTime();
               if (currDistance == null || distance < currDistance) {
                  graph.put(edge, distance);
                  vertices.add(e.source);
                  vertices.add(inc.source);
                  graph.put(edge(e.source, e.source), 0l);
                  graph.put(edge(inc.source, inc.source), 0l);
               }
            }
         }
      }
      boolean hasNegativeCycles;
      Map<String, Long> distances;
      do {
         /* Find negative cycles using Floyd-Warshall algorithm */
         distances = new HashMap<String, Long>(graph);
         for (String k : vertices) {
            for (String i : vertices) {
               for (String j : vertices) {
                  Long ik = distances.get(edge(i, k));
                  Long kj = distances.get(edge(k, j));
                  String ijEdge = edge(i, j);
                  Long ij = distances.get(ijEdge);
                  if (ik != null && kj != null) {
                     if (ij == null || ik + kj < ij) {
                        distances.put(ijEdge, ik + kj);
                     }
                  }
               }
            }
         }
         /* Increase weight on some of the vertices in order to remove the negative cycle */
         hasNegativeCycles = false;
         for (String v : vertices) {
            Long distanceToSelf = distances.get(edge(v, v));
            if (distanceToSelf < 0) {
               hasNegativeCycles = true;
               negativeCycles++;
               for (Map.Entry<String, Long> edge : graph.entrySet()) {
                  if (edge.getKey().startsWith(v)) {
                     graph.put(edge.getKey(), edge.getValue() - distanceToSelf);
                  }
               }
            }
         }
      } while (hasNegativeCycles);
      /* Distances are computed, no negative cycles present. Find max timestamp shifts (min distance) */
      Map<String, Long> shifts = new HashMap<String, Long>();
      for (Map.Entry<String, Long> path : distances.entrySet()) {
         String source = path.getKey().split("|")[0];
         Long currentShift = shifts.get(source);
         if (currentShift == null || currentShift > path.getValue()) {
            shifts.put(source, path.getValue());
         }
      }
      /* Correct timestamps */
      for (Event e : events) {
         Long shift = shifts.get(e.source);
         if (shift != null) {
            e.correctedTimestamp = e.timestamp.getTime() - shift;
         } else {
            e.correctedTimestamp = e.timestamp.getTime();
         }
      }
      /* Sort the events */
      Map<String, List<Event>> eventsBySource = getEventsBySource();
      ArrayList<Event> all = new ArrayList<Event>(events.size());
      for (List<Event> local : eventsBySource.values()) {
         Collections.sort(local, new Event.LocalTimestampComparator());
         all.addAll(local);
      }
      Collections.sort(all, new Event.CorrectedTimestampComparator());
      /* Shift improperly causally sorted messages */
      Map<MessageId, List<Event>> delayedEvents = new HashMap<>();
      Set<MessageId> transmissions = new HashSet<>();
      Map<String, Set<MessageId>> delayingSources = new HashMap<>();
      Map<Event, Set<MessageId>> delayCauses = new HashMap<>();
      List<Event> finalEvents = new ArrayList<Event>(all.size());
      for (Event e : all) {
         tryFlushEvent(e, finalEvents, delayedEvents, transmissions, availableTransmissions, delayingSources, delayCauses);
      }
      if (finalEvents.size() != this.events.size()) {
         throw new IllegalStateException(messages.toString());
      }
      /* Checks for causality */
//      transmissions.clear();
//      for (int i = 0; i < finalEvents.size(); ++i) {
//         Event started = finalEvents.get(i);
//         if (started.type != Event.Type.OUTCOMING_DATA_STARTED && started.type != Event.Type.RETRANSMISSION) continue;
//         // double transmissions? Possible for non-unique messages s.a. FD
//         if (transmissions.contains(started.text)) continue;
//         transmissions.add(started.text);
//         for (int j = i - 1; j >= 0; --j) {
//            Event handling = finalEvents.get(j);
//            if (handling.type == Event.Type.MSG_PROCESSING_START && handling.text.equals(started.text)) {
//               throw new IllegalStateException(started.text);
//            }
//         }
//      }
      this.events = finalEvents;
   }

   private void tryFlushEvent(Event e, List<Event> finalEvents, Map<MessageId, List<Event>> delayedEvents, Set<MessageId> transmissions, Set<MessageId> availableTransmissions, Map<String, Set<MessageId>> delayingSources, Map<Event, Set<MessageId>> delayCauses) {
      Set<MessageId> sourceDelay = delayingSources.get(e.source);
      if (sourceDelay != null) {
         for (MessageId message : sourceDelay) {
            delayedEvents.get(message).add(e);
         }
         delayCauses.put(e, sourceDelay);
      } else {
         if (e.type == Event.Type.OUTCOMING_DATA_STARTED || e.type == Event.Type.RETRANSMISSION) {
            transmissions.add((MessageId) e.payload);
            finalEvents.add(e);
            // remove source delay before flushing the delayed events
            for (Iterator<Set<MessageId>> iterator = delayingSources.values().iterator(); iterator.hasNext(); ) {
               Set<MessageId> pending = iterator.next();
               pending.remove(e.payload);
               if (pending.isEmpty()) {
                  iterator.remove();
               }
            }
            List<Event> list = delayedEvents.remove(e.payload);
            if (list != null) {
               for (Event delayed : list) {
                  Set<MessageId> cause = delayCauses.get(delayed);
                  if (cause == null) {
                     if (!finalEvents.contains(delayed)) throw new IllegalStateException();
                  } else {
                     cause.remove(e.payload);
                     if (cause.isEmpty()) {
                        delayCauses.remove(delayed);
                        tryFlushEvent(delayed, finalEvents, delayedEvents, transmissions, availableTransmissions, delayingSources, delayCauses);
                     }
                  }
               }
            }
         } else if (e.type == Event.Type.INCOMING_DATA) {
            if (transmissions.contains(e.payload) || !availableTransmissions.contains(e.payload)) {
               finalEvents.add(e);
            } else {
               List<Event> list = delayedEvents.get(e.payload);
               if (list == null) {
                  list = new ArrayList<>();
                  delayedEvents.put((MessageId) e.payload, list);
               }
               list.add(e);
               Set<MessageId> pending = delayingSources.get(e.source);
               if (pending == null) {
                  pending = new HashSet<>();
               } else {
                  pending = new HashSet<>(pending);
               }
               delayingSources.put(e.source, pending);
               pending.add((MessageId) e.payload);
               delayCauses.put(e, pending);
            }
         } else {
            finalEvents.add(e);
         }
      }
   }

   private final String edge(String from, String to) {
      return from + "|" + to;
   }

   public void sortByTimestamps() {
      Map<String, List<Event>> eventsBySource = getEventsBySource();
      ArrayList<Event> all = new ArrayList<Event>(events.size());
      for (List<Event> local : eventsBySource.values()) {
         Collections.sort(local, new Event.LocalTimestampComparator());
         all.addAll(local);
      }
      Collections.sort(all, new Event.GlobalTimestampComparator());
      this.events = all;
   }

   public Map<String, List<Event>> getEventsBySpan() {
      return getEventsBySelector(e -> e.source + "|" + e.span);
   }

   private Map<String, List<Event>> getEventsBySource() {
      return getEventsBySelector(e -> e.source);
   }

   private static <T> List<T> mergeLists(List<T> l1, List<T> l2) {
      // TODO: just use concat list
      ArrayList<T> list = new ArrayList<>(l1.size() + l2.size());
      list.addAll(l1);
      list.addAll(l2);
      return list;
   }

   public <T> Map<T, List<Event>> getEventsBySelector(Function<Event, T> selector) {
      return events.stream().collect(Collectors.toMap(selector, Collections::singletonList, Trace::mergeLists));
   }

   public static class SourcedThread {
      public final String source;
      public final String threadName;

      public SourcedThread(String source, String threadName) {
         this.source = source;
         this.threadName = threadName;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         SourcedThread that = (SourcedThread) o;

         if (!source.equals(that.source)) return false;
         return threadName.equals(that.threadName);

      }

      @Override
      public int hashCode() {
         int result = source.hashCode();
         result = 31 * result + threadName.hashCode();
         return result;
      }
   }
}
