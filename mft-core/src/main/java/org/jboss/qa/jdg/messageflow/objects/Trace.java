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

/**
* @author Radim Vansa &lt;rvansa@redhat.com&gt;
*/
public class Trace {
   public static final String NON_CAUSAL = "NC";
   public static final String SPAN = "SPAN";
   public static final String EVENT = "E";

   public List<Event> events = new ArrayList<Event>();
   public Set<String> messages = new HashSet<String>();
   public int negativeCycles = 0;
   public volatile boolean retired = false;
   public int mergeCounter = 0;
   public Trace mergedInto = null;

   // this lock both against internal manipulation AND removing/replacing in messageFlows
   public Lock lock = new ReentrantLock();

   public void addMessage(String msg) {
      if (retired) throw new IllegalStateException();
      messages.add(msg);
   }

   public void addEvent(Event e) {
      if (retired) throw new IllegalStateException();
      events.add(e);
   }

   public void sortCausally() {
      /* Identify handling events */
      Set<String> availableTransmissions = new HashSet<String>();
      Map<String, Set<Event>> incomingEvents = new HashMap<String, Set<Event>>(messages.size());
      Map<String, List<Event>> eventsBySpan = getEventsBySpan();
      for (List<Event> spe : eventsBySpan.values()) {
         for (Event e : spe) {
            if (e.type != Event.Type.MSG_PROCESSING_START && e.type != Event.Type.CONTAINS) continue;
            Set<Event> incoming = incomingEvents.get(e.text);
            if (incoming == null) {
               incoming = new HashSet<Event>();
               incomingEvents.put(e.text, incoming);
            }
            for (Event inc : spe) {
               if (inc.type == Event.Type.INCOMING_DATA) {
                  inc.text = e.text;
                  incoming.add(e);
                  break;
               }
            }
         }
      }
      /* Find first moment when the message was sent - in fact retransmissions may hapen even BEFORE the message
         was sent the first time in a regular way */
      Map<String, Event> outcomings = new HashMap<String, Event>();
      for (Event e : events) {
         if (e.type == Event.Type.OUTCOMING_DATA_STARTED || e.type == Event.Type.RETRANSMISSION) {
            Event out = outcomings.get(e.text);
            if (out == null || out.nanoTime > e.nanoTime) {
               outcomings.put(e.text, e);
            }
         }
      }
      // TODO: maps with strings are not very effective
      /* Setup the graph */
      Map<String, Long> graph = new HashMap<String, Long>();
      Set<String> vertices = new HashSet<String>();
      for (Event e : outcomings.values()) {
         availableTransmissions.add(e.text);
         Set<Event> incoming = incomingEvents.get(e.text);
         if (incoming != null) {
            for (Event inc : incoming) {
               if (inc.source == e.source && inc.nanoTime < e.nanoTime) {
                  System.err.printf("Message %s was received prior to first transmission, trace cannot be sorted causally.\n", e.text);
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
      Map<String, List<Event>> delayedEvents = new HashMap<String, List<Event>>();
      Set<String> transmissions = new HashSet<String>();
      Map<String, Set<String>> delayingSources = new HashMap<String, Set<String>>();
      Map<Event, Set<String>> delayCauses = new HashMap<Event, Set<String>>();
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

   private void tryFlushEvent(Event e, List<Event> finalEvents, Map<String, List<Event>> delayedEvents, Set<String> transmissions, Set<String> availableTransmissions, Map<String, Set<String>> delayingSources, Map<Event, Set<String>> delayCauses) {
      Set<String> sourceDelay = delayingSources.get(e.source);
      if (sourceDelay != null) {
         for (String message : sourceDelay) {
            delayedEvents.get(message).add(e);
         }
         delayCauses.put(e, sourceDelay);
      } else {
         if (e.type == Event.Type.OUTCOMING_DATA_STARTED || e.type == Event.Type.RETRANSMISSION) {
            transmissions.add(e.text);
            finalEvents.add(e);
            // remove source delay before flushing the delayed events
            for (Iterator<Set<String>> iterator = delayingSources.values().iterator(); iterator.hasNext(); ) {
               Set<String> pending = iterator.next();
               pending.remove(e.text);
               if (pending.isEmpty()) {
                  iterator.remove();
               }
            }
            List<Event> list = delayedEvents.remove(e.text);
            if (list != null) {
               for (Event delayed : list) {
                  Set<String> cause = delayCauses.get(delayed);
                  if (cause == null) {
                     if (!finalEvents.contains(delayed)) throw new IllegalStateException();
                  } else {
                     cause.remove(e.text);
                     if (cause.isEmpty()) {
                        delayCauses.remove(delayed);
                        tryFlushEvent(delayed, finalEvents, delayedEvents, transmissions, availableTransmissions, delayingSources, delayCauses);
                     }
                  }
               }
            }
         } else if (e.type == Event.Type.INCOMING_DATA) {
            if (transmissions.contains(e.text) || !availableTransmissions.contains(e.text)) {
               finalEvents.add(e);
            } else {
               List<Event> list = delayedEvents.get(e.text);
               if (list == null) {
                  list = new ArrayList<Event>();
                  delayedEvents.put(e.text, list);
               }
               list.add(e);
               Set<String> pending = delayingSources.get(e.source);
               if (pending == null) {
                  pending = new HashSet<String>();
               } else {
                  pending = new HashSet<String>(pending);
               }
               delayingSources.put(e.source, pending);
               pending.add(e.text);
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
      return getEventsBySelector(new Selector<String>() {
         @Override
         public String select(Event e) {
            return e.source + "|" + e.span;
         }
      });
   }

   private Map<String, List<Event>> getEventsBySource() {
      return getEventsBySelector(new Selector<String>() {
         @Override
         public String select(Event e) {
            return e.source;
         }
      });
   }

   public <T> Map<T, List<Event>> getEventsBySelector(Selector<T> selector) {
      Map<T, List<Event>> eventsBySelector = new HashMap<T, List<Event>>();
      for (Event e : events) {
         T selection = selector.select(e);
         List<Event> list = eventsBySelector.get(selection);
         if (list == null) {
            list = new ArrayList<Event>();
            eventsBySelector.put(selection, list);
         }
         list.add(e);
      }
      return eventsBySelector;
   }

   private interface Selector<T> {
      T select(Event e);
   }
}
