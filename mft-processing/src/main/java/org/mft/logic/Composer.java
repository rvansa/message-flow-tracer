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

package org.mft.logic;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.mft.objects.Event;
import org.mft.objects.Header;
import org.mft.objects.Message;
import org.mft.objects.MessageId;
import org.mft.objects.Span;
import org.mft.objects.ThreadChange;
import org.mft.objects.Trace;
import org.mft.persistence.FlightRecording;
import org.mft.persistence.Persister;
import org.mft.processors.Processor;

/**
 * Merges multiple spans from several nodes into trace
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Composer extends Logic {

   // this map is populated in first pass and should contain the number of references to each message
   // in second pass it is only read and when the counter reaches zero for all messages in the trace
   // the trace is ready to be written to file
   private ConcurrentMap<MessageId, AtomicInteger> messageReferences = new ConcurrentHashMap<>();
   private AtomicInteger messagesRead = new AtomicInteger(0);
   private ConcurrentMap<MessageId, Trace> traces = new ConcurrentHashMap<>();

   private LinkedBlockingQueue<Trace> finishedTraces = new LinkedBlockingQueue<Trace>(1000);
   private AtomicLongArray highestUnixTimestamps;

   private List<Processor> processors = new ArrayList<Processor>();
   private boolean reportMemoryUsage = false;
   private int totalMessages;
   private boolean sortCausally = true;
   private long maxAdvanceMillis = 10000;
   private List<Predicate<Trace>> filters = new ArrayList<>();
   private long maxMessages = Long.MAX_VALUE;
   private long maxTraces = Long.MAX_VALUE;
   private Map<String, FlightRecording> flrBySource;
   private long injectedEvents = 0;

   @Override
   public void run() {
      System.err.println("Loading flight recordings");
      Thread[] flrThreads = new Thread[flightRecordings.size()];
      for (int i = 0; i < flrThreads.length; ++i) {
         FlightRecording flightRecording = flightRecordings.get(i);
         flrThreads[i] = new Thread(() -> flightRecording.load(getSource(flightRecording.getInput().name())), "FLR: " + flightRecording.getInput().name());
         flrThreads[i].start();
      }
      if (!joinAll(flrThreads)) return;
      flrBySource = flightRecordings.stream().collect(Collectors.toMap(flr -> getSource(flr.getInput().name()), Function.identity()));

      System.err.println("Starting first pass");
      FirstPassThread[] firstThreads = new FirstPassThread[logs.size()];
      for (int i = 0; i < logs.size(); ++i) {
         FirstPassThread t = new FirstPassThread(logs.get(i));
         firstThreads[i] = t;
         t.start();
      }
      if (!joinAll(firstThreads)) return;
      totalMessages = messageReferences.size();
      System.err.printf("Found %d messages\n", totalMessages);
      if (reportMemoryUsage) {
         reportMemoryUsage();
      }
      System.err.println("Starting second pass");
      highestUnixTimestamps = new AtomicLongArray(logs.size());
      SecondPassThread[] secondThreads = new SecondPassThread[logs.size()];
      for (int i = 0; i < logs.size(); ++i) {
         SecondPassThread t = new SecondPassThread(logs.get(i), i, firstThreads[i].getThreadNames());
         secondThreads[i] = t;
         t.start();
      }
      ProcessorThread processorThread = new ProcessorThread(processors);
      processorThread.start();
      joinAll(secondThreads);
      while (!finishedTraces.isEmpty()) {
         Thread.yield();
      }
      processorThread.finish();
      try {
         processorThread.join();
      } catch (InterruptedException e) {
      }
      System.err.printf("Memory:\n\tmessage references: %d\n\ttraces: %d\n",
                        messageReferences.size(), traces.size());
   }

   private static void reportMemoryUsage() {
      //System.gc();
      long used = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);
      long max = Runtime.getRuntime().maxMemory() / (1024 * 1024);
      System.err.printf("Using %d/%d MB of memory\n", used, max);
   }

   public void setReportMemoryUsage(boolean reportMemoryUsage) {
      this.reportMemoryUsage = reportMemoryUsage;
   }

   public void addProcessor(Processor processor) {
      processors.add(processor);
   }

   public List<Processor> getProcessors() {
      return processors;
   }

   public boolean isSortCausally() {
      return sortCausally;
   }

   public void setSortCausally(boolean sortCausally) {
      this.sortCausally = sortCausally;
   }

   public void setMaxAdvanceMillis(long maxAdvanceMillis) {
      this.maxAdvanceMillis = maxAdvanceMillis;
   }

   public void addFilter(Predicate<Trace> filter) {
      filters.add(filter);
   }

   public void setMaxMessages(long maxMessages) {
      this.maxMessages = maxMessages;
   }

   public void setMaxTraces(long maxTraces) {
      this.maxTraces = maxTraces;
   }

   private class FirstPassThread extends Thread {
      private Persister persister;
      private Map<Long, List<ThreadChange>> threadNames = new HashMap<>();

      private FirstPassThread(Persister persister) {
         super("First pass: " + persister.getInput().name());
         this.persister = persister;
      }

      @Override
      public void run() {
         try {
            persister.openForRead();
            try {
               persister.setSpanConsumer(span -> {
                  for (MessageId msg : span.getMessages()) {
                     AtomicInteger prev = messageReferences.putIfAbsent(msg, new AtomicInteger(1));
                     if (prev != null) {
                        prev.incrementAndGet();
                     }
                     int read = messagesRead.incrementAndGet();
                     if (read % 1000000 == 0) {
                        System.err.printf("%s Read %d message references (~%d messages)\n",
                           new SimpleDateFormat("HH:mm:ss").format(new Date()), read, messageReferences.size());
                     }
                  }
               }, false);
               persister.setThreadChangeConsumer(threadChange ->
                  threadNames.computeIfAbsent(threadChange.getId(), id -> new ArrayList<>()).add(threadChange)
               );
               persister.read();
               System.err.println("Finished reading (first pass) " + persister.getInput().name());
            } finally {
               persister.close();
            }
         } catch (IOException e) {
            System.err.println("Error reading " + persister + " due to " + e);
            e.printStackTrace();
            System.exit(1);
         }
      }

      public Map<Long, List<ThreadChange>> getThreadNames() {
         threadNames.forEach((id, list) -> Collections.sort(list, (tc1, tc2) -> Long.compare(tc1.getNanoTime(), tc2.getNanoTime())));
         return threadNames;
      }
   }

   public class SecondPassThread extends Thread {
      private final Map<Long, List<ThreadChange>> threadNames;
      private final Persister persister;
      private final int selfIndex;
      private long highestUnixTimestamp = 0;

      public SecondPassThread(Persister persister, int selfIndex, Map<Long, List<ThreadChange>> threadNames) {
         super("Second pass: " + persister.getInput().name());
         this.persister = persister;
         this.selfIndex = selfIndex;
         this.threadNames = threadNames;
      }

      @Override
      public void run() {
         try {
            String source = getSource(persister.getInput().name());

            AtomicInteger spanCounter = new AtomicInteger();

            Header header = persister.openForRead();
            for (Processor processor : processors) {
               processor.processHeader(header);
            }
            try {
               persister.setSpanConsumer(span -> {
                  int spanId = spanCounter.getAndIncrement();
                  if (!span.isNonCausal()) {
                     Trace trace = null;
                     for (MessageId message : span.getMessages()) {
                        if (trace == null) {
                           trace = retrieveTraceFor(message);
                        } else {
                           Trace traceForThisMessage = traces.get(message);
                           if (trace == traceForThisMessage) {
                              // We have sent message to ourselves
                           } else if (traceForThisMessage == null) {
                              trace.addMessage(message);
                              traceForThisMessage = traces.putIfAbsent(message, trace);
                              if (traceForThisMessage != null) {
                                 trace = mergeTraces(trace, message, traceForThisMessage);
                              }
                           } else {
                              trace = mergeTraces(trace, message, traceForThisMessage);
                           }
                        }
                        decrementMessageRefCount(message);
                     }
                     if (trace == null) {
                        // no message associated, but tracked?
                        trace = new Trace();
//                        System.err.printf("%d K Locking %08x%n", Thread.currentThread().getId(), System.identityHashCode(trace.lock));
                        trace.lock();
//                        System.err.printf("%d K Locked %08x%n", Thread.currentThread().getId(), System.identityHashCode(trace.lock));
                     }
                     for (Span.LocalEvent event : span.getEvents()) {
                        Event e = new Event(header.getNanoTime(), header.getUnixTime(), event.timestamp, source,
                           spanId, getThreadName(event.threadId, event.timestamp), event.type, event.payload);
                        trace.addEvent(e);
                        checkAdvance(e.timestamp.getTime());
                     }
                     tryRetire(trace);
                  } else {
                     for (Span.LocalEvent event : span.getEvents()) {
                        if (event.type == Event.Type.OUTCOMING_DATA_STARTED) {
                           Message message = (Message) event.payload;
                           Trace traceForThisMessage = retrieveTraceFor(message.id());
                           Event e = new Event(header.getNanoTime(), header.getUnixTime(), event.timestamp,
                              source, spanId, getThreadName(event.threadId, event.timestamp), Event.Type.RETRANSMISSION, event.payload);
                           traceForThisMessage.addEvent(e);

                           decrementMessageRefCount(message.id());
                           tryRetire(traceForThisMessage);
                           checkAdvance(e.timestamp.getTime());
                        } else if (event.type == Event.Type.TRACE_TAG) {
                           System.err.println(String.format("Warning: Span with trace tag (%s) marked as non-causal (%s line %d)", event.payload, source, persister.getPosition()));
                        }
                     }
                  }
               }, true);
               persister.setThreadChangeConsumer(tc -> {});
               persister.read();
            } finally {
               persister.close();
            }
            // as we have finished reading, nobody should be blocked by our old timestamp
            highestUnixTimestamps.set(selfIndex, Long.MAX_VALUE);
            System.err.println("Span counter second pass: " + spanCounter.get());
         } catch (IOException e) {
            System.err.println("Error reading " + persister + " due to " + e);
            e.printStackTrace();
            System.exit(1);
         }
      }

      private String getThreadName(long threadId, long timestamp) {
         List<ThreadChange> threadChanges = threadNames.get(threadId);
         if (threadChanges == null) {
            return "(unknown)-" + threadId;
         }
         ThreadChange last = null;
         for (ThreadChange tc : threadChanges) {
            if (timestamp >= tc.getNanoTime()) {
               return last != null ? last.getThreadName() : tc.getThreadName();
            }
         }
         return last != null ? last.getThreadName() : "(unknown)-" + threadId;
      }

      private void checkAdvance(long eventUnixTimestamp) {
         if (eventUnixTimestamp > highestUnixTimestamp) {
            highestUnixTimestamp = eventUnixTimestamp;
            highestUnixTimestamps.set(selfIndex, highestUnixTimestamp);
            for (;;) {
               long min = highestUnixTimestamp;
               for (int i = 0; i < highestUnixTimestamps.length(); ++i) {
                  min = Math.min(highestUnixTimestamps.get(i), min);
               }
               if (highestUnixTimestamp > min + maxAdvanceMillis) {
                  try {
                     Thread.sleep(1000);
                  } catch (InterruptedException e1) {
                  }
               } else {
                  break;
               }
            }
         }
      }

      private Trace mergeTraces(Trace trace, MessageId message, Trace traceForThisMessage) {
         // hopefully even if we merge the traces twice the result is still correct
         for (;;) {
//            System.err.printf("%d TryLocking %08x for %s%n", Thread.currentThread().getId(), System.identityHashCode(traceForThisMessage.lock), message);
            if (traceForThisMessage.tryLock()) {
               // check if we have really the right trace
               Trace possiblyOtherTrace = traces.get(message);
               if (possiblyOtherTrace != traceForThisMessage) {
                  traceForThisMessage.unlock();
                  if (possiblyOtherTrace == trace) {
                     // Somebody had done the job for us when we have released trace.lock for a moment
                     return trace;
                  } else if (possiblyOtherTrace == null) {
                     trace.unlock();
                     throw new IllegalStateException();
                  }
                  traceForThisMessage = possiblyOtherTrace;
               } else {
                  try {
                     if (traceForThisMessage.mergedInto != null) {
                        trace.unlock();
                        throw new IllegalStateException("in thread " + Thread.currentThread().getId());
                     }

                     for (MessageId msg : traceForThisMessage.messages) {
                        if (traces.put(msg, trace) != traceForThisMessage) {
                           trace.unlock();
                           throw new IllegalStateException();
                        }
                        trace.addMessage(msg);
                     }
                     for (Event e : traceForThisMessage.events) {
                        trace.addEvent(e);
                     }

                     traceForThisMessage.mergedInto = trace;
                     trace.mergeCounter += traceForThisMessage.mergeCounter;
                     break;
                  } finally {
                     traceForThisMessage.unlock();
                  }
               }
            } else {
               trace.mergeCounter++;
               trace.unlock();
//               Thread.yield();
               LockSupport.parkNanos(1);
               trace.lock();
               trace.mergeCounter--;
               while (trace.mergedInto != null) {
                  Trace old = trace;
                  trace = trace.mergedInto;
                  old.unlock();
                  trace.lock();
                  // as the merge counter has been added during merge, we have to decrement it on the merged trace as well
                  trace.mergeCounter--;
               }
               if (trace == traceForThisMessage) {
                  return trace;
               }
            }
         }
         return trace;
      }

      private Trace retrieveTraceFor(MessageId message) {
         Trace trace = traces.get(message);
         if (trace == null) {
            trace = new Trace();
            trace.addMessage(message);
            trace.lock();
            Trace prev = traces.putIfAbsent(message, trace);
            if (prev == null) {
               return trace;
            } else {
               trace.unlock();
               trace = prev;
            }
         }
         for (;;) {
//            System.err.printf("%d H Locking %08x for %s%n", Thread.currentThread().getId(), System.identityHashCode(trace.lock), message);
            trace.lock();
//            System.err.printf("%d H Locked %08x for %s%n", Thread.currentThread().getId(), System.identityHashCode(trace.lock), message);
            Trace traceFromMap = traces.get(message);
            if (traceFromMap == null) {
               // removal from the map happens only when the trace is retired, therefore, no more
               // references for the message are alive
               trace.unlock();
//               System.err.printf("%d I Unlocked %08x%n", Thread.currentThread().getId(), System.identityHashCode(trace.lock));
               throw new IllegalStateException("in thread " + Thread.currentThread().getId());
            }
            if (traceFromMap != trace) {
               trace.unlock();
//               System.err.printf("%d J Unlocked %08x%n", Thread.currentThread().getId(), System.identityHashCode(trace.lock));
               trace = traceFromMap;
            } else {
               break;
            }
         }
         if (trace.mergedInto != null) {
            throw new IllegalStateException();
         }
         return trace;
      }

       /**
        * retire trace and remove all the trace references from the traces map
        * if merge counter is not possitive and if messages from this trace are not in messageReferences
        * @param trace
        */
      private void tryRetire(Trace trace) {
         if (trace == null) return;
         try {
            if (trace.mergedInto != null) {
               throw new IllegalStateException();
            }
            if (trace.mergeCounter > 0) return;
            for (MessageId message : trace.messages) {
               if (messageReferences.get(message) != null) {
                  return;
               }
            }
            for (MessageId message : trace.messages) {
               traces.remove(message);
            }
            trace.retired = true;
            retireTrace(trace);
         } catch (InterruptedException e) {
            System.err.println("Interrupted when adding to queue!");
         } finally {
            trace.unlock();
//            System.err.printf("%d L Unlocked %08x%n", Thread.currentThread().getId(), System.identityHashCode(trace.lock));
         }
      }
   }

   private String getSource(String name) {
      int dotIndex = name.lastIndexOf('.');
      return dotIndex < 0 ? name : name.substring(0, dotIndex);
   }

   /**
     * Sort trace and put into finishedTraces
     * @param trace
     * @throws InterruptedException
     */
   private void retireTrace(Trace trace) throws InterruptedException {
      injectFlightRecording(trace);
      if (sortCausally) {
         try {
            trace.sortCausally();
         } catch (Exception e) {
            System.err.println("Failed to sort trace causally: " + e);
            trace.sortByTimestamps();
         }
      } else {
         trace.sortByTimestamps();
      }
      finishedTraces.put(trace);
   }

   private void injectFlightRecording(Trace trace) {
      if (flightRecordings.isEmpty()) {
         return;
      }
      Map<Trace.SourcedThread, MinMax> threadOccurences = new HashMap<>();
      Map<String, MinMax> sourceOccurences = new HashMap<>();
      for (Event event : trace.events) {
         Trace.SourcedThread sourcedThread = new Trace.SourcedThread(event.source, event.threadName);
         MinMax threadMinMax = threadOccurences.get(sourcedThread);
         if (threadMinMax == null) {
            threadOccurences.put(sourcedThread, new MinMax(event.timestamp.getTime()));
         } else {
            threadMinMax.accept(event.timestamp.getTime());
         }
         MinMax sourceMinMax = sourceOccurences.get(event.source);
         if (sourceMinMax == null) {
            sourceOccurences.put(event.source, new MinMax(event.timestamp.getTime()));
         } else {
            sourceMinMax.accept(event.timestamp.getTime());
         }
      }
      for (Map.Entry<Trace.SourcedThread, MinMax> entry :threadOccurences.entrySet()) {
         FlightRecording flr = flrBySource.get(entry.getKey().source);
         if (flr == null) continue;
         NavigableMap<Long, Event> events = flr.getEvents(entry.getKey().threadName);
         if (events == null) continue;
         for (Event event : events.subMap(entry.getValue().min, entry.getValue().max).values()) {
            trace.addEvent(event);
            injectedEvents++;
         }
      }
      for (Map.Entry<String, MinMax> entry: sourceOccurences.entrySet()) {
         FlightRecording flr = flrBySource.get(entry.getKey());
         if (flr == null) continue;
         for (Event event : flr.getGlobalEvents().subMap(entry.getValue().min, entry.getValue().max).values()) {
            trace.addEvent(event);
            injectedEvents++;
         }
      }
   }

   private static class MinMax implements LongConsumer {
      long min, max;

      public MinMax(long value) {
         this.min = value;
         this.max = value;
      }

      @Override
      public void accept(long value) {
         this.min = Math.min(min, value);
         this.max = Math.max(max, value);
      }
   }

   private void decrementMessageRefCount(MessageId message) {
      AtomicInteger counter = messageReferences.get(message);
      if (counter == null) {
         throw new IllegalStateException("No message counter for " + message);
      }
      int refCount = counter.decrementAndGet();
      if (refCount == 0) {
         messageReferences.remove(message);
      }
   }

   private class ProcessorThread extends Thread {
      private volatile boolean finished = false;
      private final List<Processor> processors;

      private ProcessorThread(List<Processor> processors) {
         setName("TraceProcessor");
         this.processors = processors;
      }

      @Override
      public void run() {
         long traceCounter = 0, filteredTraceCounter = 0;
         OUTER: while (!finished || !finishedTraces.isEmpty()) {
            Trace trace;
            try {
               for (;;) {
                  if (finished) break OUTER;
                  if (finishedTraces.isEmpty()) {
                     Thread.yield();
                  } else {
                     break;
                  }
               }
               trace = finishedTraces.take();
            } catch (InterruptedException e) {
               System.err.println("Printer interrupted!");
               break;
            }
            if (filters.stream().anyMatch(f -> !f.test(trace))) {
               ++filteredTraceCounter;
               continue;
            }
            for (Processor processor : processors) {
               try {
                  processor.process(trace, traceCounter);
               } catch (Throwable exception) {
                  System.err.printf("Failed to process trace %d in %s: %s\n", traceCounter, processor.getClass().getSimpleName(), exception);
                  exception.printStackTrace(System.err);
               }
            }
            if ((++traceCounter + filteredTraceCounter) % 10000 == 0) {
               System.err.printf("%s Processed %d traces (%d filtered out), %d/%d messages\n",
                  new SimpleDateFormat("HH:mm:ss").format(new Date()),
                  traceCounter, filteredTraceCounter, totalMessages - messageReferences.size(), totalMessages);
               if (reportMemoryUsage) {
                  Composer.reportMemoryUsage();
               }
            }
            if (totalMessages - messageReferences.size() > maxMessages) {
               System.err.println("Stopping because the limit of messages has been reached.");
               finish();
            } else if (traceCounter > maxTraces) {
               System.err.println("Stopping because the limit of traces has been reached.");
               finish();
            }
         }
         for (Processor processor : processors) {
            processor.finish();
         }
         System.err.printf("Processing finished, %d traces (%d filtered out), %d injected events\n", traceCounter, filteredTraceCounter, injectedEvents);
      }

      public void finish() {
         this.finished = true;
      }
   }
}
