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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Merges multiple spans from several nodes into trace
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Composer {
   private static final long MAX_ADVANCE_MILIS = 10000;

   // this map is populated in first pass and should contain the number of references to each message
   // in second pass it is only read and when the counter reaches zero for all messages in the trace
   // the trace is ready to be written to file
   private ConcurrentMap<String, AtomicInteger> messageReferences = new ConcurrentHashMap<String, AtomicInteger>();

   private ConcurrentMap<String, Trace> traces = new ConcurrentHashMap<String, Trace>();

   private LinkedBlockingQueue<Trace> finishedTraces = new LinkedBlockingQueue<Trace>(1000);
   private AtomicLongArray highestUnixTimestamps;

   private List<String> inputFiles = new ArrayList<String>();
   private String traceLogFile;
   private Set<Class<? extends Processor>> processorClasses = new HashSet<Class<? extends Processor>>();
   private boolean reportMemoryUsage = false;
   private int totalMessages;


   public Composer() {
   }

   public static void main(String[] args) {
      Composer composer = new Composer();
      int i;
      for (i = 0; i < args.length; ++i) {
         if (args[i].equals("-o")) {
            if (i + 1 >= args.length) {
               printUsage();
               return;
            } else {
               composer.setTraceLogFile(args[i + 1]);
               ++i;
            }
         } else if (args[i].equals("-r")) {
            composer.setReportMemoryUsage(true);
         } else if (args[i].equals("-p")) {
            composer.processorClasses.add(PrintTrace.class);
         } else if (args[i].equals("-m")) {
            composer.processorClasses.add(AnalyseMessages.class);
         } else if (args[i].equals("-l")) {
            composer.processorClasses.add(AnalyseLocks.class);
         } else if (args[i].equals("-t")) {
            composer.processorClasses.add(AnalyseTraces.class);
         } else if (args[i].equals("-a")) {
            composer.processorClasses.add(PrintTrace.class);
            composer.processorClasses.add(AnalyseMessages.class);
            composer.processorClasses.add(AnalyseLocks.class);
            composer.processorClasses.add(AnalyseTraces.class);
         } else if (args[i].startsWith("-")) {
            System.err.println("Unknown option " + args[i]);
            printUsage();
            return;
         } else {
            break;
         }
      }
      if (composer.processorClasses.isEmpty()) {
         composer.processorClasses.add(PrintTrace.class);
      }
      for (; i < args.length; ++i) {
         composer.inputFiles.add(args[i]);
      }
      composer.run();
   }

   private static void printUsage() {
      System.err.println("Usage  [-r] [-p|-m|-l|-t|-a] [-o trace_log] span_logs...");
      System.err.println("\t-r\tReport memory usage");
      System.err.println("\t-p\tPrint log of traces");
      System.err.println("\t-m\tAnalyze messages");
      System.err.println("\t-l\tAnalyze locks");
      System.err.println("\t-t\tAnalyze traces");
      System.err.println("\t-a\tPrints log of traces and runs all available analyses");
      System.err.println("\t-o\tTrace log output file");
   }

   public void run() {
      System.err.println("Starting first pass");
      Thread[] threads = new Thread[inputFiles.size()];
      for (int i = 0; i < inputFiles.size(); ++i) {
         Thread t = new FirstPassThread(inputFiles.get(i));
         t.setName("First pass: " + inputFiles.get(i));
         threads[i] = t;
         t.start();
      }
      if (!joinAll(threads)) return;
      totalMessages = messageReferences.size();
      System.err.printf("Found %d messages\n", totalMessages);
      if (reportMemoryUsage) {
         reportMemoryUsage();
      }
      System.err.println("Starting second pass");
      highestUnixTimestamps = new AtomicLongArray(inputFiles.size());
      for (int i = 0; i < inputFiles.size(); ++i) {
         Thread t = new SecondPassThread(inputFiles.get(i), i);
         t.setName("Second pass: " + inputFiles.get(i));
         threads[i] = t;
         t.start();
      }
      List<Processor> processors = new ArrayList(processorClasses.size());
      for (Class<? extends Processor> clazz : processorClasses) {
         processors.add(getProcessor(clazz));
      }
      ProcessorThread processorThread = new ProcessorThread(processors);
      processorThread.start();
      joinAll(threads);
      processorThread.finish();
      while (!finishedTraces.isEmpty()) {
         Thread.yield();
      }
      System.err.printf("Memory:\n\tmessage references: %d\n\ttraces: %d\n",
                        messageReferences.size(), traces.size());
   }

   private Processor getProcessor(Class<? extends Processor> clazz) {
      try {
         Processor processor = clazz.newInstance();
         processor.init(this);
         return processor;
      } catch (Exception e) {
         System.err.println("Cannot instantiate processor: " + e);
         System.exit(1);
         return null;
      }
   }

   private boolean joinAll(Thread[] threads) {
      for (int i = 0; i < threads.length; ++i) {
         try {
            threads[i].join();
         } catch (InterruptedException e) {
            System.err.println("Interrupted!");
            return false;
         }
      }
      return true;
   }

   private static void reportMemoryUsage() {
      //System.gc();
      long used = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);
      long max = Runtime.getRuntime().maxMemory() / (1024 * 1024);
      System.err.printf("Using %d/%d MB of memory\n", used, max);
   }

   public void setTraceLogFile(String traceLogFile) {
      if (!traceLogFile.equals("-")) {
         this.traceLogFile = traceLogFile;
      }
   }

   public String getTraceLogFile() {
      return traceLogFile;
   }

   public void setReportMemoryUsage(boolean reportMemoryUsage) {
      this.reportMemoryUsage = reportMemoryUsage;
   }

   private class FirstPassThread extends Thread {
      private String file;

      private FirstPassThread(String file) {
         this.file = file;
      }

      @Override
      public void run() {
         try {
            read();
            System.err.println("Finished reading (first pass) " + file);
         } catch (IOException e) {
            System.err.println("Error reading " + file + " due to " + e);
            e.printStackTrace();
            System.exit(1);
         }
      }

      private void read() throws IOException {
         BufferedReader reader = new BufferedReader(new FileReader(file));

         String line = reader.readLine(); // timestamp info - ignored
         while ((line = reader.readLine()) != null) {
            if (line.startsWith(Trace.SPAN) || line.startsWith(Trace.NON_CAUSAL)) {
               String[] parts = line.split(";");
               for (int i = line.startsWith(Trace.NON_CAUSAL) ? 2 : 1; i < parts.length; ++i) {
                  if (parts[i].equals("null")) continue;
                  AtomicInteger prev = messageReferences.putIfAbsent(parts[i], new AtomicInteger(1));
                  if (prev != null) {
                     int refCount = prev.incrementAndGet();
                     //System.out.println(source + " inc " + parts[i] + " -> " + refCount);
                  } else {
                     //System.out.println(source + " add " + parts[i]);
                  }
               }
            }
         }
      }
   }

   private class SecondPassThread extends Thread {
      private String file;
      private int selfIndex;
      private long highestUnixTimestamp = 0;

      public SecondPassThread(String file, int selfIndex) {
         this.file = file;
         this.selfIndex = selfIndex;
      }

      @Override
      public void run() {
         try {
            read();
            System.err.println("Finished reading (second pass) " + file);
         } catch (IOException e) {
            System.err.println("Error reading " + file + " due to " + e);
            e.printStackTrace();
            System.exit(1);
         }
      }

      public void read() throws IOException {
         String source = new File(file).getName();
         source = source.substring(0, source.lastIndexOf('.') < 0 ? source.length() : source.lastIndexOf('.'));

         BufferedReader reader = new BufferedReader(new FileReader(file));

         String timeSync = reader.readLine();
         if (timeSync == null) {
            System.err.println("Empty file!");
            return;
         }
         String[] nanoAndUnix = timeSync.split("=");
         long nanoTime = Long.parseLong(nanoAndUnix[0]);
         long unixTime = Long.parseLong(nanoAndUnix[1]);

         Trace trace = null;
         long localHighestUnixTimestamp = 0;
         int lineNumber = 1;
         int spanCounter = 0;
         String line;
         while ((line = reader.readLine()) != null) {
            try {
               lineNumber++;
               String[] parts = line.split(";");
               if (line.startsWith(Trace.SPAN)) {
                  tryRetire(trace);
                  trace = null;
                  checkAdvance(localHighestUnixTimestamp);
                  spanCounter++;
                  for (int i = 1; i < parts.length; ++i) {
                     String message = parts[i];
                     if (message.equals("null")) continue;
                     if (trace == null) {
                        trace = retrieveTraceFor(message);
                     } else {
                        Trace traceForThisMessage = traces.get(message);
                        if (trace == traceForThisMessage) {
                           //System.err.println("Message should not be twice in one trace on one machine! (" + message + ", " + source + ":" + lineNumber + ")");
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
                     trace.lock.lock();
                  }
               } else if (line.startsWith(Trace.NON_CAUSAL)) {
                  tryRetire(trace);
                  trace = null;
                  checkAdvance(localHighestUnixTimestamp);
               } else if (line.startsWith(Trace.EVENT)) {
                  if (parts.length < 4 || parts.length > 5) {
                     System.err.println("Invalid line " + lineNumber + "\n" + line);
                     continue;
                  }

                  long eventTime = Long.valueOf(parts[1]);
                  String threadName = parts[2];
                  String type = parts[3];
                  String text = parts.length > 4 ? parts[4].trim() : null;

                  if (trace != null) {
                     Event e = new Event(nanoTime, unixTime, eventTime, source, spanCounter, threadName, type, text);
                     trace.addEvent(e);
                     localHighestUnixTimestamp = Math.max(localHighestUnixTimestamp, e.timestamp.getTime());
                  } else {
                     // NON-DATA
                     if (type.equals(Event.Type.OUTCOMING_DATA_STARTED.toString())) {
                        Trace traceForThisMessage = retrieveTraceFor(parts[4].trim());
                        Event e = new Event(nanoTime, unixTime, eventTime, source, spanCounter,
                                            threadName, Event.Type.RETRANSMISSION.toString(), text);
                        traceForThisMessage.addEvent(e);

                        decrementMessageRefCount(text);
                        tryRetire(traceForThisMessage);
                        checkAdvance(e.timestamp.getTime());
                     }
                  }
               } else {
                  System.err.println("Invalid line: " + line);
               }
            } catch (RuntimeException e) {
               System.err.println("Error reading line " + lineNumber + ":\n" + line + "\n");
               throw e;
            }
         }
         tryRetire(trace);
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
               if (highestUnixTimestamp > min + MAX_ADVANCE_MILIS) {
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

      private Trace mergeTraces(Trace trace, String message, Trace traceForThisMessage) {
         // hopefully even if we merge the traces twice the result is still correct
         for (;;) {
            if (traceForThisMessage.lock.tryLock()) {
               // check if we have really the right trace
               Trace possiblyOtherTrace = traces.get(message);
               if (possiblyOtherTrace != traceForThisMessage) {
                  traceForThisMessage.lock.unlock();
                  if (possiblyOtherTrace == trace || possiblyOtherTrace == null) {
                     // nobody can manipulate with current trace and the trace cannot be retired either
                     trace.lock.unlock();
                     throw new IllegalStateException();
                  }
                  traceForThisMessage = possiblyOtherTrace;
               } else {
                  for (String msg : traceForThisMessage.messages) {
                     traces.put(msg, trace);
                     trace.addMessage(msg);
                  }
                  for (Event e : traceForThisMessage.events) {
                     trace.addEvent(e);
                  }

                  if (traceForThisMessage.mergedInto != null) {
                     throw new IllegalStateException();
                  }
                  traceForThisMessage.mergedInto = trace;
                  trace.mergeCounter += traceForThisMessage.mergeCounter;
                  traceForThisMessage.lock.unlock();
                  break;
               }
            } else {
               trace.mergeCounter++;
               trace.lock.unlock();
               Thread.yield();
               trace.lock.lock();
               trace.mergeCounter--;
               while (trace.mergedInto != null) {
                  Trace old = trace;
                  trace = trace.mergedInto;
                  old.lock.unlock();
                  trace.lock.lock();
                  trace.mergeCounter--;
               }
            }
         }
         return trace;
      }

      private Trace retrieveTraceFor(String message) {
         Trace mf = traces.get(message);
         if (mf == null) {
            mf = new Trace();
            mf.addMessage(message);
            Trace prev = traces.putIfAbsent(message, mf);
            if (prev != null) {
               mf = prev;
            }
         }
         for (;;) {
            mf.lock.lock();
            Trace mfFromMap = traces.get(message);
            if (mfFromMap == null) {
               // removal from the map happens only when the trace is retired, therefore, no more
               // references for the message are alive
               mf.lock.unlock();
               throw new IllegalStateException();
            }
            if (mfFromMap != mf) {
               mf.lock.unlock();
               mf = mfFromMap;
            } else {
               break;
            }
         }
         return mf;
      }

      private void tryRetire(Trace mf) {
         if (mf == null) return;
         try {
            if (mf.mergeCounter > 0) return;
            for (String message : mf.messages) {
               if (messageReferences.get(message) != null) {
                  return;
               }
            }
            for (String message : mf.messages) {
               traces.remove(message);
            }
            mf.retired = true;
            finishedTraces.put(mf);
         } catch (InterruptedException e) {
            System.err.println("Interrupted when adding to queue!");
         } finally {
            mf.lock.unlock();
         }
      }
   }

   private void decrementMessageRefCount(String message) {
      int refCount = messageReferences.get(message).decrementAndGet();
      if (refCount == 0) {
         messageReferences.remove(message);
      }
      //System.out.println(source + " dec " + message + " -> " + refCount);
   }

   private class ProcessorThread extends Thread {
      private int tracesProcessed;
      private volatile boolean finished = false;
      private final List<Processor> processors;

      private ProcessorThread(List<Processor> processors) {
         setName("TraceProcessor");
         this.processors = processors;
      }

      @Override
      public void run() {
         while (!finished || !finishedTraces.isEmpty()) {
            Trace trace = null;
            try {
               while (!finished && finishedTraces.isEmpty()) {
                  Thread.yield();
               }
               if (finishedTraces.isEmpty()) break;
               trace = finishedTraces.take();
            } catch (InterruptedException e) {
               System.err.println("Printer interrupted!");
               break;
            }
            for (Processor processor : processors) {
               processor.process(trace);
            }
            if (++tracesProcessed % 10000 == 0) {
               System.err.printf("Processed %d traces, %d/%d messages\n",
                                 tracesProcessed, totalMessages - messageReferences.size(), totalMessages);
               if (reportMemoryUsage) {
                  Composer.reportMemoryUsage();
               }
            }
         }
         for (Processor processor : processors) {
            processor.finish();
         }
         System.err.println("Processing finished");
      }

      public void finish() {
         this.finished = true;
      }
   }
}
