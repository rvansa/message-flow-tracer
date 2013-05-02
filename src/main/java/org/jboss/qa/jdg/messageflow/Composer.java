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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Merges multiple ControlFlows from several nodes into MessageFlows.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Composer {
   private static final long MAX_ADVANCE_MILIS = 10000;

   // this map is populated in first pass and should contain the number of references to each message
   // in second pass it is only read and when the counter reaches zero for all messages in the message flow
   // the message flow is ready to be written to file
   private ConcurrentMap<String, AtomicInteger> messageReferences = new ConcurrentHashMap<String, AtomicInteger>();

   private ConcurrentMap<String, MessageFlow> messageFlows = new ConcurrentHashMap<String, MessageFlow>();

   private LinkedBlockingQueue<MessageFlow> finishedFlows = new LinkedBlockingQueue<MessageFlow>(1000);
   private AtomicLongArray highestUnixTimestamps;

   private List<String> inputFiles = new ArrayList<String>();
   private String outputFile;
   private Class<? extends Processor> processorClass = PrintMessageFlow.class;
   private boolean reportMemoryUsage = false;
   private int totalMessages;


   public Composer() {
   }

   public static void main(String[] args) {
      Composer composer = new Composer();
      int i;
      for (i = 0; i < args.length; ++i) {
         if (args[i].equals("-p")) {
            if (i + 1 >= args.length) {
               printUsage();
               return;
            } else {
               composer.setOutputFile(args[i + 1]);
               composer.setProcessorClass(PrintMessageFlow.class);
               ++i;
            }
         } else if (args[i].equals("-r")) {
            composer.setReportMemoryUsage(true);
         } else if (args[i].equals("-m")) {
            composer.setProcessorClass(AnalyzeMessages.class);
         } else if (args[i].equals("-l")) {
            composer.setProcessorClass(AnalyzeLocks.class);
         } else if (args[i].startsWith("-")) {
            System.err.println("Unknown option " + args[i]);
            printUsage();
            return;
         } else {
            break;
         }
      }
      for (; i < args.length; ++i) {
         composer.addInput(args[i]);
      }
      composer.run();
   }

   private void addInput(String file) {
      inputFiles.add(file);
   }

   private static void printUsage() {
      System.err.println("Usage [-o outputfile] inputfiles...");
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
      ProcessorThread processorThread = new ProcessorThread(getProcessor());
      processorThread.start();
      joinAll(threads);
      processorThread.finish();
      while (!finishedFlows.isEmpty()) {
         Thread.yield();
      }
      System.err.printf("Memory:\n\tmessage references: %d\n\tmessage flows: %d\n",
                        messageReferences.size(), messageFlows.size());
   }

   private Processor getProcessor() {
      try {
         Processor processor = processorClass.newInstance();
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

   public void setOutputFile(String outputFile) {
      if (!outputFile.equals("-")) {
         this.outputFile = outputFile;
      }
   }

   public void setProcessorClass(Class<? extends Processor> processorClass) {
      this.processorClass = processorClass;
   }

   public String getOutputFile() {
      return outputFile;
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
         String source = new File(file).getName();
         source = source.substring(0, source.lastIndexOf('.') < 0 ? source.length() : source.lastIndexOf('.'));
         BufferedReader reader = new BufferedReader(new FileReader(file));

         String line = reader.readLine(); // timestamp info
         while ((line = reader.readLine()) != null) {
            if (line.startsWith(MessageFlow.CONTROL_FLOW) || line.startsWith(MessageFlow.NON_CAUSAL)) {
               String[] parts = line.split(";");
               for (int i = line.startsWith(MessageFlow.NON_CAUSAL) ? 2 : 1; i < parts.length; ++i) {
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

         MessageFlow mf = null;
         long localHighestUnixTimestamp = 0;
         int lineNumber = 1;
         int controlFlowCounter = 0;
         String line;
         while ((line = reader.readLine()) != null) {
            try {
               lineNumber++;
               String[] parts = line.split(";");
               if (line.startsWith(MessageFlow.CONTROL_FLOW)) {
                  tryRetire(mf);
                  mf = null;
                  checkAdvance(localHighestUnixTimestamp);
                  controlFlowCounter++;
                  for (int i = 1; i < parts.length; ++i) {
                     String message = parts[i];
                     if (message.equals("null")) continue;
                     if (mf == null) {
                        mf = retrieveMessageFlowFor(message);
                     } else {
                        MessageFlow mfForThisMessage = messageFlows.get(message);
                        if (mf == mfForThisMessage) {
                           //System.err.println("Message should not be twice in one flow on one machine! (" + message + ", " + source + ":" + lineNumber + ")");
                        } else if (mfForThisMessage == null) {
                           mf.addMessage(message);
                           mfForThisMessage = messageFlows.putIfAbsent(message, mf);
                           if (mfForThisMessage != null) {
                              mf = mergeFlows(mf, message, mfForThisMessage);
                           }
                        } else {
                           mf = mergeFlows(mf, message, mfForThisMessage);
                        }
                     }
                     decrementMessageRefCount(message);
                  }
                  if (mf == null) {
                     // no message associated, but tracked?
                     mf = new MessageFlow();
                     mf.lock.lock();
                  }
               } else if (line.startsWith(MessageFlow.NON_CAUSAL)) {
                  tryRetire(mf);
                  mf = null;
                  checkAdvance(localHighestUnixTimestamp);
               } else if (line.startsWith(MessageFlow.EVENT)) {
                  if (parts.length < 4 || parts.length > 5) {
                     System.err.println("Invalid line " + lineNumber + "\n" + line);
                     continue;
                  }

                  long eventTime = Long.valueOf(parts[1]);
                  String threadName = parts[2];
                  String type = parts[3];
                  String text = parts.length > 4 ? parts[4].trim() : null;

                  if (mf != null) {
                     Event e = new Event(nanoTime, unixTime, eventTime, source, controlFlowCounter, threadName, type, text);
                     mf.addEvent(e);
                     localHighestUnixTimestamp = Math.max(localHighestUnixTimestamp, e.timestamp.getTime());
                  } else {
                     // NON-DATA
                     if (type.equals(Event.Type.OUTCOMING_DATA_STARTED.toString())) {
                        MessageFlow mfForThisMessage = retrieveMessageFlowFor(parts[4].trim());
                        Event e = new Event(nanoTime, unixTime, eventTime, source, controlFlowCounter,
                                            threadName, Event.Type.RETRANSMISSION.toString(), text);
                        mfForThisMessage.addEvent(e);

                        decrementMessageRefCount(text);
                        tryRetire(mfForThisMessage);
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
         tryRetire(mf);
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

      private MessageFlow mergeFlows(MessageFlow mf, String message, MessageFlow mfForThisMessage) {
         // hopefully even if we merge the flows twice the result is still correct
         for (;;) {
            if (mfForThisMessage.lock.tryLock()) {
               // check if we have really the right messageFlow
               MessageFlow possiblyOtherMF = messageFlows.get(message);
               if (possiblyOtherMF != mfForThisMessage) {
                  mfForThisMessage.lock.unlock();
                  if (possiblyOtherMF == mf || possiblyOtherMF == null) {
                     // nobody can manipulate with current mf and the mf cannot be retired either
                     mf.lock.unlock();
                     throw new IllegalStateException();
                  }
                  mfForThisMessage = possiblyOtherMF;
               } else {
                  for (String msg : mfForThisMessage.messages) {
                     messageFlows.put(msg, mf);
                     mf.addMessage(msg);
                  }
                  for (Event e : mfForThisMessage.events) {
                     mf.addEvent(e);
                  }

                  if (mfForThisMessage.mergedInto != null) {
                     throw new IllegalStateException();
                  }
                  mfForThisMessage.mergedInto = mf;
                  mf.mergeCounter += mfForThisMessage.mergeCounter;
                  mfForThisMessage.lock.unlock();
                  break;
               }
            } else {
               mf.mergeCounter++;
               mf.lock.unlock();
               Thread.yield();
               mf.lock.lock();
               mf.mergeCounter--;
               while (mf.mergedInto != null) {
                  MessageFlow old = mf;
                  mf = mf.mergedInto;
                  old.lock.unlock();
                  mf.lock.lock();
                  mf.mergeCounter--;
               }
            }
         }
         return mf;
      }

      private MessageFlow retrieveMessageFlowFor(String message) {
         MessageFlow mf = messageFlows.get(message);
         if (mf == null) {
            mf = new MessageFlow();
            mf.addMessage(message);
            MessageFlow prev = messageFlows.putIfAbsent(message, mf);
            if (prev != null) {
               mf = prev;
            }
         }
         for (;;) {
            mf.lock.lock();
            MessageFlow mfFromMap = messageFlows.get(message);
            if (mfFromMap == null) {
               // removal from the map happens only when the flow is retired, therefore, no more
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

      private void tryRetire(MessageFlow mf) {
         if (mf == null) return;
         try {
            if (mf.mergeCounter > 0) return;
            for (String message : mf.messages) {
               if (messageReferences.get(message) != null) {
                  return;
               }
            }
            for (String message : mf.messages) {
               messageFlows.remove(message);
            }
            mf.retired = true;
            finishedFlows.put(mf);
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
      private int flowsProcessed;
      private volatile boolean finished = false;
      private final Processor interpret;

      private ProcessorThread(Processor interpret) {
         setName("MessageFlowProcessor");
         this.interpret = interpret;
      }

      @Override
      public void run() {
         while (!finished || !finishedFlows.isEmpty()) {
            MessageFlow mf = null;
            try {
               while (!finished && finishedFlows.isEmpty()) {
                  Thread.yield();
               }
               if (finishedFlows.isEmpty()) break;
               mf = finishedFlows.take();
            } catch (InterruptedException e) {
               System.err.println("Printer interrupted!");
               break;
            }
            interpret.process(mf);
            if (++flowsProcessed % 10000 == 0) {
               System.err.printf("Processed %d message flows, %d/%d messages\n",
                                 flowsProcessed, totalMessages - messageReferences.size(), totalMessages);
               if (reportMemoryUsage) {
                  Composer.reportMemoryUsage();
               }
            }
         }
         interpret.finish();
         System.err.println("Processing finished");
      }

      public void finish() {
         this.finished = true;
      }
   }
}
