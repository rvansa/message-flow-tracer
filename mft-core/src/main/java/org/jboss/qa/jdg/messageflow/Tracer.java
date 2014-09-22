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

import org.jboss.qa.jdg.messageflow.objects.BatchSpan;
import org.jboss.qa.jdg.messageflow.objects.Event;
import org.jboss.qa.jdg.messageflow.objects.Span;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class should track the execution path from the external request
 * (such as Cache.put() call) on one node, tracking the requests into Spans.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Tracer {
   private static ConcurrentHashMap<Object, Span> spans = new ConcurrentHashMap<Object, Span>();
   private static ConcurrentHashMap<Object, AtomicInteger> referenceCounters = new ConcurrentHashMap<Object, AtomicInteger>();
   private static ConcurrentLinkedQueue<Span> finishedSpans = new ConcurrentLinkedQueue<Span>();

   private static ThreadLocal<Span> contextSpan = new ThreadLocal<Span>();
   private static ThreadLocal<Integer> contextCounter = new ThreadLocal<Integer>();
   private static ThreadLocal<Boolean> contextManaged = new ThreadLocal<Boolean>();
   private static ThreadLocal<String> contextMark = new ThreadLocal<String>();

   private static ConcurrentHashMap<Object, String> markedObjects = new ConcurrentHashMap<Object, String>();

   private static volatile boolean running = true;

   static {
      final Thread writer = new Thread() {
         @Override
         public void run() {
            String path = System.getProperty("org.jboss.qa.messageflowtracer.output");
            if (path == null) try {
               path = File.createTempFile("span.", ".txt", new File(System.getProperty("org.jboss.qa.messageflowtracer.outputdir", "/tmp"))).getAbsolutePath();
            } catch (IOException e) {
               path = "/tmp/span.txt";
            }
            boolean binarySpans = System.getProperty("org.jboss.qa.messageflowtracer.binarySpans") != null ? true : false;

            if (binarySpans){
                  //without java serialization
                  DataOutputStream dOStream = null;

                  try {
                     dOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));

                     dOStream.writeLong(System.nanoTime());
                     dOStream.writeLong(System.currentTimeMillis());

                     while (running || !finishedSpans.isEmpty()) {
                        Span span;
                        while ((span = finishedSpans.poll()) != null) {
                           span.binaryWriteTo(dOStream, false);
                        }
                        try {
                           Thread.sleep(10);
                        } catch (InterruptedException e) {
                           break;
                        }
                     }
                  } catch (IOException e) {
                     e.printStackTrace();
                  } finally {
                     if (dOStream != null) {
                        try {
                           dOStream.close();
                        } catch (IOException e) {
                           e.printStackTrace();
                        }
                     }
                     synchronized (Tracer.class) {
                        Tracer.class.notifyAll();
                     }
                  }
            } else {
               PrintStream writer = null;
               try {
                  writer = new PrintStream(new BufferedOutputStream(new FileOutputStream(path)));
                  writer.printf("%d=%d\n", System.nanoTime(), System.currentTimeMillis());
                  while (running || !finishedSpans.isEmpty()) {
                     Span span;
                     while ((span = finishedSpans.poll()) != null) {
                        span.writeTo(writer, false);
                     }
                     try {
                        Thread.sleep(10);
                     } catch (InterruptedException e) {
                        break;
                     }
                  }
               } catch (FileNotFoundException e) {
               } finally {
                  if (writer != null) writer.close();
                  synchronized (Tracer.class) {
                     Tracer.class.notifyAll();
                  }
               }
            }
         }
      };
      writer.setDaemon(true);
      writer.setName("SpanWriter");
      writer.start();
      Runtime.getRuntime().addShutdownHook(new Thread() {
         @Override
         public void run() {
            running = false;
            synchronized (Tracer.class) {
               try {
                  Tracer.class.wait();
               } catch (InterruptedException e) {
                  System.err.println("Waiting for writer interrupted.");
               }
            }
            reportReferenceCounters();
            reportSpans();
            System.err.println(finishedSpans.size() + " not written finished spans.");
            Span.debugPrintUnfinished();
         }

         private void reportSpans() {
            System.err.println(spans.size() + " unfinished spans");
            int counter = 0;
            for (Map.Entry<Object, Span> entry : spans.entrySet()) {
               System.out.printf("%s:%08x (refcount=%s) -> ", entry.getKey().getClass().getName(),
                                 entry.getKey().hashCode(), referenceCounters.get(entry.getKey()));
               entry.getValue().writeWithChildren(System.out);
               if (++counter > 500) break; // shutdown hook must execute quickly
            }
            if (counter > 500) {
               System.out.println("Too many unfinished spans, truncated...");
            }
         }

         private void reportReferenceCounters() {
            System.err.println(referenceCounters.size() + " not cleaned reference counters");
            int counter = 0;
            for (Map.Entry<Object, AtomicInteger> entry : referenceCounters.entrySet()) {
               System.out.printf("%s:%08x -> %d\n",  entry.getKey().getClass().getName(),
                                 entry.getKey().hashCode(), entry.getValue().get());
               if (++counter > 500) break; // shutdown hook must execute quickly
            }
            if (counter > 500) {
               System.out.println("Too many references, truncated...");
            }
         }
      });
   }

   /**
    * User entry into control flow
    */
   public void createManagedContext() {
      // destroy any pending context first
      Span span = contextSpan.get();
      if (span != null) {
         span.decrementRefCount(finishedSpans);
         contextSpan.remove();
      }
      contextManaged.set(true);
   }

   /**
    * User exit from control flow
    */
   public void destroyManagedContext() {
      Span span = contextSpan.get();
      if (span != null) {
         span.decrementRefCount(finishedSpans);
         contextSpan.remove();
      }
      contextManaged.remove();
   }

   public void incomingData(int length) {
      ensureSpan().addEvent(Event.Type.INCOMING_DATA, length + " b");
      contextManaged.set(true);
   }

   /**
    * The control flow will be passed to another thread. Either the new thread should call threadHandoverSuccess
    * or any thread should call threadHandoverFailure.
    * @param annotation
    */
   public void threadHandoverStarted(Object annotation) {
      incrementRefCount(annotation);
      Span span = ensureSpan();
      Span current = span.getCurrent();
      Span prev = spans.putIfAbsent(annotation, current);
      if (prev != null && prev != current) {
         throw new IllegalStateException();
      }
      span.incrementRefCount();
      span.addEvent(Event.Type.THREAD_HANDOVER_STARTED, null);
      //span.addEvent(Event.Type.THREAD_HANDOVER_STARTED, String.format("%s:%08x", annotation.getClass().getName(), annotation.hashCode()));
   }

   /**
    * Use only when thread-local span has been transformed into non-thread-local
    * In fact this is used when we do threadHandoverStart but the span on the origin thread shouldn't continue because
    * the context is unmanaged.
    */
   public void threadHandoverCompleted() {
      Boolean managed = contextManaged.get();
      if (managed != null) {
         return;
      }
      Span span = contextSpan.get();
      if (span == null) {
         return;
      }
      span.decrementRefCount(finishedSpans);
      contextSpan.remove();
   }

   /**
    * The control flow has returned to the point where the thread started processing this message
    */
   public void threadProcessingCompleted() {
      Span span = contextSpan.get();
      if (span == null) {
         return;
      }
      Integer localCount = contextCounter.get();
      if (localCount != null) {
         contextCounter.set(localCount == 1 ? null : localCount - 1);
      } else {
         span.addEvent(Event.Type.THREAD_PROCESSING_COMPLETE, null);
         span.decrementRefCount(finishedSpans);
         contextSpan.remove();
         contextManaged.remove();
      }
   }

   /**
    * The control flow was passed to another thread and this is now processing the annotation
    * @param annotation
    */
   public void threadHandoverSuccess(Object annotation) {
      Span span = contextSpan.get();
      if (span != null) {
         // we are in Runnable.run() executed directly in thread which already has context
         // which will be followed by threadProcessingComplete()
         Integer localCount = contextCounter.get();
         contextCounter.set(localCount == null ? 1 : localCount + 1);
         return;
      } else {
         span = decrementRefCount(annotation);
      }
      if (span == null) {
         //debug(String.format("No span for %s:%08x", annotation.getClass().getName(), annotation.hashCode()));
         return;
      }
      span.addEvent(Event.Type.THREAD_HANDOVER_SUCCESS, null);
      //span.addEvent(Event.Type.THREAD_HANDOVER_SUCCESS, String.format("%s:%08x", annotation.getClass().getName(), annotation.hashCode()));
      contextSpan.set(span);
      contextManaged.set(true);
   }

   public void threadHandoverFailure(Object annotation) {
      Span span = decrementRefCount(annotation);
      if (span == null) {
         return;
      }
      span.addEvent(Event.Type.THREAD_HANDOVER_FAILURE, null);
      //span.addEvent(Event.Type.THREAD_HANDOVER_FAILURE, String.format("%s:%08x", annotation.getClass().getName(), annotation.hashCode()));
      span.decrementRefCount(finishedSpans);
   }

   public void forkSpan() {
      Span current = contextSpan.get();

       //This might be wrongly inserted in Byteman rules
       if (current == null){
           System.err.println("Possible problem with the rules: Invoking \"forkSpan\" with empty contextSpan");
           return;
       }
      Span child = new Span(current);
      contextSpan.set(child);
      //System.err.printf("%s start %08x %08x -> %08x\n", Thread.currentThread().getName(), contextAnnotation.get().hashCode(), current.hashCode(), child.hashCode());
   }

   public void unforkSpan() {
      switchToParent();
      //System.err.printf("%s finish %08x %08x -> %08x\n", Thread.currentThread().getName(), contextAnnotation.get().hashCode(), current.hashCode(), current.getParent().hashCode());
   }

   public BatchSpan startNewBatch() {
      Span current = contextSpan.get();

      BatchSpan batchSpan = BatchSpan.newOrphan(current);
      contextSpan.set(batchSpan);
      batchSpan.addEvent(Event.Type.BATCH_PROCESSING_START, null);
      return batchSpan;
   }

   public void endNewBatch() {
      Span current = contextSpan.get();
      if (!(current instanceof BatchSpan)) {
         throw new IllegalStateException();
      }
      current.addEvent(Event.Type.BATCH_PROCESSING_END, null);
      Span suppressed = ((BatchSpan) current).getSuppressed();
      if (suppressed == null) {
         throw new IllegalStateException();
      }
      contextSpan.set(suppressed);
   }

   public void handlingMessage(String messageId) {
      Span span = contextSpan.get();
      //This might be wrongly inserted in Byteman rules
      if (span == null){
          System.err.println("Possible problem with the rules: Invoking \"handlingMessage\" with empty contextSpan");
          return;
      }
      span.addEvent(Event.Type.MSG_PROCESSING_START, messageId);
      span.setIncoming(messageId);
   }

   public void batchProcessingStart(List<String> messageIds) {
      Span current = contextSpan.get();
       //This might be wrongly inserted in Byteman rules
       if (current == null){
           System.err.println("Possible problem with the rules: Invoking \"batchProcessingStart\" with empty contextSpan");
           return;
       }

      BatchSpan batchSpan = BatchSpan.newChild(current, messageIds);
      contextSpan.set(batchSpan);
      StringBuilder sb = new StringBuilder();
      for (String mid : messageIds) {
         sb.append(mid).append("||");
      }
      batchSpan.addEvent(Event.Type.BATCH_PROCESSING_START, sb.toString());
   }

   public void batchProcessingEnd() {
      contextSpan.get().addEvent(Event.Type.BATCH_PROCESSING_END, null);
      switchToParent();
   }

   private void switchToParent() {
      Span current = contextSpan.get();

       //This might be wrongly inserted in Byteman rules
       if (current == null){
           System.err.println("Possible problem with the rules: Invoking \"switchToParent\" with empty contextSpan");
           return;
       }

       if (current.getParent() == null) {
         throw new IllegalStateException("Current span has no parent");
      }
      contextSpan.set(current.getParent());
   }

   public void batchPush(String messageId) {
      Span current = contextSpan.get();


       //This might be wrongly inserted in Byteman rules
       if (current == null){
           System.err.println("Possible problem with the rules: Invoking \"batchPush\" with empty contextSpan");
           return;
       }

      if (current instanceof BatchSpan) {
         ((BatchSpan) current).push(messageId);
      } else {
         throw new IllegalStateException("Current span is: " + current);
      }
   }

   public void batchPop() {
      Span current = contextSpan.get();

       //This might be wrongly inserted in Byteman rules
       if (current == null){
           System.err.println("Possible problem with the rules: Invoking \"batchPop\" with empty contextSpan");
           return;
       }

      if (current instanceof BatchSpan) {
         ((BatchSpan) current).pop();
      } else {
         throw new IllegalStateException("Current span is: " + current);
      }
   }

   public void discardMessages(List<String> messageIds) {
      if (messageIds == null) return;
      Span mf = ensureSpan();
      for (String messageId : messageIds) {
         Span child = new Span(mf);
         child.setIncoming(messageId);
         child.addEvent(Event.Type.DISCARD, messageId);
      }
   }

    /***
     *
     * @return The current span, or creates new one and set it in contextSpan
     */
   private Span ensureSpan() {
      Span span = contextSpan.get();

      if (span == null) {
         span = new Span();
         contextSpan.set(span);
      }
      return span;
   }

   private void incrementRefCount(Object annotation) {
      AtomicInteger refCount = referenceCounters.putIfAbsent(annotation, new AtomicInteger(1));
      if (refCount != null) {
         int count = refCount.incrementAndGet();
//         debug(String.format("%s:%08x inc to %d", annotation.getClass().getName(), annotation.hashCode(), count));
      } else {
//         debug(String.format("%s:%08x set to 1", annotation.getClass().getName(), annotation.hashCode()));
      }
   }

   private Span decrementRefCount(Object annotation) {
      AtomicInteger refCount = referenceCounters.get(annotation);
      if (refCount == null) {
//         debug(String.format("%s:%08x no refcount", annotation.getClass().getName(), annotation.hashCode()));
         return null;
      }
      int count = refCount.decrementAndGet();
//      debug(String.format("%s:%08x dec to %d", annotation.getClass().getName(), annotation.hashCode(), count));
      if (count == 0) {
         referenceCounters.remove(annotation);
         Span span = spans.remove(annotation);
//         debug(String.format("%s rem %08x -> %08x", Thread.currentThread().getName(), annotation.hashCode(), span.hashCode()));
         return span;
      } else {
         return spans.get(annotation);
      }
   }

   /**
    * We are about to send message (sync/async) to another node
    */
   public void outcomingStarted(String messageId) {
      Span span = ensureSpan();
      span.addOutcoming(messageId);
      span.addEvent(Event.Type.OUTCOMING_DATA_STARTED, messageId);
   }

   public void outcomingFinished() {
      Span span = contextSpan.get();

       //This might be wrongly inserted in Byteman rules
       if (span == null){
           System.err.println("Possible problem with the rules: Invoking \"outcomingFinished\" with empty contextSpan");
           return;
       }
      span.addEvent(Event.Type.OUTCOMING_DATA_FINISHED, null);
      if (contextManaged.get() == null) {
         span.decrementRefCount(finishedSpans);
         contextSpan.remove();
         contextManaged.remove();
      }
   }

   public void setNonCausal() {
      Span span = contextSpan.get();
      if (span != null) {
         span.setNonCausal();
      }
   }

   /**
    * Important event
    * @param message
    */
   public void checkpoint(String message) {
      if (contextSpan.get() == null) {
        //  System.err.println("No span in checkpoint for: " + message);
      }
      ensureSpan().addEvent(Event.Type.CHECKPOINT, message);
   }

   public void traceTag(String tag) {
      ensureSpan().addEvent(Event.Type.TRACE_TAG, tag);
   }

   public void msgTag(String tag) {
      ensureSpan().addEvent(Event.Type.MESSAGE_TAG, tag);
   }

   public void msgTagWithClass(Object object) {
      ensureSpan().addEvent(Event.Type.MESSAGE_TAG, object.getClass().getSimpleName());
   }

   public void stackpoint() {
      ensureSpan().addEvent(Event.Type.STACKPOINT, getStackTrace());
   }

   public static String getStackTrace() {
      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
      StringBuilder text = new StringBuilder("STACK");
      for (StackTraceElement ste : stackTrace) {
         text.append(" at ").append(ste);
      }
      return text.toString();
   }

   public void setMark(Object obj, String mark) {
      if (mark == null) {
         return;
      }
      if (markedObjects.putIfAbsent(obj, mark) != null) {
         throw new IllegalStateException("Object already marked");
      }
   }

   public void setContextMarkWithClass(Object o) {
      setContextMark(o.getClass().getSimpleName());
   }

   public void setContextMark(String mark) {
      contextMark.set(mark);
   }

   public void setMarkFromContext(Object obj) {
      setMark(obj, contextMark.get());
   }

   public String getMark(Object obj) {
      return markedObjects.get(obj);
   }

   public void removeMark(Object obj) {
      String mark = markedObjects.remove(obj);
      //System.err.println("Removed mark " + mark);
   }

   public String getLastMsgTag() {
      Span span = contextSpan.get();
      if (span == null) return null;
      return span.getLastMsgTag();
   }

   public void debug(String message) {
      System.err.println(Thread.currentThread().getName() + ": " + message);
   }

   public String simpleName(Object object) {
      return object.getClass().getSimpleName();
   }

   public String toString(Object object) {
      return object == null ? "null" : object.toString();
   }
}
