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

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.qa.jdg.messageflow.objects.Event;
import org.jboss.qa.jdg.messageflow.objects.Span;

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
   private static ThreadLocal<Object> contextAnnotation = new ThreadLocal<Object>();
   private static ThreadLocal<String> contextMark = new ThreadLocal<String>();

   private static ConcurrentHashMap<Object, String> markedObjects = new ConcurrentHashMap<Object, String>();

   private static volatile boolean running = true;

   static {
      Thread writer = new Thread() {
         @Override
         public void run() {
            String path = System.getProperty("org.jboss.qa.messageflowtracer.output");
            if (path == null) path = "/tmp/span.txt";
            PrintStream writer = null;
            try {
               writer = new PrintStream(new BufferedOutputStream(new FileOutputStream(path)));
               writer.printf("%d=%d\n", System.nanoTime(), System.currentTimeMillis());
               while (running || !finishedSpans.isEmpty()) {
                  Span span;
                  while ((span = finishedSpans.poll()) != null) {
                     span.writeTo(writer);
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
               entry.getValue().writeTo(System.out);
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

   private static class ManagedAnnotation {}

   /**
    * User entry into control flow
    */
   public void createManagedContext() {
      // destroy any pending context first
      Span span = contextSpan.get();
      if (span != null) {
         span.decrementOrRetire(finishedSpans);
         contextSpan.remove();
      }
      contextAnnotation.set(new ManagedAnnotation());
   }

   public boolean hasManagedContext() {
      return contextAnnotation.get() instanceof ManagedAnnotation;
   }

   /**
    * User exit from control flow
    */
   public void destroyManagedContext() {
      Span span = contextSpan.get();
      if (span != null) {
         span.decrementOrRetire(finishedSpans);
      }
      cleanContext();
   }

   public void incomingData() {
      ensureSpan().addEvent(Event.Type.INCOMING_DATA, null);
   }

   public void setContextAnnotation(Object annotation) {
      contextAnnotation.set(annotation);
      incrementRefCount(annotation);
      Span span = contextSpan.get();
      if (span != null) {
         //System.err.printf("%s set %08x -> %08x\n", Thread.currentThread().getName(), annotation.hashCode(), span.hashCode());
         Span other = spans.putIfAbsent(annotation, span);
         if (other != null) {
            throw new IllegalStateException();
         }
         span.setThreadLocalOnly(false);
      }
   }

   public void cleanContext() {
      contextAnnotation.remove();
      contextSpan.remove();
   }

   /**
    * The control flow will be passed to another thread. Either the new thread should call threadHandoverSuccess
    * or any thread should call threadHandoverFailure.
    * @param annotation
    */
   public void threadHandoverStarted(Object annotation) {
      incrementRefCount(annotation);
      Span span = ensureSpan();
      Span prev = spans.putIfAbsent(annotation, span);
      if (prev != null && prev != span) {
         throw new IllegalStateException();
      }
      if (span.isThreadLocalOnly()) {
         span.setThreadLocalOnly(false);
         span.incrementRefCount();
         // now we have to call threadHandoverCompleted
      }
      span.addEvent(Event.Type.THREAD_HANDOVER_STARTED, null);
      //span.addEvent(Event.Type.THREAD_HANDOVER_STARTED, String.format("%s:%08x", annotation.getClass().getName(), annotation.hashCode()));
   }

   /**
    * Use only when thread-local span has been transformed into non-thread local
    */
   public void threadHandoverCompleted() {
      Span span = contextSpan.get();
      span.decrementRefCount(finishedSpans);
      cleanContext();
   }

   /**
    * The control flow was passed to another thread and this is now processing the annotation
    * @param annotation
    */
   public void threadHandoverSuccess(Object annotation) {
      if (!referenceCounters.containsKey(annotation)) {
         // we have not registered handover start
         return;
      }
      contextAnnotation.set(annotation);
      ensureSpan().addEvent(Event.Type.THREAD_HANDOVER_SUCCESS, null);
   }

   public void threadHandoverFailure(Object annotation) {
      ensureSpan().addEvent(Event.Type.THREAD_HANDOVER_FAILURE, null);
      //ensureSpan().addEvent(Event.Type.THREAD_HANDOVER_FAILURE, String.format("%s:%08x", annotation.getClass().getName(), annotation.hashCode()));
      decrementRefCount(annotation);
   }

   public void messageHandlingStarted(String messageId) {
      Span current = contextSpan.get();
      Span child = new Span(current);
      contextSpan.set(child);
      child.setThreadLocalOnly(false);
      //System.err.printf("%s start %08x %08x -> %08x\n", Thread.currentThread().getName(), contextAnnotation.get().hashCode(), current.hashCode(), child.hashCode());
      if (!spans.replace(contextAnnotation.get(), current, child)) {
         // the message has local origin
         Span other = spans.putIfAbsent(contextAnnotation.get(), child);
         if (other != null) {
            //System.err.printf("Should be %08x\n", current.hashCode());
            //current.writeTo(System.err);
            //System.err.printf("But it is %08x\n", other.hashCode());
            //other.writeTo(System.err);
            throw new IllegalStateException();
         }
      }
      child.addEvent(Event.Type.HANDLING, messageId);
      child.setIncoming(messageId);
   }

   public void messageHandlingFinished() {
      Span current = contextSpan.get();
      contextSpan.set(current.getParent());
      //System.err.printf("%s finish %08x %08x -> %08x\n", Thread.currentThread().getName(), contextAnnotation.get().hashCode(), current.hashCode(), current.getParent().hashCode());
      if (!spans.replace(contextAnnotation.get(), current, current.getParent())) {
//         System.err.println("Expected:");
//         current.writeWithChildren(System.err);
//         Span actual = spans.get(contextAnnotation.get());
//         if (actual != null) {
//            System.err.println("Actual:");
//            actual.writeWithChildren(System.err);
//         } else {
//            System.err.println("Actual is null");
//         }
         throw new IllegalStateException(String.format("For annotation %08x is expected different span", contextAnnotation.get().hashCode()));
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

   public void associateData(Object newAnnotation) {
      if (newAnnotation == null) {
         throw new IllegalArgumentException();
      }
      Span span = ensureSpan();
      //System.err.printf("%s assoc %08x -> %08x\n", Thread.currentThread().getName(), newAnnotation.hashCode(), span.hashCode());
      Span old = spans.putIfAbsent(newAnnotation, span);
      if (old != null) {
//         System.err.println("Old span:");
//         old.writeTo(System.err);
//         System.err.println("New span:");
//         span.writeTo(System.err);
         throw new IllegalArgumentException("New data already have span");
      }
      span.setThreadLocalOnly(false);
      span.incrementRefCount();
   }

   private Span ensureSpan() {
      Span span = contextSpan.get();
      if (span == null) {
         Object annotation = contextAnnotation.get();
         if (annotation != null) {
            span = spans.get(annotation);
         }
         if (span == null) {
            span = new Span();
         }
         contextSpan.set(span);
      }
      return span;
   }

   private void incrementRefCount(Object annotation) {
      AtomicInteger refCount = referenceCounters.putIfAbsent(annotation, new AtomicInteger(1));
      if (refCount != null) {
         int count = refCount.incrementAndGet();
//         System.err.printf("%s:%08x inc to %d in %s\n", annotation.getClass().getName(), annotation.hashCode(),
//                           count, Thread.currentThread().getName());
      } else {
//         System.err.printf("%s:%08x set to 1 in %s\n", annotation.getClass().getName(), annotation.hashCode(),
//                           Thread.currentThread().getName());
      }
   }

   private void decrementRefCount(Object annotation) {
      AtomicInteger refCount = referenceCounters.get(annotation);
      if (refCount == null) {
         throw new IllegalStateException(String.format("No refcount for %s:%08x", annotation.getClass().getName(), annotation.hashCode()));
      }
      int count = refCount.decrementAndGet();
//      System.out.printf("%s:%08x dec to %d in %s\n", annotation.getClass().getName(), annotation.hashCode(),
//                        count, Thread.currentThread().getName());
      if (count == 0) {
         //System.out.printf("%08x removing refcount in %s\n", annotation.hashCode(), Thread.currentThread().getName());
         referenceCounters.remove(annotation);
         Span span = spans.remove(annotation);
         //System.err.printf("%s rem %08x -> %08x\n", Thread.currentThread().getName(), annotation.hashCode(), span.hashCode());
         if (span == null) {
            System.err.println("Annotation was not identified!");
            return;
         }
         span.decrementRefCount(finishedSpans);
      }
   }

   /**
    * The control flow has returned to the point where the thread started processing this message
    */
   public void threadProcessingComplete() {
      Object annotation = contextAnnotation.get();
      if (annotation == null) {
         return;
      }
      ensureSpan().addEvent(Event.Type.THREAD_PROCESSING_COMPLETE, null);
      decrementRefCount(annotation);
      cleanContext();
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
      span.addEvent(Event.Type.OUTCOMING_DATA_FINISHED, null);
      if (contextAnnotation.get() == null && span.isThreadLocalOnly()) {
         span.decrementOrRetire(finishedSpans);
         contextSpan.remove();
      }
   }

   public void setNonCausal() {
      Span span = contextSpan.get();
      if (span != null) {
         span.setNonCausal(true);
      }
   }

   /**
    * Important event
    * @param message
    */
   public void checkpoint(String message) {
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

   public boolean hasContextAnnotation() {
      return contextAnnotation.get() != null;
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

}
