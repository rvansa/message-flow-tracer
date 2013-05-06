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
               }
            }
            reportReferenceCounters();
            reportSpans();
            System.err.println(finishedSpans.size() + " not written finished spans.");
         }

         private void reportSpans() {
            System.err.println(spans.size() + " unfinished spans");
            int counter = 0;
            for (Map.Entry<Object, Span> entry : spans.entrySet()) {
               System.out.printf("%08x (refcount=%s) -> ", entry.getKey().hashCode(), referenceCounters.get(entry.getKey()));
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
               System.out.printf("%08x -> %d\n", entry.getKey().hashCode(), entry.getValue().get());
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
      contextAnnotation.set(new Object());
   }

   /**
    * User exit from control flow
    */
   public void destroyManagedContext() {
      Span span = contextSpan.get();
      if (span.isThreadLocalOnly()) {
         finishedSpans.add(span);
      }
      cleanContext();
   }

   public void incomingData() {
      ensureSpan().addEvent(Event.Type.INCOMING_DATA, null);
   }

   public void setContextData(Object annotation) {
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
      span.setThreadLocalOnly(false);
      span.addEvent(Event.Type.THREAD_HANDOVER_STARTED, null);
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

   public void threadHandoverFailure() {
      ensureSpan().addEvent(Event.Type.THREAD_HANDOVER_FAILURE, null);
      decrementRefCount(contextAnnotation.get());
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
      //System.err.printf("%s finish %08x %08x -> %08x\n", Thread.currentThread().getName(), contextAnnotation.get().hashCode(), current.hashCode(), current.parent.hashCode());
      if (!spans.replace(contextAnnotation.get(), current, current.getParent())) {
         throw new IllegalStateException();
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
         //System.err.printf("%08x inc to %d in %s\n", annotation.hashCode(), count, Thread.currentThread().getName());
      }
   }

   private void decrementRefCount(Object annotation) {
      AtomicInteger refCount = referenceCounters.get(annotation);
      if (refCount == null) {
         throw new IllegalStateException();
      }
      //System.out.printf("%08x refcount = %d in %s\n", annotation.hashCode(), refCount.get(), Thread.currentThread().getName());
      if (refCount.decrementAndGet() == 0) {
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
      ensureSpan().addEvent(Event.Type.THREAD_PROCESSING_COMPLETE, null);
      decrementRefCount(contextAnnotation.get());
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
         finishedSpans.add(span);
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
      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
      StringBuilder message = new StringBuilder("STACK");
      for (StackTraceElement ste : stackTrace) {
         message.append(" at ").append(ste);
      }
      ensureSpan().addEvent(Event.Type.STACKPOINT, message.toString());
   }

   public boolean hasContextData() {
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

   public void setMarkFromLastMsgTag(Object obj) {
      setMark(obj, getLastMsgTag());
   }

   public String getMark(Object obj) {
      return markedObjects.get(obj);
   }

   public void removeMark(Object obj) {
      String mark = markedObjects.remove(obj);
      System.err.println("Removed mark " + mark);
   }

   public String getLastMsgTag() {
      Span span = contextSpan.get();
      if (span == null) return null;
      return span.getLastMsgTag();
   }
}
