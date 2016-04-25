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
import org.jboss.qa.jdg.messageflow.objects.MessageId;
import org.jboss.qa.jdg.messageflow.objects.Span;
import org.jboss.qa.jdg.messageflow.persistence.BinaryPersister;
import org.jboss.qa.jdg.messageflow.persistence.Persister;
import org.jboss.qa.jdg.messageflow.persistence.TextPersister;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
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
   private static final boolean logAnnotations = Boolean.getBoolean("org.jboss.qa.messageflowtracer.logAnnotations");

   private static ThreadLocal<Context> context = new ThreadLocal<>();

   private static class Context {
      Span span;
      int counter;
      boolean managed;
      String mark;
   }

   private static ConcurrentHashMap<Object, String> markedObjects = new ConcurrentHashMap<Object, String>();

   private static volatile boolean running = true;

   static {
      final Thread writer = new Thread() {
         @Override
         public void run() {
            String path = System.getProperty("org.jboss.qa.messageflowtracer.output");
            if (path == null) {
               String suffixProperty = System.getProperty("org.jboss.qa.messageflowtracer.suffix.property");
               String suffix = suffixProperty == null ? null : System.getProperty(suffixProperty);
               String dir = System.getProperty("org.jboss.qa.messageflowtracer.outputdir", "/tmp");
               if (suffix != null) {
                  path = Paths.get(dir, "span." + suffix + ".txt").toString();
               } else {
                  try {
                     path = File.createTempFile("span.", ".txt", new File(dir)).getAbsolutePath();
                  } catch (IOException e) {
                     path = "/tmp/span.txt";
                  }
               }
            }
            boolean binarySpans = System.getProperty("org.jboss.qa.messageflowtracer.binarySpans") != null ? true : false;
            Persister persister = binarySpans ? new BinaryPersister() : new TextPersister();
            try {
               persister.open(path, System.nanoTime(), System.currentTimeMillis());
               while (running || !finishedSpans.isEmpty()) {
                  Span span;
                  while ((span = finishedSpans.poll()) != null) {
                     persister.write(span, false);
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
               try {
                  persister.close();
               } catch (IOException e) {
                  e.printStackTrace();
               }
            }
            synchronized (Tracer.class) {
               Tracer.class.notifyAll();
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
               entry.getValue().print(System.out, "");
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
      Context context = this.context.get();
      if (context != null) {
         if (context.span != null) {
            context.span.decrementRefCount(finishedSpans);
            context = null;
         }
      } else {
         this.context.set(context = new Context());
      }
      context.managed = true;
   }

   /**
    * User exit from control flow
    */
   public void destroyManagedContext() {
      Context context = this.context.get();
      if (context == null) {
         return;
      }
      if (context.span != null) {
         context.span.decrementRefCount(finishedSpans);
         context.span = null;
      }
      context.managed = false;
   }

   public void incomingData(int length) {
      Context context = ensureContextSpan();
      context.span.addEvent(Event.Type.INCOMING_DATA, length + " b");
      context.managed = true;
   }

   /**
    * The control flow will be passed to another thread. Either the new thread should call threadHandoverSuccess
    * or any thread should call threadHandoverFailure.
    * @param annotation
    */
   public void threadHandoverStarted(Object annotation) {
      incrementRefCount(annotation);
      Context context = ensureContextSpan();
      Span current = context.span.getCurrent();
      Span prev = spans.putIfAbsent(annotation, current);
      if (prev != null && prev != current) {
         throw new IllegalStateException();
      }
      context.span.incrementRefCount();
      context.span.addEvent(Event.Type.THREAD_HANDOVER_STARTED, logAnnotation(annotation));
   }

   public String logAnnotation(Object annotation) {
      return logAnnotations ? String.format("%s:%08x", annotation.getClass().getName(), annotation.hashCode()) : null;
   }

   /**
    * Use only when thread-local span has been transformed into non-thread-local
    * In fact this is used when we do threadHandoverStart but the span on the origin thread shouldn't continue because
    * the context is unmanaged.
    */
   public void threadHandoverCompleted() {
      Context context = this.context.get();
      if (context.managed) {
         return;
      }
      if (context.span == null) {
         return;
      }
      context.span.decrementRefCount(finishedSpans);
      context.span = null;
   }

   /**
    * The control flow has returned to the point where the thread started processing this message
    */
   public void threadProcessingCompleted() {
      Context context = this.context.get();
      if (context == null || context.span == null) {
         return;
      }
      if (context.counter > 0) {
         --context.counter;
      } else {
         context.span.addEvent(Event.Type.THREAD_PROCESSING_COMPLETE, null);
         context.span.decrementRefCount(finishedSpans);
         context.span = null;
         context.managed = false;
      }
   }

   /**
    * The control flow was passed to another thread and this is now processing the annotation
    * @param annotation
    */
   public void threadHandoverSuccess(Object annotation) {
      Context context = this.context.get();
      if (context != null) {
         if (context.span != null) {
            // we are in Runnable.run() executed directly in thread which already has context
            // which will be followed by threadProcessingComplete()
            ++context.counter;
            return;
         }
      } else {
         this.context.set(context = new Context());
      }

      context.span = decrementRefCount(annotation);
      if (context.span == null) {
         //debug(String.format("No span for %s:%08x", annotation.getClass().getName(), annotation.hashCode()));
         return;
      }
//      span.addEvent(Event.Type.THREAD_HANDOVER_SUCCESS, null);
      context.span.addEvent(Event.Type.THREAD_HANDOVER_SUCCESS, logAnnotation(annotation));
      context.managed = true;
   }

   public void threadHandoverFailure(Object annotation) {
      Span span = decrementRefCount(annotation);
      if (span == null) {
         return;
      }
//      span.addEvent(Event.Type.THREAD_HANDOVER_FAILURE, null);
      span.addEvent(Event.Type.THREAD_HANDOVER_FAILURE, logAnnotation(annotation));
      span.decrementRefCount(finishedSpans);
   }

   /**
    * Forked span separates unrelated flows, while BatchSpan multiplexes processing that affects all child spans.
    */
   public void forkSpan() {
      Context context = this.context.get();

      //This might be wrongly inserted in Byteman rules
      if (context == null || context.span == null) {
         System.err.println(Thread.currentThread().getName() + "Possible problem with the rules: Invoking \"forkSpan\" with empty contextSpan");
         new Throwable().fillInStackTrace().printStackTrace();
         return;
      }
      context.span = new Span(context.span);
      //System.err.printf("%s start %08x %08x -> %08x\n", Thread.currentThread().getName(), contextAnnotation.get().hashCode(), current.hashCode(), child.hashCode());
   }

   public void unforkSpan() {
      switchToParent(this.context.get());
      //System.err.printf("%s finish %08x %08x -> %08x\n", Thread.currentThread().getName(), contextAnnotation.get().hashCode(), current.hashCode(), current.getParent().hashCode());
   }

   public BatchSpan startNewBatch() {
      Context context = this.context.get();

      BatchSpan batchSpan = BatchSpan.newOrphan(context.span);
      batchSpan.addEvent(Event.Type.BATCH_PROCESSING_START, null);
      context.span = batchSpan;
      return batchSpan;
   }

   public void endNewBatch() {
      Context context = this.context.get();
      if (context == null || !(context.span instanceof BatchSpan)) {
         throw new IllegalStateException(String.valueOf(context));
      }
      context.span.addEvent(Event.Type.BATCH_PROCESSING_END, null);
      context.span.decrementRefCount(finishedSpans);
      Span suppressed = ((BatchSpan) context.span).getSuppressed();
      if (suppressed == null) {
         throw new IllegalStateException();
      }
      context.span = suppressed;
   }

   public void handlingMessage(MessageId messageId) {
      Context context = this.context.get();
      //This might be wrongly inserted in Byteman rules
      if (context == null || context.span == null){
          throw new IllegalStateException("Possible problem with the rules: Invoking \"handlingMessage\" with empty contextSpan");
      }
      context.span.addEvent(Event.Type.MSG_PROCESSING_START, messageId);
      context.span.setIncoming(messageId);
   }

   public void batchProcessingStart(List<MessageId> messageIds) {
      Context context = this.context.get();
      if (context == null || context.span == null) {
         System.err.println("Possible problem with the rules: Invoking \"batchProcessingStart\" with empty contextSpan");
         return;
      }

      context.span = BatchSpan.newChild(context.span, messageIds);
      StringBuilder sb = new StringBuilder();
      context.span.addEvent(Event.Type.BATCH_PROCESSING_START, messageIds);
   }

   public void batchProcessingEnd() {
      Context context = this.context.get();
      context.span.addEvent(Event.Type.BATCH_PROCESSING_END, null);
      switchToParent(context);
   }

   private void switchToParent(Context context) {
      if (context == null || context.span == null) {
         System.err.println("Possible problem with the rules: Invoking \"switchToParent\" with empty contextSpan");
         return;
      }

      if (context.span.getParent() == null) {
         throw new IllegalStateException("Current span has no parent");
      }
      context.span = context.span.getParent();
   }

   public void batchPush(MessageId messageId) {
      Context context = this.context.get();

      if (context == null || context.span == null) {
         System.err.println("Possible problem with the rules: Invoking \"batchPush\" with empty contextSpan");
         return;
      }

      if (context.span instanceof BatchSpan) {
         ((BatchSpan) context.span).push(messageId);
      } else {
         throw new IllegalStateException("Current span is: " + context.span);
      }
   }

   public void batchPop() {
      Context context = this.context.get();

      if (context == null || context.span == null) {
         System.err.println("Possible problem with the rules: Invoking \"batchPop\" with empty contextSpan");
         return;
      }

      if (context.span instanceof BatchSpan) {
         ((BatchSpan) context.span).pop();
      } else {
         throw new IllegalStateException("Current span is: " + context.span);
      }
   }

   public void discardMessages(List<MessageId> messageIds) {
      if (messageIds == null) return;
      Context context = ensureContextSpan();
      for (MessageId messageId : messageIds) {
         Span child = new Span(context.span);
         child.setIncoming(messageId);
         child.addEvent(Event.Type.DISCARD, messageId);
      }
   }

    /***
     *
     * @return The current span, or creates new one and set it in contextSpan
     */
   private Context ensureContextSpan() {
      Context context = this.context.get();
      if (context == null) {
         this.context.set(context = new Context());
      }
      if (context.span == null) {
         context.span = new Span();
      }
      return context;
   }

   private void incrementRefCount(Object annotation) {
      AtomicInteger refCount = referenceCounters.putIfAbsent(annotation, new AtomicInteger(1));
      if (refCount != null) {
         refCount.incrementAndGet();
      }
   }

   private Span decrementRefCount(Object annotation) {
      AtomicInteger refCount = referenceCounters.get(annotation);
      if (refCount == null) {
         return null;
      }
      int count = refCount.decrementAndGet();
      if (count == 0) {
         referenceCounters.remove(annotation);
         Span span = spans.remove(annotation);
         return span;
      } else {
         return spans.get(annotation);
      }
   }

   /**
    * We are about to send message (sync/async) to another node
    */
   public void outcomingStarted(MessageId messageId) {
      Context context = ensureContextSpan();
      context.span.addOutcoming(messageId);
      context.span.addEvent(Event.Type.OUTCOMING_DATA_STARTED, messageId);
   }

   public void outcomingFinished() {
      Context context = this.context.get();

      if (context == null || context.span == null){
         System.err.println(Thread.currentThread().getName() + "Possible problem with the rules: Invoking \"outcomingFinished\" with empty contextSpan");
         new Throwable().fillInStackTrace().printStackTrace();
         return;
      }
      context.span.addEvent(Event.Type.OUTCOMING_DATA_FINISHED, null);
      if (!context.managed) {
         context.span.decrementRefCount(finishedSpans);
         context.span = null;
         context.managed = false;
      }
   }

   public void setNonCausal() {
      Context context = this.context.get();
      if (context != null && context.span != null) {
         context.span.setNonCausal();
      }
   }

   /**
    * Important event
    * @param message
    */
   public void checkpoint(String message) {
      Context context = this.context.get();
      if (context == null) {
        //  System.err.println("No span in checkpoint for: " + message);
         return;
      }
      if (context.span == null) {
         context.span = new Span();
      }
      context.span.addEvent(Event.Type.CHECKPOINT, message);
   }

   public void checkpointWithClass(String message, Object object) {
      checkpoint(message + object.getClass().getSimpleName());
   }

   public void traceTag(String tag) {
      ensureContextSpan().span.addEvent(Event.Type.TRACE_TAG, tag);
   }

   public void msgTag(String tag) {
      ensureContextSpan().span.addEvent(Event.Type.MESSAGE_TAG, tag);
   }

   public void msgTagWithClass(Object object) {
      ensureContextSpan().span.addEvent(Event.Type.MESSAGE_TAG, object.getClass().getSimpleName());
   }

   public void stackpoint() {
      ensureContextSpan().span.addEvent(Event.Type.STACKPOINT, getStackTrace());
   }

   public static String getStackTrace() {
      return Arrays.toString(Thread.currentThread().getStackTrace());
//      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
//      StringBuilder text = new StringBuilder("STACK");
//      for (StackTraceElement ste : stackTrace) {
//         text.append(" at ").append(ste);
//      }
//      return text.toString();
   }

   public void setMark(Object obj, String mark) {
      if (mark == null) {
         return;
      }
      String oldMark;
      if ((oldMark = markedObjects.putIfAbsent(obj, mark)) != null) {
         System.err.println("Object " + obj + " already marked with " + oldMark + " (now marking with " + mark + ")");
         new Throwable().fillInStackTrace().printStackTrace();
         throw new IllegalStateException("Object " + obj + " already marked with " + oldMark + " (now marking with " + mark + ")");
      } else {
//         System.err.println("Marking object " + obj + " with " + mark);
//         new Throwable().fillInStackTrace().printStackTrace();
      }
   }

   public void setContextMarkWithClass(Object o) {
      setContextMark(o.getClass().getSimpleName());
   }

   public void setContextMark(String mark) {
      Context context = this.context.get();
      if (context == null) {
         this.context.set(context = new Context());
      }
      context.mark = mark;
   }

   public void setMarkFromContext(Object obj) {
      Context context = this.context.get();
      setMark(obj, context == null ? null : context.mark);
   }

   public String getMark(Object obj) {
      return markedObjects.get(obj);
   }

   public void removeMark(Object obj) {
      String mark = markedObjects.remove(obj);
      //System.err.println("Removed mark " + mark);
   }

   public String getLastMsgTag() {
      Context context = this.context.get();
      if (context == null || context.span == null) return null;
      return context.span.getLastMsgTag();
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
