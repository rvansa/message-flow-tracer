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
 * (such as Cache.put() call) on one node, tracking the requests into ControlFlows.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Tracer {
   private static ConcurrentHashMap<Object, ControlFlow> controlFlows = new ConcurrentHashMap<Object, ControlFlow>();
   private static ConcurrentHashMap<Object, AtomicInteger> referenceCounters = new ConcurrentHashMap<Object, AtomicInteger>();
   private static ConcurrentLinkedQueue<ControlFlow> finishedFlows = new ConcurrentLinkedQueue<ControlFlow>();

   private static ThreadLocal<ControlFlow> contextFlow = new ThreadLocal<ControlFlow>();
   private static ThreadLocal<Object> contextData = new ThreadLocal<Object>();

   private static ConcurrentHashMap<Object, String> markedObjects = new ConcurrentHashMap<Object, String>();

   private static volatile boolean running = true;

   static {
      Thread writer = new Thread() {
         @Override
         public void run() {
            String path = System.getProperty("org.jboss.qa.messageflowtracer.output");
            if (path == null) path = "/tmp/controlflow.txt";
            PrintStream writer = null;
            try {
               writer = new PrintStream(new BufferedOutputStream(new FileOutputStream(path)));
               writer.printf("%d=%d\n", System.nanoTime(), System.currentTimeMillis());
               while (running || !finishedFlows.isEmpty()) {
                  ControlFlow flow;
                  while ((flow = finishedFlows.poll()) != null) {
                     flow.writeTo(writer);
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
      writer.setName("MessageFlowWriter");
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
            reportControlFlows();
            System.err.println(finishedFlows.size() + " not written finished flows.");
         }

         private void reportControlFlows() {
            System.err.println(controlFlows.size() + " unfinished control flows");
            int counter = 0;
            for (Map.Entry<Object, ControlFlow> entry : controlFlows.entrySet()) {
               System.out.printf("%08x (refcount=%s) -> ", entry.getKey().hashCode(), referenceCounters.get(entry.getKey()));
               entry.getValue().writeTo(System.out);
               if (++counter > 500) break; // shutdown hook must execute quickly
            }
            if (counter > 500) {
               System.out.println("Too many control flows, truncated...");
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
      contextData.set(new Object());
   }

   /**
    * User exit from control flow
    */
   public void destroyManagedContext() {
      ControlFlow flow = contextFlow.get();
      if (flow.isThreadLocalOnly()) {
         finishedFlows.add(flow);
      }
      cleanContext();
   }

   public void incomingData() {
      ensureContextFlow().addEvent(Event.Type.INCOMING_DATA, null);
   }

   public void setContextData(Object data) {
      contextData.set(data);
      incrementRefCount(data);
      ControlFlow flow = contextFlow.get();
      if (flow != null) {
         //System.err.printf("%s set %08x -> %08x\n", Thread.currentThread().getName(), data.hashCode(), flow.hashCode());
         ControlFlow other = controlFlows.putIfAbsent(data, flow);
         if (other != null) {
            throw new IllegalStateException();
         }
         flow.setThreadLocalOnly(false);
      }
   }

   public void cleanContext() {
      contextData.remove();
      contextFlow.remove();
   }

   /**
    * The control flow will be passed to another thread. Either the new thread should call threadHandoverSuccess
    * or any thread should call threadHandoverFailure.
    * @param data
    */
   public void threadHandoverStarted(Object data) {
      incrementRefCount(data);
      ControlFlow mf = ensureContextFlow();
      ControlFlow prev = controlFlows.putIfAbsent(data, mf);
      if (prev != null && prev != mf) {
         throw new IllegalStateException();
      }
      mf.setThreadLocalOnly(false);
      mf.addEvent(Event.Type.THREAD_HANDOVER_STARTED, null);
   }

   /**
    * The control flow was passed to another thread and this is now processing the data
    * @param data
    */
   public void threadHandoverSuccess(Object data) {
      if (!referenceCounters.containsKey(data)) {
         // we have not registered handover start
         return;
      }
      contextData.set(data);
      ensureContextFlow().addEvent(Event.Type.THREAD_HANDOVER_SUCCESS, null);
   }

   public void threadHandoverFailure() {
      ensureContextFlow().addEvent(Event.Type.THREAD_HANDOVER_FAILURE, null);
      decrementRefCount(contextData.get());
   }

   public void messageHandlingStarted(String messageId) {
      ControlFlow current = contextFlow.get();
      ControlFlow child = new ControlFlow(current);
      contextFlow.set(child);
      child.setThreadLocalOnly(false);
      //System.err.printf("%s start %08x %08x -> %08x\n", Thread.currentThread().getName(), contextData.get().hashCode(), current.hashCode(), child.hashCode());
      if (!controlFlows.replace(contextData.get(), current, child)) {
         // the message has local origin
         ControlFlow other = controlFlows.putIfAbsent(contextData.get(), child);
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
      ControlFlow current = contextFlow.get();
      contextFlow.set(current.getParent());
      //System.err.printf("%s finish %08x %08x -> %08x\n", Thread.currentThread().getName(), contextData.get().hashCode(), current.hashCode(), current.parent.hashCode());
      if (!controlFlows.replace(contextData.get(), current, current.getParent())) {
         throw new IllegalStateException();
      }
   }

   public void discardMessages(List<String> messageIds) {
      if (messageIds == null) return;
      ControlFlow mf = ensureContextFlow();
      for (String messageId : messageIds) {
         ControlFlow child = new ControlFlow(mf);
         child.setIncoming(messageId);
         child.addEvent(Event.Type.DISCARD, messageId);
      }
   }

   public void associateData(Object newData) {
      if (newData == null) {
         throw new IllegalArgumentException();
      }
      ControlFlow flow = ensureContextFlow();
      //System.err.printf("%s assoc %08x -> %08x\n", Thread.currentThread().getName(), newData.hashCode(), flow.hashCode());
      ControlFlow old = controlFlows.putIfAbsent(newData, flow);
      if (old != null) {
//         System.err.println("Old flow:");
//         old.writeTo(System.err);
//         System.err.println("New flow:");
//         old.writeTo(System.err);
         throw new IllegalArgumentException("New data already have message flow");
      }
      flow.setThreadLocalOnly(false);
      flow.incrementRefCount();
   }

   private ControlFlow ensureContextFlow() {
      ControlFlow mf = contextFlow.get();
      if (mf == null) {
         Object data = contextData.get();
         if (data != null) {
            mf = controlFlows.get(data);
         }
         if (mf == null) {
            mf = new ControlFlow();
         }
         contextFlow.set(mf);
      }
      return mf;
   }

   private void incrementRefCount(Object data) {
      AtomicInteger refCount = referenceCounters.putIfAbsent(data, new AtomicInteger(1));
      if (refCount != null) {
         int count = refCount.incrementAndGet();
         //System.err.printf("%08x inc to %d in %s\n", data.hashCode(), count, Thread.currentThread().getName());
      }
   }

   private void decrementRefCount(Object data) {
      AtomicInteger refCount = referenceCounters.get(data);
      if (refCount == null) {
         throw new IllegalStateException();
      }
      //System.out.printf("%08x refcount = %d in %s\n", data.hashCode(), refCount.get(), Thread.currentThread().getName());
      if (refCount.decrementAndGet() == 0) {
         //System.out.printf("%08x removing refcount in %s\n", data.hashCode(), Thread.currentThread().getName());
         referenceCounters.remove(data);
         ControlFlow flow = controlFlows.remove(data);
         //System.err.printf("%s rem %08x -> %08x\n", Thread.currentThread().getName(), data.hashCode(), flow.hashCode());
         if (flow == null) {
            System.err.println("Data were not identified!");
            return;
         }
         flow.decrementRefCount(finishedFlows);
      }
   }

   /**
    * The control flow has returned to the point where the thread started processing this message
    */
   public void threadProcessingComplete() {
      ensureContextFlow().addEvent(Event.Type.THREAD_PROCESSING_COMPLETE, null);
      decrementRefCount(contextData.get());
      cleanContext();
   }

   /**
    * We are about to send message (sync/async) to another node
    */
   public void outcomingStarted(String messageId) {
      ControlFlow mf = ensureContextFlow();
      mf.addOutcoming(messageId);
      mf.addEvent(Event.Type.OUTCOMING_DATA_STARTED, messageId);
   }

   public void outcomingFinished() {
      ControlFlow flow = contextFlow.get();
      flow.addEvent(Event.Type.OUTCOMING_DATA_FINISHED, null);
      if (contextData.get() == null && flow.isThreadLocalOnly()) {
         finishedFlows.add(flow);
         contextFlow.remove();
      }
   }

   public void setNonCausal() {
      ControlFlow flow = contextFlow.get();
      if (flow != null) {
         flow.setNonCausal(true);
      }
   }

   /**
    * Important event
    * @param message
    */
   public void checkpoint(String message) {
      ensureContextFlow().addEvent(Event.Type.CHECKPOINT, message);
   }

   public void flowTag(String tag) {
      ensureContextFlow().addEvent(Event.Type.FLOW_TAG, tag);
   }

   public void msgTag(String tag) {
      ensureContextFlow().addEvent(Event.Type.MESSAGE_TAG, tag);
   }

   public void msgTagWithClass(Object object) {
      ensureContextFlow().addEvent(Event.Type.MESSAGE_TAG, object.getClass().getSimpleName());
   }

   public void stackpoint() {
      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
      StringBuilder message = new StringBuilder("STACK");
      for (StackTraceElement ste : stackTrace) {
         message.append(" at ").append(ste);
      }
      ensureContextFlow().addEvent(Event.Type.STACKPOINT, message.toString());
   }

   public boolean hasContextData() {
      return contextData.get() != null;
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
      ControlFlow flow = contextFlow.get();
      if (flow == null) return null;
      return flow.getLastMsgTag();
   }
}
