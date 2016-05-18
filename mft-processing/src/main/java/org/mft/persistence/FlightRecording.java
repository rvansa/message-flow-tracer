package org.mft.persistence;

import com.jrockit.mc.common.IMCMethod;
import com.jrockit.mc.flightrecorder.FlightRecordingLoader;
import com.jrockit.mc.flightrecorder.internal.model.FLRStackTrace;
import com.jrockit.mc.flightrecorder.internal.model.FLRThread;
import com.jrockit.mc.flightrecorder.provider.EventStorage;
import com.jrockit.mc.flightrecorder.provider.EventType;
import com.jrockit.mc.flightrecorder.spi.IEvent;
import com.jrockit.mc.flightrecorder.spi.IEventType;
import com.jrockit.mc.flightrecorder.spi.IView;
import org.mft.objects.Event;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class FlightRecording {
   public static final byte[] TAG = new byte[] { 'F', 'L', 'R' };
   private final Input input;
   private Map<String, NavigableMap<Long, Event>> events = new HashMap<>();
   private NavigableMap<Long, Event> globalEvents = new TreeMap<>();
   private AtomicInteger samples = new AtomicInteger();
   private AtomicInteger threadParks = new AtomicInteger();
   private AtomicInteger threadSleeps = new AtomicInteger();
   private AtomicInteger monitorEnters = new AtomicInteger();
   private AtomicInteger monitorWaits = new AtomicInteger();
   private AtomicInteger garbageCollections = new AtomicInteger();

   public FlightRecording(Input input) {
      this.input = input;
   }

   public Input getInput() {
      return input;
   }

   public NavigableMap<Long, Event> getEvents(String threadName) {
      return events.get(threadName);
   }

   public void load(String source) {
      try {
         IView view = FlightRecordingLoader.loadStream(input.stream()).createView();
         for (IEventType type : view.getEventTypes()) {
            String path = type.getPath();
            if (path.equals("vm/prof/execution_sample")) {
               processEvents(type, (infos, start, end, event, thread, stackTrace) -> {
//                  String state = (String) event.getValue("state");
                  samples.incrementAndGet();
                  infos.put(start, new Event(start, source, thread.getThreadName(),
                     Event.Type.EXECUTION_SAMPLE, toStackTrace(stackTrace)));
               });
            } else if (path.equals("java/thread_park")) {
               processEvents(type, (infos, start, end, event, thread, stackTrace) -> {
                  threadParks.incrementAndGet();
                  infos.put(start, new Event(start, source, thread.getThreadName(), Event.Type.THREAD_PARK_START, toStackTrace(stackTrace)));
                  infos.put(end, new Event(end, source, thread.getThreadName(), Event.Type.THREAD_PARK_END, String.format("%.2f ms", event.getDuration() / 1000000d)));
               });
            } else if (path.equals("java/thread_sleep")) {
               processEvents(type, (infos, start, end, event, thread, stackTrace) -> {
                  threadSleeps.incrementAndGet();
                  infos.put(start, new Event(start, source, thread.getThreadName(), Event.Type.THREAD_SLEEP_START, toStackTrace(stackTrace)));
                  infos.put(end, new Event(end, source, thread.getThreadName(), Event.Type.THREAD_SLEEP_END, String.format("%.2f ms", event.getDuration() / 1000000d)));
               });
            } else if (path.equals("java/monitor_enter")) {
               processEvents(type, (infos, start, end, event, thread, stackTrace) -> {
                  monitorEnters.incrementAndGet();
//                  FLRThread blockedBy = (FLRThread) event.getValue("previousOwner");
                  infos.put(start, new Event(start, source, thread.getThreadName(), Event.Type.MONITOR_ENTER, toStackTrace(stackTrace)));
                  infos.put(end, new Event(end, source, thread.getThreadName(), Event.Type.MONITOR_EXIT, String.format("%.2f ms", event.getDuration() / 1000000d)));
               });
            } else if (path.equals("java/monitor_wait")) {
               processEvents(type, (infos, start, end, event, thread, stackTrace) -> {
                  monitorWaits.incrementAndGet();
                  infos.put(start, new Event(start, source, thread.getThreadName(), Event.Type.MONITOR_WAIT_START, toStackTrace(stackTrace)));
                  infos.put(end, new Event(end, source, thread.getThreadName(), Event.Type.MONITOR_WAIT_END, String.format("%.2f ms", event.getDuration() / 1000000d)));
               });
            } else if (path.equals("vm/gc/collector/garbage_collection")) {
               garbageCollections.incrementAndGet();
               processGlobalEvents(type, (start, end, event) -> {
                  String description = event.getValue("name") + " " + event.getValue("gcId") + "|sum=" + event.getValue("sumOfPauses") + " ns|longest=" + event.getValue("longestPause") + " ns";
                  globalEvents.put(start, new Event(start, source, "(JVM)", Event.Type.GC_START, description));
                  globalEvents.put(end, new Event(end, source, "(JVM)", Event.Type.GC_END, description));
               });
            }
         }
      } catch (IOException e) {
         e.printStackTrace();
      }
      System.err.printf("Samples: %6d, Parked: %6d, Sleeps: %6d, Monitor enters: %6d, Monitor waits: %6d, Garbage collections: %4d\n",
         samples.get(), threadParks.get(), threadSleeps.get(), monitorEnters.get(), monitorWaits.get(), garbageCollections.get());
   }

   public void processGlobalEvents(IEventType type, GlobalEventConsumer consumer) {
      for (EventStorage storage : ((EventType) type).getStorages()) {
          for (Iterator<IEvent> it = storage.iterator(); it.hasNext(); ) {
             IEvent event = it.next();
             long start = TimeUnit.NANOSECONDS.toMillis(event.getStartTimestamp());
             long end = TimeUnit.NANOSECONDS.toMillis(event.getEndTimestamp());
             consumer.consume(start, end, event);
          }
      }
   }

   public void processEvents(IEventType type, ThreadEventConsumer infoProcessor) {
      for (EventStorage storage : ((EventType) type).getStorages()) {
         NavigableMap<Long, Event> infos = null;
         Long threadJavaId = null;
         for (Iterator<IEvent> it = storage.iterator(); it.hasNext(); ) {
            IEvent event = it.next();
            FLRThread thread = (FLRThread) event.getValue("(thread)");
            if (thread == null) {
               // this can happen when the thread is already terminating
               continue;
            }
            if (threadJavaId == null || Long.compare(threadJavaId, thread.getJavaId()) != 0) {
               threadJavaId = thread.getJavaId();
               infos = events.get(thread.getName());
               if (infos == null) {
                  events.put(thread.getName(), infos = new TreeMap<>());
               }
            }
            FLRStackTrace stackTrace = (FLRStackTrace) event.getValue("(stackTrace)");
            long start = TimeUnit.NANOSECONDS.toMillis(event.getStartTimestamp());
            long end = TimeUnit.NANOSECONDS.toMillis(event.getEndTimestamp());
            infoProcessor.consume(infos, start, end, event, thread, stackTrace);
         }
      }
   }

   public NavigableMap<Long, Event> getGlobalEvents() {
      return globalEvents;
   }

   private interface ThreadEventConsumer {
      void consume(NavigableMap infos, long start, long end, IEvent event, FLRThread thread, FLRStackTrace stackTrace);
   }

   private interface GlobalEventConsumer {
      void consume(long start, long end, IEvent event);
   }

   public StackTraceElement[] toStackTrace(FLRStackTrace stackTrace) {
      return stackTrace.getFrames().stream().map(
         frame -> {
            IMCMethod m = frame.getMethod();
            if (m == null) return new StackTraceElement("?", "unknown class", "unknown method", 0);
            return new StackTraceElement(m.getPackageName() + "." + m.getClassName(),
               m.getMethodName(), m.getFileName(), orZero(m.getLineNumber()));
         }).toArray(StackTraceElement[]::new);
   }

   private int orZero(Integer lineNumber) {
      return lineNumber == null ? 0 : lineNumber;
   }
}
