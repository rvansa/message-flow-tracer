package org.jboss.qa.jdg.messageflow.persistence;

import com.jrockit.mc.flightrecorder.FlightRecordingLoader;
import com.jrockit.mc.flightrecorder.internal.model.FLRStackTrace;
import com.jrockit.mc.flightrecorder.internal.model.FLRThread;
import com.jrockit.mc.flightrecorder.provider.EventStorage;
import com.jrockit.mc.flightrecorder.provider.EventType;
import com.jrockit.mc.flightrecorder.spi.IEvent;
import com.jrockit.mc.flightrecorder.spi.IEventType;
import com.jrockit.mc.flightrecorder.spi.IView;
import org.jboss.qa.jdg.messageflow.logic.Input;
import org.jboss.qa.jdg.messageflow.objects.Event;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
   private HashMap<String, NavigableMap<Long, Event>> events = new HashMap<>();
   private AtomicInteger samples = new AtomicInteger();
   private AtomicInteger threadParks = new AtomicInteger();
   private AtomicInteger threadSleeps = new AtomicInteger();
   private AtomicInteger monitorEnters = new AtomicInteger();
   private AtomicInteger monitorWaits = new AtomicInteger();

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
               processEvents(type, (infos, event, thread, stackTrace) -> {
//                  String state = (String) event.getValue("state");
                  samples.incrementAndGet();
                  long start = TimeUnit.NANOSECONDS.toMillis(event.getStartTimestamp());
                  infos.put(start, new Event(start, source, thread.getThreadName(),
                     Event.Type.EXECUTION_SAMPLE, toStackTrace(stackTrace)));
               });
            } else if (path.equals("java/thread_park")) {
               processEvents(type, (infos, event, thread, stackTrace) -> {
                  threadParks.incrementAndGet();
                  long start = TimeUnit.NANOSECONDS.toMillis(event.getStartTimestamp());
                  long end = TimeUnit.NANOSECONDS.toMillis(event.getEndTimestamp());
                  infos.put(start, new Event(start, source, thread.getThreadName(), Event.Type.THREAD_PARK_START, toStackTrace(stackTrace)));
                  infos.put(end, new Event(end, source, thread.getThreadName(), Event.Type.THREAD_PARK_END, String.format("%.2f ms", event.getDuration() / 1000000d)));
               });
            } else if (path.equals("java/thread_sleep")) {
               processEvents(type, (infos, event, thread, stackTrace) -> {
                  threadSleeps.incrementAndGet();
                  long start = TimeUnit.NANOSECONDS.toMillis(event.getStartTimestamp());
                  long end = TimeUnit.NANOSECONDS.toMillis(event.getEndTimestamp());
                  infos.put(start, new Event(start, source, thread.getThreadName(), Event.Type.THREAD_SLEEP_START, toStackTrace(stackTrace)));
                  infos.put(end, new Event(end, source, thread.getThreadName(), Event.Type.THREAD_SLEEP_END, String.format("%.2f ms", event.getDuration() / 1000000d)));
               });
            } else if (path.equals("java/monitor_enter")) {
               processEvents(type, (infos, event, thread, stackTrace) -> {
                  monitorEnters.incrementAndGet();
                  long start = TimeUnit.NANOSECONDS.toMillis(event.getStartTimestamp());
                  long end = TimeUnit.NANOSECONDS.toMillis(event.getEndTimestamp());
//                  FLRThread blockedBy = (FLRThread) event.getValue("previousOwner");
                  infos.put(start, new Event(start, source, thread.getThreadName(), Event.Type.MONITOR_ENTER, toStackTrace(stackTrace)));
                  infos.put(end, new Event(end, source, thread.getThreadName(), Event.Type.MONITOR_EXIT, String.format("%.2f ms", event.getDuration() / 1000000d)));
               });
            } else if (path.equals("java/monitor_wait")) {
               processEvents(type, (infos, event, thread, stackTrace) -> {
                  monitorWaits.incrementAndGet();
                  long start = TimeUnit.NANOSECONDS.toMillis(event.getStartTimestamp());
                  long end = TimeUnit.NANOSECONDS.toMillis(event.getEndTimestamp());
                  infos.put(start, new Event(start, source, thread.getThreadName(), Event.Type.MONITOR_WAIT_START, toStackTrace(stackTrace)));
                  infos.put(end, new Event(end, source, thread.getThreadName(), Event.Type.MONITOR_WAIT_END, String.format("%.2f ms", event.getDuration() / 1000000d)));
               });
            }
         }
      } catch (IOException e) {
         e.printStackTrace();
      }
      System.err.printf("Samples: %6d, Parked: %6d, Sleeps: %6d, Monitor enters: %6d, Monitor waits: %6d\n",
         samples.get(), threadParks.get(), threadSleeps.get(), monitorEnters.get(), monitorWaits.get());
   }

   public void processEvents(IEventType type, InfoProcessor infoProcessor) {
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
            infoProcessor.consume(infos, event, thread, stackTrace);
         }
      }
   }

   private interface InfoProcessor {
      void consume(NavigableMap infos, IEvent event, FLRThread thread, FLRStackTrace stackTrace);
   }

   public StackTraceElement[] toStackTrace(FLRStackTrace stackTrace) {
      return stackTrace.getFrames().stream().map(
         frame -> new StackTraceElement(
            frame.getMethod().getPackageName() + "." + frame.getMethod().getClassName(),
            frame.getMethod().getMethodName(), frame.getMethod().getFileName(), orZero(frame.getMethod().getLineNumber())))
         .toArray(StackTraceElement[]::new);
   }

   private int orZero(Integer lineNumber) {
      return lineNumber == null ? 0 : lineNumber;
   }
}
