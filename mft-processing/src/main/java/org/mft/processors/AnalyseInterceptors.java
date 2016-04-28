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

package org.mft.processors;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.mft.objects.Event;
import org.mft.objects.Header;
import org.mft.objects.Trace;

/**
 * // TODO: Document this
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class AnalyseInterceptors implements Processor {
   private SortedMap<String, SortedMap<String, AvgMinMax>> times = new TreeMap<String, SortedMap<String, AvgMinMax>>();
   private PrintStream out = System.out;

   @Override
   public void process(Trace trace, long traceCounter) {
      String traceTag = "NO_TAG";
      Map<String, Long> preInvokePart = new HashMap<String, Long>();
      SortedMap<String, AvgMinMax> interceptorProcessingTime = new TreeMap<String, AvgMinMax>();
      Map<String, List<Event>> eventsBySpan = trace.getEventsBySpan();
      for (List<Event> spanEvents : eventsBySpan.values()) {
         Map<String, TimestampAndInterceptor> interceptorEvents = new HashMap<String, TimestampAndInterceptor>();
         for (Event event : spanEvents) {
            String text;
            if (event.type == Event.Type.CHECKPOINT && (text = (String) event.payload).startsWith("INT ")) {
               String[] parts = text.substring(4).split("@");
               TimestampAndInterceptor prev = interceptorEvents.get(parts[0]); // by command
               String commandAndInterceptor = parts[0] + "@" + parts[2];
               if (prev != null) {
                  if (parts[1].equals(">")) {
                     preInvokePart.put(prev.interceptor, event.nanoTime - prev.timestamp);
                  } else if (parts[1].equals("<")) {
                     AvgMinMax avg = interceptorProcessingTime.get(commandAndInterceptor);
                     if (avg == null) {
                        interceptorProcessingTime.put(commandAndInterceptor, avg = new AvgMinMax());
                     }
                     Long preInvokeTime = preInvokePart.get(commandAndInterceptor);
                     avg.add(event.nanoTime - prev.timestamp + (preInvokeTime == null ? 0 : preInvokeTime.longValue()));
                  } else {
                     throw new IllegalArgumentException();
                  }
               }
               interceptorEvents.put(parts[0], new TimestampAndInterceptor(event.nanoTime, parts[2]));
            } else if (event.type == Event.Type.TRACE_TAG) {
               traceTag = (String) event.payload;
            }
         }
      }
      SortedMap<String, AvgMinMax> forTagged = times.get(traceTag);
      if (forTagged == null) {
         times.put(traceTag, interceptorProcessingTime);
      } else {
         for (Map.Entry<String, AvgMinMax> entry : interceptorProcessingTime.entrySet()) {
            AvgMinMax other = forTagged.get(entry.getKey());
            if (other == null) {
               forTagged.put(entry.getKey(), entry.getValue());
            } else {
               other.add(entry.getValue());
            }
         }
      }
   }

   @Override
   public void processHeader(Header header) {
   }

   @Override
   public void finish() {
      out.println("\n****************");
      out.println("* INTERCEPTORS *");
      out.println("****************");
      for (Map.Entry<String, SortedMap<String, AvgMinMax>> tagEntry : times.entrySet()) {
         out.println(tagEntry.getKey() + ":");
         int maxLength = 0;
         for (String interceptor : tagEntry.getValue().keySet()) {
            maxLength = Math.max(maxLength, interceptor.length());
         }
         for (Map.Entry<String, AvgMinMax> interceptorEntry : tagEntry.getValue().entrySet()) {
            AvgMinMax avg = interceptorEntry.getValue();
            out.printf("\t%-" + (maxLength + 1) + "s avg %9.2f us in %9d records (%.2f - %.2f)\n",
                  interceptorEntry.getKey() + ":", avg.avg() / 1000, avg.count(), avg.min() / 1000d, avg.max() / 1000d);
         }
      }
   }

   private class TimestampAndInterceptor {
      public long timestamp;
      public String interceptor;

      private TimestampAndInterceptor(long timestamp, String interceptor) {
         this.timestamp = timestamp;
         this.interceptor = interceptor;
      }
   }
}
