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

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.mft.objects.Event;
import org.mft.objects.Header;
import org.mft.objects.MessageId;
import org.mft.objects.Trace;

/**
* @author Radim Vansa &lt;rvansa@redhat.com&gt;
*/
public class PrintTrace implements Processor {
   private static int DEFAULT_BUFFER_SIZE = 1024 * 1024;
   private SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
   private PrintStream out = System.out;
   private long outLine = 0;

   public PrintTrace(String outputFile) {
      if (outputFile.trim().equals("-")) return;
      try {
         out = new PrintStream(new BufferedOutputStream(new FileOutputStream(outputFile), DEFAULT_BUFFER_SIZE));
      } catch (FileNotFoundException e) {
         System.err.println("Could not write to " + outputFile + " due to " + e);
      }
   }

   public PrintTrace() {}

   @Override
   public void finish() {
      if (out != System.out) {
         out.close();
      }
   }

   public void process(Trace trace, long traceCounter) {
      outLine++;
   //suspiciously long traces
      if (trace.events.size() > 500) {
         System.err.printf("Long trace %d (%d events, %d messages) on line %d\n",
                           traceCounter, trace.events.size(), trace.messages.size(), outLine);
      }
      out.printf("TRACE %d: %d msg\n", traceCounter, trace.messages.size());
      for (MessageId message : trace.messages) {
         String src = null;
         ArrayList<String> dest = new ArrayList<String>();
         for (Event e : trace.events) {
            if (e.payload == null || !e.payload.equals(message)) continue;
            if (e.type == Event.Type.OUTCOMING_DATA_STARTED) src = e.source;
            else if (e.type == Event.Type.MSG_PROCESSING_START) dest.add(e.source);
         }
         out.printf("%s\t-> ", src == null ? "-unknown-" : src);
         if (dest.isEmpty()) {
            out.printf("-nobody-:\t%s\n", message);
         } else {
            for (int i = 0; i < dest.size() - 1; ++i) {
               out.print(dest.get(i));
               out.print(", ");
            }
            out.printf("%s:\t%s\n", dest.get(dest.size() - 1), message);
         }
         outLine++;
      }
      if (trace.negativeCycles > 0) {
         out.printf("%d causal inconsistencies detected\n", trace.negativeCycles);
         outLine++;
      }

      int longestThreadName = 0;
      long highestGlobalDelta = 0;
      Event prevEvent = null;
      Set<String> participants = new TreeSet<String>();
      Map<String, Event> lastSpanEvents = new HashMap<String, Event>();
      for (Event event : trace.events) {
         longestThreadName = Math.max(longestThreadName, event.threadName.length());
         String sourceThread = event.source + "|" + event.threadName;
         participants.add(sourceThread);
         lastSpanEvents.put(sourceThread + "|" + event.span, event);

         long globalDelta = prevEvent == null ? 0 : event.timestamp.getTime() - prevEvent.timestamp.getTime();
         // if global delta is < 0, we multiply it with -10 in order to account the minus sign
         highestGlobalDelta = Math.max(highestGlobalDelta, globalDelta > 0 ? globalDelta : -10 * globalDelta);
         prevEvent = event;
      }
      longestThreadName = Math.min(longestThreadName, 40);
      int globalDeltaWidth = highestGlobalDelta <= 0 ? 1 : (int) Math.log10(highestGlobalDelta) + 1;
      String globalDeltaFormatString = String.format("|%%%dd ms", globalDeltaWidth);

      prevEvent = null;
      Map<String, Long> localEvents = new HashMap<String, Long>();
      Map<String, Event> lastSourceThreadEvents = new HashMap<String, Event>();
      for (Event event : trace.events) {
         outLine++;
         out.print(format.format(event.timestamp));
         /* Global time delta */
         if (prevEvent != null) {
            out.printf(globalDeltaFormatString, event.timestamp.getTime() - prevEvent.timestamp.getTime());
         } else {
            out.print(truncateOrPad("|", globalDeltaWidth + 4));
         }
         prevEvent = event;
         /* Local time delta */
         if (event.nanoTime == Long.MIN_VALUE) {
            out.print("|   ?   |");
         } else {
            Long prevLocalEventNanoTime = localEvents.get(event.source);
            if (prevLocalEventNanoTime != null) {
               out.print(formatNanos(event.nanoTime - prevLocalEventNanoTime));
            } else {
               out.print("|       |");
            }
            localEvents.put(event.source, event.nanoTime);
         }
         /* Control flow graph */
         // when the graph grows too wide, it doesn't help
         if (participants.size() < 40) {
            String sourceThread = event.source + "|" + event.threadName;
            for (String st : participants) {
               if (st.equals(sourceThread)) {
                  out.print('*');
               } else {
                  Event lastSourceThreadEvent = lastSourceThreadEvents.get(st);
                  if (lastSourceThreadEvent == null || lastSourceThreadEvent == lastSpanEvents.get(st + "|" + lastSourceThreadEvent.span)) {
                     out.print(' ');
                  } else {
                     out.print('.');
                  }
               }
            }
            lastSourceThreadEvents.put(sourceThread, event);
            out.print('|');
         }
         /* Data */
         out.print(event.source);
         out.print('|');
         out.print(truncateOrPad(event.threadName, longestThreadName));
         out.print('|');
         out.print(event.type);
         if (event.payload != null) {
            if (event.payload instanceof StackTraceElement[]) {
               out.println();
               StackTraceElement[] stackTrace = (StackTraceElement[]) event.payload;
               for (int i = 0; i < stackTrace.length; ++i) {
                  out.print("\t\t");
                  out.println(stackTrace[i]);
               }
            } else {
               out.print(' ');
               out.println(event.payload);
            }
         } else {
            out.println();
         }
      }
      out.println();
      outLine++;
   }

   @Override
   public void processHeader(Header header) {
   }

   private String formatNanos(long nanos) {
      if (nanos < 99_500_000) {
         if (nanos < 99_500) {
            if (nanos < 10_000) {
               return String.format("|%4d ns|", nanos);
            } else {
               return String.format("|%2.1f us|", ((double) nanos) / 1000d);
            }
         } else {
            if (nanos < 10_000_000) {
               return String.format("|%4d us|", nanos / 1000);
            } else {
               return String.format("|%2.1f ms|", ((double) nanos) / 1000_000d);
            }
         }
      } else {
         if (nanos < 10_000_000_000l) {
            return String.format("|%4d ms|", nanos / 1000000);
         } else if (nanos < 99_500_000_000l) {
            return String.format("|%2.1f  s|", ((double) nanos) / 1000_000_000d);
         } else {
            return String.format("|%4d  s|", nanos / 1000_000_000l);
         }
      }
   }

   private String truncateOrPad(String str, int n) {
      StringBuilder sb = new StringBuilder(n);
      sb.append(str, 0, Math.min(n, str.length()));
      for (int i = str.length(); i < n; ++i) {
         sb.append(' ');
      }
      return sb.toString();
   }
}
