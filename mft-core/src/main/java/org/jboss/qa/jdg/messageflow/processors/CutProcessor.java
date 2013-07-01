/*
 * JBoss, Home of Professional Open isting of individual contributors.
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

package org.jboss.qa.jdg.messageflow.processors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.qa.jdg.messageflow.objects.Event;
import org.jboss.qa.jdg.messageflow.objects.Trace;

/**
 * Cuts a trace which contains specific message.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class CutProcessor implements Processor {
   private final String cutMessage;
   private final String directory;

   public CutProcessor(String directory, String message) {
      this.directory = directory;
      this.cutMessage = message;
   }

   @Override
   public void process(Trace trace, long traceCounter) {
      if (!trace.messages.contains(cutMessage)) return;
      Map<String, List<Event>> eventsBySpan = trace.getEventsBySpan();
      Map<String, BufferedWriter> writers = new HashMap<String, BufferedWriter>();
      try {
         for (List<Event> events : eventsBySpan.values()) {
            Event firstEvent = events.get(0);
            BufferedWriter writer = getWriterForEvent(firstEvent, writers);
            writer.write(Trace.SPAN);
            String incoming = "null";
            for (Event event : events) {
               if (event.type == Event.Type.MSG_PROCESSING_START) {
                  incoming = event.text;
                  break;
               }
            }
            writer.write(';');
            writer.write(incoming);
            for (Event event : events) {
               if (event.type == Event.Type.OUTCOMING_DATA_STARTED || event.type == Event.Type.RETRANSMISSION) {
                  writer.write(';');
                  writer.write(event.text);
               }
            }
            writer.write('\n');
            for (Event event : events) {
               writer.write(String.format("E;%d;%s;%s;", event.nanoTime, event.threadName, event.type));
               if (event.text != null) {
                  writer.write(event.text);
                  writer.write(";\n");
               } else writer.write('\n');
            }
         }
         for (BufferedWriter writer : writers.values()) {
            writer.close();
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   private BufferedWriter getWriterForEvent(Event event, Map<String, BufferedWriter> writers) throws IOException {
      BufferedWriter writer = writers.get(event.source);
      if (writer != null) return writer;
      writer = new BufferedWriter(new FileWriter(directory + File.separator + event.source + ".for." + cutMessage));
      writer.write(String.format("%d=%d\n", event.nanoTime, event.timestamp.getTime()));
      writers.put(event.source, writer);
      return writer;
   }

   @Override
   public void finish() {
   }
}
