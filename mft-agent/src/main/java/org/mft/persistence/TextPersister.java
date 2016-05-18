package org.mft.persistence;

import org.mft.objects.Event;
import org.mft.objects.Header;
import org.mft.objects.Message;
import org.mft.objects.MessageId;
import org.mft.objects.Span;
import org.mft.objects.ThreadChange;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class TextPersister extends Persister {
   public static final byte[] TAG = new byte[] { 'M', 'F', 'T', 'T'};
   public static final String NON_CAUSAL = "NC";
   public static final String SPAN = "SPAN";
   public static final String EVENT = "E";
   private static final String THREAD = "THREAD";
   private BufferedReader reader;
   private PrintStream printStream = null;
   private String prefix = "";
   private int lineNumber;

   public TextPersister() {}

   public TextPersister(PrintStream printStream, String prefix) {
      this.printStream = printStream;
      this.prefix = prefix;
   }

   public TextPersister(Input input) {
      super(input);
   }

   @Override
   public void openForWrite(String path, Header header) throws IOException {
      close();
      printStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(path)));
      printStream.write(TAG);
      printStream.printf(";%d;%d\n", header.getNanoTime(), header.getUnixTime());
   }

   @Override
   public Header openForRead() throws IOException {
      close();
      reader = new BufferedReader(new InputStreamReader(input.stream()));
      String timeSync = reader.readLine();
      if (timeSync == null) {
         throw new IllegalArgumentException("Text spans log is empty!");
      }
      String[] parts = timeSync.split(";");
      if (parts.length != 3 || !new String(TAG).equals(parts[0])) {
         throw new IllegalArgumentException("Not a text span log");
      }
      lineNumber = 1;
      return new Header(Long.parseLong(parts[1]), Long.parseLong(parts[2]));
   }

   @Override
   public void write(Span span, boolean sort) {
      printStream.print(prefix);
      if (span.isNonCausal()) {
         printStream.print(NON_CAUSAL);
      } else {
         printStream.print(SPAN);
      }
      printStream.print(';');
      if (span.getIncoming() != null) {
         printStream.print(span.getIncoming());
      }
      if (span.getOutcoming() != null) {
         for (MessageId msg : span.getOutcoming()) {
            printStream.print(';');
            printStream.print(msg);
         }
      }
      printStream.println();
      Collection<Span.LocalEvent> events = sort ? span.getSortedEvents() : span.getEvents();
      for (Span.LocalEvent evt : events) {
         printStream.print(prefix);
         printStream.print(EVENT);
         printStream.print(';');
         printStream.print(evt.timestamp);
         printStream.print(';');
         printStream.print(evt.threadId);
         printStream.print(';');
         printStream.print(evt.type);
         printStream.print(';');
         if (evt.payload instanceof List) {
            for (Object item : (List) evt.payload) {
               printStream.print('B');
               printStream.print(item);
               printStream.print(',');
            }
         } else if (evt.payload instanceof MessageId) {
            printStream.print('M');
            printStream.print(evt.payload);
         } else if (evt.payload instanceof Message) {
            printStream.print('N');
            Message msg = (Message) evt.payload;
            printStream.print(msg.id());
            printStream.print(',');
            printStream.print(msg.identityHashCode());
         } else if (evt.payload != null){
            printStream.print('T');
            printStream.print(evt.payload);
         } else if (evt.payload instanceof Integer) {
            printStream.print('H');
            printStream.print(((Integer) evt.payload).intValue());
         }
         printStream.println();
      }
   }

   @Override
   public void close() throws IOException {
      if (reader != null) {
         reader.close();
         reader = null;
      }
      if (printStream != null) {
         printStream.close();
         printStream = null;
      }
   }

   @Override
   public void read() throws IOException {
      String line;
      Span span = null;
      while ((line = reader.readLine()) != null) {
         ++lineNumber;
         if (line.startsWith(EVENT)) {
            if (!loadEvents) continue;
            readEvent(line, span);
         } else {
            if (span != null) {
               spanConsumer.accept(span);
            }
            if (line.startsWith(THREAD)) {
               String[] parts = line.split(";");
               threadChangeConsumer.accept(new ThreadChange(parts[1], Long.parseLong(parts[2]), Long.parseLong(parts[3])));
            } else {
               span = new Span();
               int start;
               if (line.startsWith(SPAN)) {
                  start = SPAN.length() + 1;
               } else if (line.startsWith(NON_CAUSAL)) {
                  start = NON_CAUSAL.length() + 1;
               } else {
                  throw new IllegalArgumentException("Cannot parse " + line);
               }
               readSpan(line, span, start);
            }
         }
      }
   }

   public void readSpan(String line, Span span, int start) {
      int index = line.indexOf(';', start);
      if (index < 0) index = line.length();
      if (index > start) {
         span.setIncoming(parseMessageId(line, start, index));
      }
      start = index + 1;
      if (start < line.length()) {
         for (; ; ) {
            index = line.indexOf(';', start);
            if (index < 0) {
               span.addOutcoming(parseMessageId(line, start, line.length()));
               break;
            } else {
               span.addOutcoming(parseMessageId(line, start, index));
               start = index + 1;
            }
         }
      }
   }

   public void readEvent(String line, Span span) {
      int index = line.indexOf(';', 2);
      long timestamp = Long.parseLong(line.substring(2, index));
      int start = index + 1;
      index = line.indexOf(';', start);
      long threadId = Long.parseLong(line.substring(start, index));
      start = index + 1;
      index = line.indexOf(';', start);
      Event.Type type = Event.Type.get(line.substring(start, index));
      String text = line.substring(index + 1).trim();
      Object payload;
      if (text.isEmpty()) {
         payload = null;
      } else if (text.charAt(0) == 'B') {
         ArrayList<MessageId> messages = new ArrayList<>();
         start = 1;
         for (; ; ) {
            index = text.indexOf(',', start);
            if (index < 0) {
               messages.add(parseMessageId(text, start, text.length()));
               break;
            } else {
               messages.add(parseMessageId(text, start, index));
               start = index + 1;
            }
         }
         payload = messages;
      } else if (text.charAt(0) == 'M') {
         payload = parseMessageId(text, 1, text.length());
      } else if (text.charAt(0) == 'T') {
         payload = text.substring(1);
      } else if (text.charAt(0) == 'N') {
         index = text.indexOf(',', 1);
         payload = new Message(parseMessageId(text, 1, index), Integer.parseInt(text.substring(index + 1)));
      } else if (text.charAt(0) == 'H') {
         payload = Integer.parseInt(text.substring(1));
      } else {
         throw new IllegalArgumentException(text);
      }
      span.addEvent(new Span.LocalEvent(timestamp, threadId, type, payload));
   }

   public MessageId parseMessageId(String text, int fromIndex, int toIndex) {
      int colon = text.indexOf(':', fromIndex);
      return new MessageId.Impl(Short.parseShort(text.substring(fromIndex, colon)), Integer.parseInt(text.substring(colon + 1, toIndex)));
   }

   @Override
   public int getPosition() {
      return lineNumber;
   }

   @Override
   public void write(ThreadChange threadChange) throws IOException {
      printStream.print(THREAD);
      printStream.print(';');
      printStream.print(threadChange.getThreadName());
      printStream.print(';');
      printStream.print(threadChange.getNanoTime());
      printStream.print(';');
      printStream.print(threadChange.getId());
      printStream.println();
   }
}
