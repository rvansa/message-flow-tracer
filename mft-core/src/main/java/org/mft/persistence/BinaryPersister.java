package org.mft.persistence;

import org.mft.logic.Input;
import org.mft.objects.Event;
import org.mft.objects.Header;
import org.mft.objects.MessageId;
import org.mft.objects.Span;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class BinaryPersister extends Persister {
   public static final byte[] TAG = new byte[]{'M', 'F', 'T', 'B'};
   private static final byte NULL = 0;
   private static final byte TEXT = 1;
   private static final byte MESSAGE = 2;
   private static final byte BATCH = 3;

   private DataInputStream inputStream;
   private DataOutputStream outputStream = null;

   public BinaryPersister() {}

   public BinaryPersister(Input input) {
      super(input);
   }

   @Override
   public void openForWrite(String path, Header header) throws IOException {
      close();
      outputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));

      outputStream.write(TAG);
      outputStream.writeLong(header.getNanoTime());
      outputStream.writeLong(header.getUnixTime());
   }

   @Override
   public Header openForRead() throws IOException {
      close();
      this.inputStream = new DataInputStream(input.stream());
      byte[] fileTag = new byte[4];
      if (inputStream.read(fileTag) < 4 || !Arrays.equals(fileTag, TAG)) {
         throw new IllegalArgumentException("Not a binary span log");
      }
      return new Header(inputStream.readLong(), inputStream.readLong());
   }

   @Override
   public void write(Span span, boolean sort) throws IOException {
      byte flags = (byte) (span.isNonCausal() ? 1 : 0);
      flags |= (byte) (span.getIncoming() == null ? 2 : 0);
      outputStream.writeByte(flags);
      // TODO: we could instead just store forward references to the messages
      if (span.getIncoming() != null){
         outputStream.writeShort(span.getIncoming().from());
         outputStream.writeInt(span.getIncoming().id());
      }
      int outcomingCount = span.getOutcoming() != null ? span.getOutcoming().size() : 0;
      outputStream.writeShort(outcomingCount);
      if (span.getOutcoming() != null){
         for (MessageId message : span.getOutcoming()){
            outputStream.writeShort(message.from());
            outputStream.writeInt(message.id());
         }
      }
      Collection<Span.LocalEvent> events = sort ? span.getSortedEvents() : span.getEvents();
      Map<String, Integer> threadTable = new LinkedHashMap<>();
      for (Span.LocalEvent event : events) {
         if (!threadTable.containsKey(event.threadName))
            threadTable.put(event.threadName, threadTable.size());
      }
      outputStream.writeShort(threadTable.size());
      for (String threadName : threadTable.keySet()) {
         writeString(threadName);
      }
      int eventCount = span.getEvents() != null ? span.getEvents().size() : 0;
      outputStream.writeShort(eventCount);
      for (Span.LocalEvent event : events){
         outputStream.writeLong(event.timestamp);
         outputStream.writeShort(threadTable.get(event.threadName).shortValue());
         outputStream.writeByte(event.type.ordinal());
         if (event.payload == null) {
            outputStream.writeByte(NULL);
         } else if (event.payload instanceof String) {
            outputStream.writeByte(TEXT);
            writeString((String) event.payload);
         } else if (event.payload instanceof MessageId) {
            MessageId msg = (MessageId) event.payload;
            outputStream.writeByte(MESSAGE);
            outputStream.writeShort(msg.from());
            outputStream.writeInt(msg.id());
         } else if (event.payload instanceof List) {
            outputStream.writeByte(BATCH);
            List<MessageId> batch = (List<MessageId>) event.payload;
            outputStream.writeShort(batch.size());
            for (MessageId msg : batch) {
               outputStream.writeShort(msg.from());
               outputStream.writeInt(msg.id());
            }
         }
      }
   }

   public void writeString(String threadName) throws IOException {
      outputStream.writeShort(threadName.length());
      // ignore non-ascii part of chars
      for (int i = 0; i < threadName.length(); ++i) {
         outputStream.writeByte(threadName.charAt(i));
      }
   }

   @Override
   public void close() throws IOException {
      if (inputStream != null) {
         inputStream.close();
         inputStream = null;
      }
      if (outputStream != null) {
         outputStream.close();
         outputStream = null;
      }
   }

   @Override
   public void read(Consumer<Span> spanConsumer, boolean loadEvents) throws IOException {
      try {
         for (;;) {
            byte flags = inputStream.readByte();
            Span span = new Span();
            if ((flags & 1) != 0) {
               span.setNonCausal();
            }
            if ((flags & 2) == 0) {
               span.setIncoming(new MessageId.Impl(inputStream.readShort(), inputStream.readInt()));
            }
            int outcomingCount = inputStream.readShort();
            for (int i = 0; i < outcomingCount; ++i) {
               span.addOutcoming(new MessageId.Impl(inputStream.readShort(), inputStream.readInt()));
            }
            // TODO: don't ignore loadEvents
            int threadTableSize = inputStream.readShort();
            String[] threadNames = new String[threadTableSize];
            for (int i = 0; i < threadNames.length; ++i) {
               threadNames[i] = readString();
            }
            int eventCount = inputStream.readShort();
            for (int i = 0; i < eventCount; ++i) {
               span.addEvent(new Span.LocalEvent(inputStream.readLong(), threadNames[inputStream.readShort()],
                  Event.Type.values()[inputStream.readByte()], readObject()));
            }
            spanConsumer.accept(span);
         }
      } catch (EOFException e) {}
   }

   private Object readObject() throws IOException {
      switch (inputStream.readByte()) {
         case NULL: return null;
         case TEXT: return readString();
         case MESSAGE: return new MessageId.Impl(inputStream.readShort(), inputStream.readInt());
         case BATCH: {
            int num = inputStream.readShort();
            List<MessageId> batch = new ArrayList<>(num);
            for (int i = 0; i < num; ++i) {
               batch.add(new MessageId.Impl(inputStream.readShort(), inputStream.readInt()));
            }
            return batch;
         }
         default: throw new IllegalArgumentException();
      }
   }

   private String readString() throws IOException {
      int length = inputStream.readShort();
      char[] chars = new char[length];
      for (int i = 0; i < chars.length; ++i) {
         chars[i] = (char) inputStream.readByte();
      }
      return new String(chars);
   }

   @Override
   public int getPosition() {
      return 0;
   }
}
