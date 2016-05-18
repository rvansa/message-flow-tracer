package org.mft.persistence;

import org.mft.objects.Event;
import org.mft.objects.Header;
import org.mft.objects.Message;
import org.mft.objects.MessageId;
import org.mft.objects.Span;
import org.mft.objects.ThreadChange;

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
   private static final byte MESSAGE_ID = 2;
   private static final byte BATCH = 3;
   private static final byte IDENTITY_HASH_CODE = 4;
   private static final byte MESSAGE = 5;

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
      int eventCount = span.getEvents() != null ? span.getEvents().size() : 0;
      outputStream.writeShort(eventCount);
      for (Span.LocalEvent event : events){
         outputStream.writeLong(event.timestamp);
         outputStream.writeLong(event.threadId);
         outputStream.writeByte(event.type.ordinal());
         if (event.payload == null) {
            outputStream.writeByte(NULL);
         } else if (event.payload instanceof String) {
            outputStream.writeByte(TEXT);
            writeString((String) event.payload);
         } else if (event.payload instanceof MessageId) {
            MessageId msg = (MessageId) event.payload;
            outputStream.writeByte(MESSAGE_ID);
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
         } else if (event.payload instanceof Message) {
            Message msg = (Message) event.payload;
            outputStream.writeByte(MESSAGE);
            outputStream.writeShort(msg.id().from());
            outputStream.writeInt(msg.id().id());
            outputStream.writeInt(msg.identityHashCode());
         } else if (event.payload instanceof Integer) {
            outputStream.writeByte(IDENTITY_HASH_CODE);
            outputStream.writeInt((Integer) event.payload);
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
   public void read() throws IOException {
      try {
         for (;;) {
            byte flags = inputStream.readByte();
            if ((flags & 64) != 0) {
               threadChangeConsumer.accept(readThreadChange());
            } else {
               spanConsumer.accept(readSpan(flags));
            }
         }
      } catch (EOFException e) {}
   }

   private ThreadChange readThreadChange() throws IOException {
      return new ThreadChange(readString(), inputStream.readLong(), inputStream.readLong());
   }

   private Span readSpan(byte flags) throws IOException {
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
      int eventCount = inputStream.readShort();
      for (int i = 0; i < eventCount; ++i) {
         span.addEvent(new Span.LocalEvent(inputStream.readLong(), inputStream.readLong(),
            Event.Type.values()[inputStream.readByte()], readObject()));
      }
      return span;
   }

   private Object readObject() throws IOException {
      switch (inputStream.readByte()) {
         case NULL: return null;
         case TEXT: return readString();
         case MESSAGE_ID: return new MessageId.Impl(inputStream.readShort(), inputStream.readInt());
         case BATCH: {
            int num = inputStream.readShort();
            List<MessageId> batch = new ArrayList<>(num);
            for (int i = 0; i < num; ++i) {
               batch.add(new MessageId.Impl(inputStream.readShort(), inputStream.readInt()));
            }
            return batch;
         }
         case MESSAGE: return new Message(new MessageId.Impl(inputStream.readShort(), inputStream.readInt()), inputStream.readInt());
         case IDENTITY_HASH_CODE: return inputStream.readInt();
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

   @Override
   public void write(ThreadChange threadChange) throws IOException {
      outputStream.writeByte(64);
      writeString(threadChange.getThreadName());
      outputStream.writeLong(threadChange.getNanoTime());
      outputStream.writeLong(threadChange.getId());
   }
}
