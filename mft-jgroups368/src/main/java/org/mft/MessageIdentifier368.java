package org.mft;

import org.mft.objects.MessageId;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.util.MessageBatch;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by rmacor on 3/10/15.
 */
public class MessageIdentifier368 {
    public static MessageId getMessageIdentifier(Message msg) {
       return (Identifier.Header) msg.getHeader(Identifier.PROTOCOL_ID);
    }

    public static List<MessageId> getDataIdentifiers(Runnable r) throws NoSuchFieldException, IllegalAccessException {
         Class<?> clazz = r.getClass();
         if (clazz.getSimpleName().equals("MyHandler")) {
             return identifyMyHandler(r, clazz);
         } else if (clazz.getSimpleName().equals("BatchHandler")) {
             return identifyBatchHandler(r, clazz);
         }
        return null;
    }

    public static List<MessageId> identifyMyHandler(Runnable r, Class<?> clazz) throws NoSuchFieldException, IllegalAccessException {
        Field msgField = clazz.getDeclaredField("msg");
        msgField.setAccessible(true);
        Message msg = (Message) msgField.get(r);
        return Collections.singletonList(getMessageIdentifier(msg));
    }

    public static List<MessageId> identifyBatchHandler(Runnable r, Class<?> clazz) throws NoSuchFieldException, IllegalAccessException {
        Field batchField = clazz.getDeclaredField("batch");
        batchField.setAccessible(true);
        MessageBatch batch = (MessageBatch) batchField.get(r);
        return getMessageIdentifiers(batch);
    }

    public static List<MessageId> getMessageIdentifiers(MessageBatch batch) {
        List<MessageId> list = new ArrayList<>(batch.size());
        for (Message m : batch.array()) {
            if (m != null) {
                list.add(getMessageIdentifier(m));
            }
        }
        return list;
    }

    //is using getMessageIdentifier
    public static MessageId getEventIdentifier(Object e) {
        //Should be type checked ?
        Event event = (Event) e;
        switch (event.getType()) {
            case Event.MSG:
                return getMessageIdentifier((Message) event.getArg());
            default:
                return null;
        }
    }
}
