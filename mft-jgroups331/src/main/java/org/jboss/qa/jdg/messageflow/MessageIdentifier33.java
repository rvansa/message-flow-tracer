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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jgroups.Message;
import org.jgroups.util.MessageBatch;

/**
* @author Radim Vansa &lt;rvansa@redhat.com&gt;
*/
public class MessageIdentifier33 extends org.jboss.qa.jdg.messageflow.MessageIdentifier32 {

   public static List<String> getDataIdentifiers(Runnable r) {
      try {
      Class<?> clazz = r.getClass();
      if (clazz.getSimpleName().equals("MyHandler")) {
         return identifyMyHandler(r, clazz);
      } else if (clazz.getSimpleName().equals("BatchHandler")) {
         return identifyBatchHandler(r, clazz);
      }
      } catch (IllegalAccessException e) {
         e.printStackTrace();
      } catch (NoSuchFieldException e) {
         e.printStackTrace();
      }
      return org.jboss.qa.jdg.messageflow.MessageIdentifier32.getDataIdentifiers(r);
   }

   public static List<String> identifyBatchHandler(Runnable r, Class<?> clazz) throws NoSuchFieldException, IllegalAccessException {
      Field batchField = clazz.getDeclaredField("batch");
      batchField.setAccessible(true);
      MessageBatch batch = (MessageBatch) batchField.get(r);
      return getMessageIdentifiers(batch);
   }

   public static List<String> identifyMyHandler(Runnable r, Class<?> clazz) throws NoSuchFieldException, IllegalAccessException {
      Field msgField = clazz.getDeclaredField("msg");
      msgField.setAccessible(true);
      Message msg = (Message) msgField.get(r);
      return Collections.singletonList(getMessageIdentifier(msg));
   }

   public static String getBatchIdentifier(MessageBatch batch) {
      StringBuilder sb = new StringBuilder();
      Message[] messages = batch.array();
      int i = 0;
      for (; i < messages.length; ++i) {
         if (messages[i] != null) {
            sb.append(getMessageIdentifier(messages[i]));
            ++i;
            break;
         }
      }
      for (; i < messages.length; ++i) {
         if (messages[i] != null) {
            sb.append("||");
            sb.append(getMessageIdentifier(messages[i]));
         }
      }
      return sb.toString();
   }

   public static List<String> getMessageIdentifiers(MessageBatch batch) {
      List<String> list = new ArrayList<String>(batch.size());
      for (Message m : batch.array()) {
         if (m != null) {
            list.add(getMessageIdentifier(m));
         }
      }
      return list;
   }
}
