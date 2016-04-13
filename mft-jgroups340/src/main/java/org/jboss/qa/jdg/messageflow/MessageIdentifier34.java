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

import org.jgroups.Message;
import org.jgroups.util.MessageBatch;

import java.util.List;

/**
* @author Radim Vansa &lt;rvansa@redhat.com&gt;
*/
public class MessageIdentifier34 extends MessageIdentifier33 {
   public static List<String> getDataIdentifiers(Runnable r) {
      return MessageIdentifier33.getDataIdentifiers(r);
   }

   public static List<String> identifyBatchHandler(Runnable r, Class<?> clazz) throws NoSuchFieldException, IllegalAccessException {
      return MessageIdentifier33.identifyBatchHandler(r, clazz);
   }

   public static List<String> identifyMyHandler(Runnable r, Class<?> clazz) throws NoSuchFieldException, IllegalAccessException {
      return MessageIdentifier33.identifyMyHandler(r, clazz);
   }

   public static String getBatchIdentifier(MessageBatch batch) {
      return MessageIdentifier33.getBatchIdentifier(batch);
   }

   public static List<String> getMessageIdentifiers(MessageBatch batch) {
      return MessageIdentifier33.getMessageIdentifiers(batch);
   }

   public static String getMessageObjectIdentifier(Object o) {
      return MessageIdentifier32.getMessageIdentifier((Message) o);
   }
}
