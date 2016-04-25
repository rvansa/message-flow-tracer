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

package org.jboss.qa.jdg.messageflow.objects;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Span that may multiplex events to its children.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class BatchSpan extends Span {

   private Map<MessageId, Span> childrenMap = new HashMap<>();
   private ThreadLocal<MessageId> currentMessage = new ThreadLocal<>();
   private Span suppressed;

   private BatchSpan() {}

   private BatchSpan(Span parent) {
      super(parent);
   }

    /**
     * Create new batchspan and new child span for each message
     * @param parent
     * @param messageIds
     * @return
     */
   public static BatchSpan newChild(Span parent, List<MessageId> messageIds) {
      BatchSpan batchSpan = new BatchSpan(parent);
      for (MessageId msg : messageIds) {
         Span child = new Span(batchSpan);
         child.addEvent(Event.Type.CONTAINS, msg);
         child.setIncoming(msg);
         batchSpan.childrenMap.put(msg, child);
      }
      return batchSpan;
   }

    /**
     * Create new batchspan and connect to suppressed span
     * @param suppressed
     * @return
     */
   public static BatchSpan newOrphan(Span suppressed) {
      BatchSpan batchSpan = new BatchSpan();
      batchSpan.suppressed = suppressed;
      return batchSpan;
   }

   public void push(MessageId message) {
      currentMessage.set(message);
      addEvent(Event.Type.MSG_PROCESSING_START, message);
   }

   public void pop() {
      addEvent(Event.Type.MSG_PROCESSING_END, null);
      currentMessage.remove();
   }

   public Span getSuppressed() {
      return suppressed;
   }

   @Override
   public Span getCurrent() {
      MessageId currentMessage = this.currentMessage.get();
      if (currentMessage == null) {
         return this;
      } else {
         return getChild(currentMessage);
      }
   }

   @Override
   public void addOutcoming(MessageId identifier) {
      MessageId currentMessage = this.currentMessage.get();
      if (currentMessage == null) {
         super.addOutcoming(identifier);
      } else {
         getChild(currentMessage).addOutcoming(identifier);
      }
   }

   @Override
   public void addEvent(Event.Type type, Object payload) {
      MessageId currentMessage = this.currentMessage.get();
      if (currentMessage == null) {
         super.addEvent(type, payload);
      } else {
         getChild(currentMessage).addEvent(type, payload);
      }
   }

   @Override
   public void setNonCausal() {
      MessageId currentMessage = this.currentMessage.get();
      if (currentMessage == null) {
         super.setNonCausal();
      } else {
         getChild(currentMessage).setNonCausal();
      }
   }

   private synchronized Span getChild(MessageId msg) {
      Span child = childrenMap.get(msg);
      if (child == null) {
         child = new Span(this);
         childrenMap.put(msg, child);
         child.setIncoming(msg);
      }
      return child;
   }
}
