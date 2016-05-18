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

package org.mft.objects;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Span that may multiplex events to its children.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class BatchSpan extends Span {

   private final Map<MessageId, Span> childrenMap;
   private final Map<Thread, Span> currentSpan = new HashMap<>(4);
   private Span suppressed;

   private BatchSpan() {
      childrenMap = new HashMap<>(4);
   }

   private BatchSpan(Span parent, int numChildren) {
      super(parent);
      childrenMap = new HashMap<>(numChildren);
   }

    /**
     * Create new batchspan and new child span for each message
     * @param parent
     * @param messageIds
     * @return
     */
   public static BatchSpan newChild(Span parent, List<MessageId> messageIds) {
      BatchSpan batchSpan = new BatchSpan(parent, messageIds.size());
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

   public synchronized void push(MessageId message) {
      Span child = getChild(message);
      currentSpan.put(Thread.currentThread(), child);
      child.addEvent(Event.Type.MSG_PROCESSING_START, message);
   }

   public synchronized void pop() {
      Span span = currentSpan.remove(Thread.currentThread());
      if (span != null) {
         span.addEvent(Event.Type.MSG_PROCESSING_END, null);
      } else {
         throw new IllegalStateException();
      }
   }

   public Span getSuppressed() {
      return suppressed;
   }

   @Override
   public synchronized Span getCurrent() {
      Span current = this.currentSpan.get(Thread.currentThread());
      if (current == null) {
         return this;
      } else {
         return current;
      }
   }

   @Override
   public void addOutcoming(MessageId identifier) {
      Span current = this.currentSpan.get(Thread.currentThread());
      if (current == null) {
         super.addOutcoming(identifier);
      } else {
         current.addOutcoming(identifier);;
      }
   }

   @Override
   public void addEvent(Event.Type type, Object payload) {
      Span current = this.currentSpan.get(Thread.currentThread());
      if (current == null) {
         super.addEvent(type, payload);;
      } else {
         current.addEvent(type, payload);;
      }
   }

   @Override
   public void setNonCausal() {
      Span current = this.currentSpan.get(Thread.currentThread());
      if (current == null) {
         super.setNonCausal();
      } else {
         current.setNonCausal();
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
