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

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
* @author Radim Vansa &lt;rvansa@redhat.com&gt;
*/
class Trace implements Comparable<Trace> {
   static final String NON_CAUSAL = "NC";
   static final String SPAN = "SPAN";
   static final String EVENT = "E";

   public SortedSet<Event> events = new TreeSet<Event>();
   public Set<String> messages = new HashSet<String>();
   public volatile boolean retired = false;
   public int mergeCounter = 0;
   public Trace mergedInto = null;

   // this lock both against internal manipulation AND removing/replacing in messageFlows
   public Lock lock = new ReentrantLock();

   public void addMessage(String msg) {
      if (retired) throw new IllegalStateException();
      messages.add(msg);
   }

   public void addEvent(Event e) {
      if (retired) throw new IllegalStateException();
      events.add(e);
   }

   @Override
   public int compareTo(Trace o) {
      return events.first().compareTo(o.events.first());
   }
}
