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

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class AvgMinMax {
   private long sum = 0;
   private long count = 0;
   private long min = Long.MAX_VALUE;
   private long max = Long.MIN_VALUE;

   public void add(long value) {
      sum += value;
      count++;
      min = Math.min(value, min);
      max = Math.max(value, max);
   }

   public double avg() {
      return (double) sum / count;
   }

   public long min() {
      return min;
   }

   public long max() {
      return max;
   }

   public long count() {
      return count;
   }
}
