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

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class DagVertex implements Comparable<DagVertex> {
   private Set<DagVertex> up = new HashSet<DagVertex>();

   public DagVertex() {
   }

   public DagVertex(DagVertex previous) {
      up.add(previous);
   }

   public int compareTo(DagVertex o, boolean other) {
      if (this == o) return 0;
      Set<DagVertex> gen = new HashSet<DagVertex>(up);
      Set<DagVertex> nextGen = new HashSet<DagVertex>();
      while (nextGen.isEmpty()) {
         if (gen.contains(o)) return 1;
         for (DagVertex v : gen) {
            nextGen.addAll(v.up);
         }
         gen = nextGen;
      }
      if (other) {
         return 0;
      } else {
         return -o.compareTo(this, true);
      }
   }

   @Override
   public int compareTo(DagVertex o) {
      return compareTo(o, false);
   }

   public void addUp(DagVertex vertex) {
      up.add(vertex);
   }
}
