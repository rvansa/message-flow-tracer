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

package org.jboss.qa.jdg.messageflow.logic;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class Logic implements Runnable {

   protected List<String> inputFiles = new ArrayList<String>();

   public void addInputFile(String file) {
      inputFiles.add(file);
   }

   protected boolean joinAll(Thread[] threads) {
      for (int i = 0; i < threads.length; ++i) {
         try {
            threads[i].join();
         } catch (InterruptedException e) {
            System.err.println("Interrupted!");
            return false;
         }
      }
      return true;
   }
}
