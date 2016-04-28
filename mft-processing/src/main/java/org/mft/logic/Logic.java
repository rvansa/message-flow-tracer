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

package org.mft.logic;

import org.mft.persistence.BinaryPersister;
import org.mft.persistence.FlightRecording;
import org.mft.persistence.Input;
import org.mft.persistence.Persister;
import org.mft.persistence.TextPersister;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class Logic implements Runnable {

   protected List<Persister> logs = new ArrayList<>();
   protected List<FlightRecording> flightRecordings = new ArrayList<>();

   public void addInput(Input input) {
      byte[] magic = new byte[4];
      try {
         if (input.peek(magic) < magic.length) {
            throw new IllegalArgumentException("Cannot determine input type for " + input.name());
         }
      } catch (IOException e) {
         throw new IllegalArgumentException("Cannot determine input type for " + input.name(), e);
      }
      if (startsWith(magic, BinaryPersister.TAG)) {
         logs.add(new BinaryPersister(input));
      } else if (startsWith(magic, TextPersister.TAG)) {
         logs.add(new TextPersister(input));
      } else if (startsWith(magic, FlightRecording.TAG)) {
         flightRecordings.add(new FlightRecording(input));
      }
   }

   private boolean startsWith(byte[] array, byte[] prefix) {
      if (prefix.length > array.length) return false;
      for (int i = 0; i < prefix.length; ++i) {
         if (prefix[i] != array[i]) return false;
      }
      return true;
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
