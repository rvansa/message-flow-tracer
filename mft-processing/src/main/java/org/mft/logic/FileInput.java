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

import org.mft.persistence.Input;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class FileInput implements Input {
   private static int DEFAULT_BUFFER_SIZE = 1024 * 1024;
   private String filename;

   public FileInput(String filename) {
      this.filename = filename;
   }

   @Override
   public int peek(byte[] bytes) throws IOException {
      try (FileInputStream stream = new FileInputStream(filename)) {
         return stream.read(bytes);
      }
   }

   @Override
   public InputStream stream() throws IOException {
      return new BufferedInputStream(new FileInputStream(filename), DEFAULT_BUFFER_SIZE);
   }

   @Override
   public String name() {
      return new File(filename).getName();
   }

   @Override
   public String toString() {
      return new File(filename).getAbsolutePath();
   }
}
