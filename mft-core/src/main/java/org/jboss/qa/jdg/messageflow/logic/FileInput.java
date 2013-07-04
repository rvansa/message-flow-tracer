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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class FileInput implements Input {
   private String filename;
   private BufferedReader reader;

   public FileInput(String filename) {
      this.filename = filename;
   }

   @Override
   public void open() throws IOException {
      if (reader != null) close();
      reader = new BufferedReader(new FileReader(filename));
   }

   @Override
   public String readLine() throws IOException {
      return reader.readLine();
   }

   @Override
   public String shortName() {
      return new File(filename).getName();
   }

   @Override
   public void close() throws IOException {
      try {
         reader.close();
      } finally {
         reader = null;
      }
   }

   @Override
   public String toString() {
      return new File(filename).getAbsolutePath();
   }
}
