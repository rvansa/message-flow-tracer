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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class InputFactory {
   public static Collection<Input> create(String location) {
      if (location.contains(":")) {
         String[] parts = location.split(":", 3);
         int port = -1;
         String filename = null;
         if (parts.length == 2) {
            filename = parts[1];
         } else if (parts.length == 3) {
            try {
               port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
               System.err.printf("Cannot parse port '%s'\n", parts[1]);
            }
            filename = parts[2];
         }
         String username = null;
         String hostpart;
         int atSignIndex = parts[0].indexOf('@');
         if (atSignIndex >= 0) {
            username = parts[0].substring(0, atSignIndex);
            hostpart = parts[0].substring(atSignIndex + 1);
         } else {
            hostpart = parts[0];
         }
         if (hostpart.matches("[\\w.-]*\\[\\d+-\\d+\\]")) {
            int openBracketIndex = hostpart.lastIndexOf('[');
            String prefix = hostpart.substring(0, openBracketIndex);
            String range = hostpart.substring(openBracketIndex + 1, hostpart.length() - 1);
            String[] rangeParts = range.split("-");
            Set<Input> inputs = new HashSet<Input>();
            for (String number : enumerate(rangeParts[0], rangeParts[1])) {
               inputs.add(new SFTPInput(username, prefix + number, port, filename));
            }
            return inputs;
         } else {
            return Collections.<Input>singleton(new SFTPInput(username, hostpart, port, filename));
         }
      } else {
         return Collections.<Input>singleton(new FileInput(location));
      }
   }

   private static List<String> enumerate(String from, String to) {
      List<String> numbers = new ArrayList<String>();
      StringBuilder current = new StringBuilder(from);
      final int index = current.length() - 1;
      while (true) {
         for (char c = current.charAt(index); c <= '9'; ++c) {
            current.setCharAt(index, c);
            numbers.add(current.toString());
            if (current.toString().equals(to)) {
               return numbers;
            }
         }
         current.setCharAt(index, '0');
         for (int i = index - 1; i >= 0; --i) {
            char higher = current.charAt(i);
            if (higher == '9') {
               if (i == 0) {
                  return numbers;
               }
               current.setCharAt(i, '0');
            } else {
               current.setCharAt(i, (char)(higher + 1));
               break;
            }
         }
         numbers.add(current.toString());
         if (current.toString().equals(to)) {
            return numbers;
         }
      }
   }
}
