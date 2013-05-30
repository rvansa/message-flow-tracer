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

import java.lang.reflect.Field;
import java.util.concurrent.FutureTask;

/**
 * Helper class for dealing with executors
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Unwrapper {
   /**
    * For cases where we have submitted the callable (and we are executing callable, therefore we don't have access
    * to the wrapping Runnable upon success) but upon rejection we have to unwrap it.
    * @param rejected
    * @return
    */
   public static Object possiblyUnwrapCallable(Object rejected) {
      try {
         if (rejected.getClass() == FutureTask.class) {
            Field syncField = FutureTask.class.getField("sync");
            syncField.setAccessible(true);
            Object sync = syncField.get(rejected);
            Field callableField = sync.getClass().getField("callable");
            callableField.setAccessible(true);
            return callableField.get(sync);
         } else {
            return rejected;
         }
      } catch (Throwable e) {
         throw new RuntimeException(e);
      }
   }
}
