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

import org.jboss.qa.jdg.messageflow.logic.Composer;
import org.jboss.qa.jdg.messageflow.logic.Input;
import org.jboss.qa.jdg.messageflow.logic.InputFactory;
import org.jboss.qa.jdg.messageflow.logic.Logic;
import org.jboss.qa.jdg.messageflow.processors.AnalyseInterceptors;
import org.jboss.qa.jdg.messageflow.processors.AnalyseLocks;
import org.jboss.qa.jdg.messageflow.processors.AnalyseMessages;
import org.jboss.qa.jdg.messageflow.processors.AnalyseTraces;
import org.jboss.qa.jdg.messageflow.processors.CutProcessor;
import org.jboss.qa.jdg.messageflow.processors.PrintTrace;

/**
 * Entry point, args parsing etc...
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Main {

   public static void main(String[] args) {
      Composer composer = new Composer();
      Logic logic = composer;
      int i;
      for (i = 0; i < args.length; ++i) {
         if (args[i].equals("-r")) {
            composer.setReportMemoryUsage(true);
         } else if (args[i].equals("-p")) {
            if (i + 1 >= args.length) {
               printUsage();
               return;
            }
            composer.addProcessor(new PrintTrace(args[++i]));
         } else if (args[i].equals("-m")) {
            composer.addProcessor(new AnalyseMessages());
         } else if (args[i].equals("-l")) {
            composer.addProcessor(new AnalyseLocks());
         } else if (args[i].equals("-t")) {
            composer.addProcessor(new AnalyseTraces());
         } else if (args[i].equals("-i")) {
            composer.addProcessor(new AnalyseInterceptors());
         } else if (args[i].equals("-a")) {
            composer.addProcessor(new AnalyseMessages());
            composer.addProcessor(new AnalyseLocks());
            composer.addProcessor(new AnalyseTraces());
            composer.addProcessor(new AnalyseInterceptors());
         } else if (args[i].equals("-z")) {
            composer.setSortCausally(false);
         } else if (args[i].equals("-c")) {
            if (i + 2 >= args.length) {
               printUsage();
               return;
            }
            composer.addProcessor(new CutProcessor(args[++i], args[++i]));
         } else if (args[i].equals("-d")) {
            if (i + 1 > args.length) {
               printUsage();
               return;
            }
            composer.setMaxAdvanceMillis(Long.parseLong(args[++i]));
         } else if (args[i].startsWith("-")) {
            System.err.println("Unknown option " + args[i]);
            printUsage();
            return;
         } else {
            break;
         }
      }
      if (composer.getProcessors().isEmpty()) {
         composer.addProcessor(new PrintTrace());
      }
      for (; i < args.length; ++i) {
         for (Input input : InputFactory.create(args[i]))
         logic.addInput(input);
      }
      logic.run();
   }

   private static void printUsage() {
      System.err.println("Usage [-r] [([-m] [-l] [-t] | -a)] [-p trace_log] [-c dir message] span_logs...");
      System.err.println("\t-r             \tReport memory usage");
      System.err.println("\t-p trace_log   \tPrint log of traces");
      System.err.println("\t-z             \tThe ordering of events in trace log should be based only on timestamps (not causally)");
      System.err.println("\t-m             \tAnalyze messages");
      System.err.println("\t-l             \tAnalyze locks");
      System.err.println("\t-t             \tAnalyze traces");
      System.err.println("\t-i             \tAnalyze interceptors");
      System.err.println("\t-a             \tPrints log of traces and runs all available analyses");
      System.err.println("\t-c dir message \tWrite spans participating on trace with the message to the dir");
      System.err.println("\t-d milliseconds\tMaximum difference between highest processed timestamp in second-pass threads");
   }
}
