package org.mft.processors;

import org.mft.objects.Event;
import org.mft.objects.Trace;

import java.util.function.Predicate;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Filters {

   public static Predicate<Trace> parse(String string) {
      int index = string.indexOf(':');
      String name = index < 0 ? string : string.substring(0, index);
      String param;
      long value;
      switch (name) {
         case "trace-tag":
            return new TraceTag(param(string, index));
         case "total-time":
            param = param(string, index);
            value = parseNonNegativeLong(param);
            return new TotalTime(parseOperator(index, param), value);
         case "messages":
            param = param(string, index);
            value = parseNonNegativeLong(param);
            return new Messages(parseOperator(index, param), value);

      }
      throw new IllegalArgumentException("Unknown filter " + name);
   }

   public static void printUsage() {
      System.err.println("\t\tFilter    \tArguments       \tDescription");
      System.err.println("\t\ttrace-tag \tTAG             \tTraces tagged with TAG.");
      System.err.println("\t\ttotal-time\t(>|>=|<|<=|==)MS\tTraces with wall-clock delta higher|... than MS milliseconds.");
      System.err.println("\t\tmessages  \t(>|>=|<|<=|==)N \tTraces with more|... than N messages.");
   }


   public static long parseNonNegativeLong(String param) {
      for (int i = 0; i < param.length(); ++i) {
         if (param.charAt(i) >= '0' && param.charAt(i) <= '9') {
            return Long.parseLong(param.substring(i));

         }
      }
      throw new IllegalArgumentException("No value in " + param);
   }

   public static LongBiPredicate parseOperator(int index, String param) {
      if (param.startsWith(">=") || param.startsWith("ge")) {
         return (l1, l2) -> l1 >= l2;
      } else if (param.startsWith(">") || param.startsWith("gt")){
         return (l1, l2) -> l1 > l2;
      } else if (param.startsWith("<=") || param.startsWith("le")){
         return (l1, l2) -> l1 <= l2;
      } else if (param.startsWith("<") || param.startsWith("lt")) {
         return (l1, l2) -> l1 < l2;
      } else if (param.startsWith("==") || param.startsWith("eq")) {
         return (l1, l2) -> l1 == l2;
      } else {
         throw new IllegalArgumentException("Unknow operator " + param);
      }
   }

   private static String param(String string, int index) {
      if (index < 0) throw new IllegalArgumentException(string);
      String param = string.substring(index + 1).trim();
      if (param.isEmpty()) throw new IllegalArgumentException(string);
      return param;
   }

   private static class TraceTag implements Predicate<Trace> {
      private final String tag;

      public TraceTag(String tag) {
         this.tag = tag;
      }

      @Override
      public boolean test(Trace trace) {
         return trace.events.stream().anyMatch(e -> e.type == Event.Type.TRACE_TAG && tag.equals(e.payload));
      }
   }

   private static class TotalTime implements Predicate<Trace> {
      private final LongBiPredicate comparator;
      private final long milliseconds;

      private TotalTime(LongBiPredicate comparator, long milliseconds) {
         this.comparator = comparator;
         this.milliseconds = milliseconds;
      }

      @Override
      public boolean test(Trace trace) {
         long min = trace.events.stream().mapToLong(e -> e.timestamp.getTime()).min().orElse(0);
         long max = trace.events.stream().mapToLong(e -> e.timestamp.getTime()).max().orElse(0);
         return comparator.test(max - min, milliseconds);
      }
   }

   private static class Messages implements Predicate<Trace> {
      private final LongBiPredicate comparator;
      private final long messages;

      private Messages(LongBiPredicate comparator, long messages) {
         this.comparator = comparator;
         this.messages = messages;
      }

      @Override
      public boolean test(Trace trace) {
         return comparator.test(trace.messages.size(), messages);
      }
   }

   private interface LongBiPredicate {
      boolean test(long l1, long l2);
   }
}
