********************************
*      MESSAGE FLOW TRACER     *
********************************

ATTACHING TO INFINISPAN
-----------------------

Add this to the command when running the JVM:

-javaagent:/home/rvansa/bin/byteman/byteman.jar=\
   boot:/path/to/byteman.jar,\
   sys:/path/to/message-flow-tracer.jar,\
   sys:/path/to/jgroups-(JGroups version).jar,\
   script:/path/to/MessageFlowTracer_jgroups-(JGroups version).btm,\
   script:/path/to/MessageFlowTracer_ispn-(Infinispan version).btm

By default the logs are written to /tmp/span.txt, you can change this by specifying
   
-Dorg.jboss.qa.messageflowtracer.output=/other/location/span.txt

(if you're using shared location, set this variable different for each instance)
   
The Byteman scripts are located in src/main/resources directory.
For details how to use Byteman see http://downloads.jboss.org/byteman/2.1.2/ProgrammersGuideSinglePage.html

Remarks:
* Currently running with message-flow-tracer enabled means about 2-3x worse performance (standard stress-test, distributed mode without transactions).
* Although it would certainly improve performance, you cannot use org.jboss.byteman.compileToBytecode option. This is due to Byteman bugs traced in BYTEMAN-235 and BYTEMAN-236 JIRAs.

MERGING SPANS INTO TRACES
-----------------------------------------------

Each instance creates log of events raised locally in this instance. Each set of such events that are raised causally (in the same or different thread) is called SPAN. These spans can be then merged into a TRACE - a logical unit serving one request (e.g. command), or operation started by a timer which results in one or more messages processed in multiple threads.
There are some threads that send messages which are not causally related (such as during multiple messages retransmission) - these are called non-causal.

In order to produce log of traces, run

java -jar message-flow-tracer.jar -p -o /path/to/output_log.txt /span/for/instance1.txt /span/for/instance2.txt ...

The traces are not sorted in the trace output log in any particular order, these are written as soon as no more spans should participate in the processing.
The merge process has two phases - in the first phase we record the number of occurrences for all messages, in the second phase the spans are actually merged. All message counts have to be stored in memory in one moment, therefore, the process may require a great amount of memory. The amount of memory required for second phase should be limited (does not depend on the overall amount of spans).

The output contains logs in this format:
MF (number of messages)
(message 1 source) -> (message 1 destinations): (message identifier)

These are the columns for events:
* wall time timestamp
* wall time delta from previous event
* local time delta (nanosecond precision) from previous event on the same node
* visualisation: each column of asterisks belongs to one thread on one node
* node name (or rather the filename of the log without extension)
* thread name
* event type
* event data

Message identifiers can be in on of these formats:
Unicast2 messages: src|dest|U(type):(seqno)
NakAck2 multicast messages: src|M(type):(seqno)
other service messages: src[|dest]|(protocol)[(type):](identification)
Messages that have no unique identifer (such as PING, FD_ALL) use wall time unix timestamp (in seconds) as identification. Therefore, it's possible that these are not properly matched.

Event types:
* Incoming - something was just received from the socket. The payload may carry one or more messages. However, each message will eventually create it's own span.
* THStarted - we have requested another thread to process some data
* THSuccess - another thread has started processing the data
* THFailure - the data were rejected and will not be processed
* TPComplete - current thread has finished processing its data
* Handling - the message processing has started
* Discard - the message will not be processed
* ODStarted - we started sending some message
* ODFinished - the message was written to the socket
* Checkpoint - user-defined stuff has been encountered
* Stackpoint - trace of the current stack (expensive operation, use for debugging)
* MsgTag - the business logic of the message was identifed
* TraceTag - the business logic of the trace was identified
* Retransmission - other node has retransmitted some message (non-causally)

The events are sorted according to:
1)  Causality: sending message always precedes receiving it
2a) On the same node, event with lower local timestamp always precedes event with higher timestamp
3a) On different nodes, events are sorted according to wall time
4)  In case of tie events are sorted according to their hashcode

MESSAGE STATISTICS
------------------

The message processing can be statistically processed. In order to categorize the messages these are marked with MsgTag. Identification of the message type is always executed only when the message is processed, therefore, if the message is lost or not paired properly (in the PING or FD case) the message accounts into non-tagged messages.

The processing is started with

java -jar message-flow-tracer.jar -m /span/for/instance1.txt /span/for/instance2.txt ...

Then a report is produced with these information:
* Incoming to handle time: specifies the delay between reading the message from the socket and the moment we start to process this message (which happens in another thread).
* Transport/Latency: the delay between sending the message (writing it to socket or passing into bundler) and reading from the socket on another node. As we cannot use wall time for measuring the delay, the latency is computed from nanotime timestamps on both nodes. The messages must be sent in both directions (we assume that the communication latency is symmetric). Let us denote Tx the timestamp on transmission on node x, Rx reception on node x. Then the latency for messages sent between nodes 1 and 2 is L = (average(R2 - T1) + average(R1 - T2))/2.
* Transport/Unique: amount of logical messages (those with different sequence numbers)
* Transport/Total: amount of messages going over network (unique + retransmissions, duplicates etc)
* Transport/Discarded: messages discarded when trying to pass to another thread
* Transport/Ignored: retransmissions ignored because this message was already processed
* Transport/Lost: Messages sent from one node but not received on other node (note: usually lost messages are not tagged, this is a rare case)
* Transport/Duplicate: Messages received multiple times from the network

The report contains total number of lost/transmission not detected messages.

TRACE STATISTICS
-----------------------

For each trace tagged with traceTag a statistics are produced. We write down the total amount of traces tagged with this traceTag and average, minimum and maximum amount of messages sent from a trace.
Then we write down average amount of messages of each tag(s) and average number of nodes participating in the trace.
The number of participating threads is reported as well, where one physical thread can participate multiple times if it processes multiple requests.
The processing time denotes sum of time for all threads that this request has blocked, not CPU time (the thread can sleep while blocked).

java -jar message-flow-tracer.jar -t /span/for/instance1.txt /span/for/instance2.txt ...

LOCKING STATISTICS
------------------

Here we can see how many lock attempts have been executed and how many succeeded/failed. We report the time spent waiting for the lock and time spent holding the lock.

java -jar message-flow-tracer.jar -l /span/for/instance1.txt /span/for/instance2.txt ...

INTERCEPTOR STATISTICS
----------------------

Shows how long took calling of each interceptor.

USAGE REFERENCE
--------------------
Usage  [-r] [([-m] [-l] [-t] | -a)] [-p trace_log] [-c dir message] span_logs...
-r                Report memory usage
-p file           Print log of traces to the file
-z                The ordering of events in trace log should be based only on timestamps (not causally)
-m                Analyze messages
-l                Analyze locks
-t                Analyze traces
-i                Analyze interceptors
-a                Prints log of traces and runs all available analyses
-c dir message    Write spans participating on trace with the message to the dir
-d milliseconds   Maximum difference between highest processed timestamp in second-pass threads

