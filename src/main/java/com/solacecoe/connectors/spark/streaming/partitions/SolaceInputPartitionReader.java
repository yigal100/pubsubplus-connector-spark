package com.solacecoe.connectors.spark.streaming.partitions;

import com.google.gson.Gson;
import com.solacecoe.connectors.spark.streaming.offset.SolaceMessageTracker;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import com.solacecoe.connectors.spark.streaming.properties.SolaceHeaders;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.*;
import com.solacecoe.connectors.spark.streaming.solace.EventListener;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceConsumerException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceMessageException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceSessionException;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.SDTException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.streaming.MicroBatchExecution;
import org.apache.spark.sql.execution.streaming.StreamExecution;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SolaceInputPartitionReader implements PartitionReader<InternalRow>, Serializable {
    private final transient Logger log = LogManager.getLogger(SolaceInputPartitionReader.class);
    private final boolean includeHeaders;
    private final SolaceInputPartition solaceInputPartition;
    private final Map<String, String> properties;
    private final int batchSize;
    private final long taskId;
    private final String uniqueId;
    private final String checkpointLocation;
    private final long receiveWaitTimeout;
    private final TaskContext taskContext;
    private final boolean closeReceiversOnPartitionClose;
    private final boolean isCommitTriggered;
    private final CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints;
    private SolaceMessage solaceMessage;
    private SolaceBroker solaceBroker;
    private int messages = 0;
    private Iterator<SolaceMessage> iterator;
    private boolean shouldTrackMessage = true;
    private boolean isPartitionQueue = false;
    public SolaceInputPartitionReader(SolaceInputPartition inputPartition, boolean includeHeaders, Map<String, String> properties,
                                      TaskContext taskContext, CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints, String checkpointLocation) {

        log.info("SolaceSparkConnector - Initializing Solace Input Partition reader with id {}", inputPartition.getId());

        this.solaceInputPartition = inputPartition;
        this.uniqueId = this.solaceInputPartition.getId();
        this.includeHeaders = includeHeaders;
        this.properties = properties;
        this.taskContext = taskContext;
        this.taskId = taskContext.taskAttemptId();
        this.checkpoints = checkpoints;
        this.checkpointLocation = checkpointLocation;
        this.batchSize = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT));
        this.receiveWaitTimeout = Long.parseLong(properties.getOrDefault(SolaceSparkStreamingProperties.QUEUE_RECEIVE_WAIT_TIMEOUT, SolaceSparkStreamingProperties.QUEUE_RECEIVE_WAIT_TIMEOUT_DEFAULT));
        this.closeReceiversOnPartitionClose = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.CLOSE_RECEIVERS_ON_PARTITION_CLOSE, SolaceSparkStreamingProperties.CLOSE_RECEIVERS_ON_PARTITION_CLOSE_DEFAULT));
        boolean ackLastProcessedMessages = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.ACK_LAST_PROCESSED_MESSAGES, SolaceSparkStreamingProperties.ACK_LAST_PROCESSED_MESSAGES_DEFAULT));
        String replayStrategy = this.properties.getOrDefault(SolaceSparkStreamingProperties.REPLAY_STRATEGY, null);
        if (replayStrategy != null && !replayStrategy.isEmpty()) {
            ackLastProcessedMessages = false;
        }

        String currentBatchId = taskContext.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY());
        /*
         * In case when multiple operations are performed on dataframe, input partition will be called as part of Spark scan.
         * We need to acknowledge messages only if new batch is started. In case of same batch we will return the same messages.
         */
        if (!currentBatchId.equals(SolaceMessageTracker.getLastBatchId(this.uniqueId))) {
            /* Currently solace can ack messages on consumer flow. So ack previous messages before starting to process new ones.
             * If Spark starts new input partition it indicates previous batch of data is successful. So we can acknowledge messages here.
             * Solace connection is always active and acknowledgements should be successful. It might throw exception if connection is lost
             * */
            isCommitTriggered = true;
            log.info("SolaceSparkConnector - Acknowledging any processed messages to Solace as commit is successful");
            long startTime = System.currentTimeMillis();
            SolaceMessageTracker.ackMessages(uniqueId);
            log.info("SolaceSparkConnector - Total time taken to acknowledge messages {} ms", (System.currentTimeMillis() - startTime));
        } else {
            log.info("SolaceSparkConnector - Spark Batch with id {} is requesting data again. It may be because of multiple operations on same dataframe.", currentBatchId);
            isCommitTriggered = false;
            CopyOnWriteArrayList<SolaceMessage> messageList = SolaceMessageTracker.getMessages(uniqueId);
            if (messageList != null) {
                iterator = messageList.iterator();
                if(messageList.size() < batchSize) {
                    if(messageList.size() == 1) {
                        log.info("SolaceSparkConnector - Only {} message is available from earlier request. This is most likely due to the result of isEmpty or similar operation on dataframe. Spark will immediately terminate input partition if one message is available from source.", messageList.size());
                    }
                    log.info("SolaceSparkConnector - Since only {} messages are available from earlier request. The remaining {} messages will be consumed from queue, if available, to fill the configured batch size of {}", messageList.size(), (batchSize - messageList.size()), this.batchSize);
                } else {
                    log.info("SolaceSparkConnector - {} messages available from the earlier request, matching the configured batch size of {}. The same messages will be returned as batch is not yet completed.", this.batchSize, messageList.size());
                }

            }
        }

        SolaceMessageTracker.setLastBatchId(this.uniqueId, currentBatchId);



        log.info("SolaceSparkConnector - Checking for connection {}", inputPartition.getId());

        // Get existing connection if a new task is scheduled on executor or create a new one
        if (SolaceConnectionManager.getConnection(inputPartition.getId()) != null) {
            solaceBroker = SolaceConnectionManager.getConnection(inputPartition.getId());
            if (solaceBroker != null && !solaceBroker.isConnected()) {
                checkException();
                int unackedMessages = 0;
                CopyOnWriteArrayList<SolaceMessage> messageList = SolaceMessageTracker.getMessages(uniqueId);
                if(messageList != null) {
                    unackedMessages = messageList.size();
                }
                log.info("SolaceSparkConnector - Connection has been closed on input partition {}. {} messages will be redelivered as they are not acknowledged due to connection closure.", inputPartition.getId(), unackedMessages);
                SolaceConnectionManager.removeConnection(inputPartition.getId());
                createNewConnection(inputPartition.getId(), ackLastProcessedMessages);
                log.info("SolaceSparkConnector - Previous connection to Solace closed due to idle timeout. Established a new connection.");
            }
            if (closeReceiversOnPartitionClose) {
                createReceiver(inputPartition.getId(), ackLastProcessedMessages);
            }
        } else {
            createNewConnection(inputPartition.getId(), ackLastProcessedMessages);
        }
        checkException();
        log.info("SolaceSparkConnector - Current consumer session on input partition {} is {}", inputPartition.getId(), this.solaceBroker.getUniqueName());
        registerTaskListener();
    }

    @Override
    public boolean next() {
        checkException();

        if (TaskContext.get() != null && TaskContext.get().isInterrupted()) {
            log.info("SolaceSparkConnector - Interrupted while waiting for next message");
            SolaceConnectionManager.close(this.solaceInputPartition.getId());
            SolaceMessageTracker.resetId(uniqueId);
            throw new RuntimeException("Task was interrupted.");
        }
        solaceMessage = getNextMessage();
        return solaceMessage != null;
    }

    @Override
    public InternalRow get() {
        try {
            SolaceRecord solaceRecord = SolaceRecord.getMapper(this.properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT)).map(solaceMessage.bytesXMLMessage);
            long timestamp = solaceRecord.getSenderTimestamp();
            if (solaceRecord.getSenderTimestamp() == 0) {
                timestamp = System.currentTimeMillis();
            }
            InternalRow row;
            if (this.includeHeaders) {
                Map<String, Object> headers = getStringObjectMap(solaceRecord);
                MapData mapData = new ArrayBasedMapData(new GenericArrayData(headers.keySet().stream().filter(key -> headers.get(key) != null).map(UTF8String::fromString).toArray()), new GenericArrayData(headers.values().stream().filter(Objects::nonNull).map(value -> value.toString().getBytes(StandardCharsets.UTF_8)).toArray()));
                row = new GenericInternalRow(new Object[]{UTF8String.fromString(solaceRecord.getMessageId()),
                        solaceRecord.getPayload(), UTF8String.fromString(solaceRecord.getPartitionKey()), UTF8String.fromString(solaceRecord.getDestination()),
                        DateTimeUtils.fromJavaTimestamp(new Timestamp(timestamp)), mapData
                });
            } else {
                row = new GenericInternalRow(new Object[]{UTF8String.fromString(solaceRecord.getMessageId()),
                        solaceRecord.getPayload(), UTF8String.fromString(solaceRecord.getPartitionKey()), UTF8String.fromString(solaceRecord.getDestination()),
                        DateTimeUtils.fromJavaTimestamp(new Timestamp(timestamp))
                });
            }
            // No need to add message to tracker as the call is from same dataframe operation.
            if (shouldTrackMessage) {
                if (solaceRecord.getPartitionKey() != null && !solaceRecord.getPartitionKey().isEmpty() && isPartitionQueue) {
                    SolaceMessageTracker.addMessageID(solaceRecord.getPartitionKey(), solaceRecord.getMessageId());
                } else {
                    SolaceMessageTracker.addMessageID(this.uniqueId, solaceRecord.getMessageId());
                }
                SolaceMessageTracker.addMessage(this.uniqueId, solaceMessage);
            }
            solaceBroker.setLastMessageTimestamp(System.currentTimeMillis());
            return row;
        } catch (Exception e) {
            log.error("SolaceSparkConnector- Exception while reading message", e);
            throw new SolaceMessageException(e);
        }
    }

    private Map<String, Object> getStringObjectMap(SolaceRecord solaceRecord) {
        Map<String, Object> headers = new HashMap<>();
        Map<String, Object> userProperties = (solaceRecord.getProperties() != null) ? solaceRecord.getProperties() : new HashMap<>();
        if (!userProperties.isEmpty()) {
            headers.putAll(userProperties);
        }
        if (solaceRecord.getSequenceNumber() != null) {
            headers.put(SolaceHeaders.SEQUENCE_NUMBER, solaceRecord.getSequenceNumber());
        }
        headers.put(SolaceHeaders.EXPIRATION, solaceRecord.getExpiration());
        headers.put(SolaceHeaders.TIME_TO_LIVE, solaceRecord.getTimeToLive());
        headers.put(SolaceHeaders.PRIORITY, solaceRecord.getPriority());
        headers.put(SolaceHeaders.REDELIVERED, solaceRecord.isRedelivered());
        return headers;
    }

    private SolaceMessage getNextMessage() {
        /*
          If commit is triggered or messageList is null we need to fetch messages from Solace.
          In case of same batch just return the available messages in message tracker.
         */
        if (this.isCommitTriggered || iterator == null) {
            LinkedBlockingQueue<SolaceMessage> queue = solaceBroker.getMessages(0);
            if (queue != null) {
                while (shouldProcessMoreMessages(batchSize, messages)) {
                    try {
                        solaceMessage = queue.poll(receiveWaitTimeout, TimeUnit.MILLISECONDS);
                        if (solaceMessage == null) {
                            return null;
                        }

                        if (batchSize > 0) {
                            messages++;
                        }
                        if (isMessageAlreadyProcessed(solaceMessage)) {
                            log.info("Message is added to previous partitions for processing. Moving to next message");
                        } else {
                            return solaceMessage;
                        }

                    } catch (InterruptedException | SDTException e) {
                        log.warn("No messages available within specified receiveWaitTimeout", e);
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
            }
        } else {
            while (shouldProcessMoreMessages(batchSize, messages)) {
                try {
                    if (iterator.hasNext()) {
//                        shouldTrackMessage = false;
                        solaceMessage = iterator.next();
                        if (solaceMessage == null) {
                            return null;
                        }

                        if (batchSize > 0) {
                            messages++;
                        }
                        if (isMessageAlreadyProcessed(solaceMessage)) {
                            log.info("Message is added to previous partitions for processing. Moving to next message");
                        } else {
                            return solaceMessage;
                        }
                    } else {
                        LinkedBlockingQueue<SolaceMessage> queue = solaceBroker.getMessages(0);
                        if (queue != null) {
                            try {
                                solaceMessage = queue.poll(receiveWaitTimeout, TimeUnit.MILLISECONDS);
                                if (solaceMessage == null) {
                                    return null;
                                }

                                if (batchSize > 0) {
                                    messages++;
                                }
                                if (isMessageAlreadyProcessed(solaceMessage)) {
                                    log.info("Message is added to previous partitions for processing. Moving to next message");
                                } else {
                                    return solaceMessage;
                                }

                            } catch (InterruptedException | SDTException e) {
                                log.warn("No messages available within specified receiveWaitTimeout", e);
                                Thread.currentThread().interrupt();
                                return null;
                            }
                        }
                        return null;
                    }
                } catch (Exception e) {
                    log.warn("No messages available within specified receiveWaitTimeout", e);
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
    }

    private boolean isMessageAlreadyProcessed(SolaceMessage solaceMessage) throws SDTException {
        String messageId = SolaceUtils.getMessageID(
                solaceMessage.bytesXMLMessage,
                this.properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT)
        );
        return SolaceMessageTracker.containsMessageID(messageId);
    }

    private boolean shouldProcessMoreMessages(int batchSize, int messages) {
        return batchSize == 0 || (batchSize > 0 && messages < batchSize);
    }

    @Override
    public void close() {
        log.info("SolaceSparkConnector - Input partition reader with ID {} with task {} is closed", this.solaceInputPartition.getId(), this.uniqueId);
        checkException();
    }

    private void logShutdownMessage(TaskContext context) {
        log.info("SolaceSparkConnector - Closing connections to Solace as task {} is interrupted or failed", String.join(",", context.getLocalProperty(StreamExecution.QUERY_ID_KEY()),
                context.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY()),
                Integer.toString(context.stageId()),
                Integer.toString(context.partitionId())));
        SolaceConnectionManager.close(this.solaceInputPartition.getId());
        SolaceMessageTracker.resetId(uniqueId);
    }

    private void registerTaskListener() {
        this.taskContext.addTaskFailureListener((context, error) -> {
            log.error("SolaceSparkConnector - Input Partition {} failed with error", this.solaceInputPartition.getId(), error);
            logShutdownMessage(context);
        });
        this.taskContext.addTaskCompletionListener(context -> {
            log.info("SolaceSparkConnector - Task {} state is completed :: {}, failed :: {}, interrupted :: {}", uniqueId, context.isCompleted(), context.isFailed(), context.isInterrupted());
            if (context.isInterrupted() || context.isFailed()) {
                logShutdownMessage(context);
            } else if (context.isCompleted()) {
                String processedMessageIDs = SolaceMessageTracker.getProcessedMessagesIDs(this.solaceInputPartition.getId());
                Path path = Paths.get(this.checkpointLocation + "/" + this.solaceInputPartition.getId() + ".txt");
                log.info("SolaceSparkConnector - File path {} to store checkpoint processed in worker node {}", path.toString(), this.solaceInputPartition.getPreferredLocation());
                if(processedMessageIDs != null && !processedMessageIDs.isEmpty()) {
                    try {
                        Path parentDir = path.getParent();
                        if (parentDir != null) {
                            // Create the directory and all nonexistent parent directories
                            Files.createDirectories(parentDir);
                            log.info("SolaceSparkConnector - Created parent directory {} for file path {}", parentDir.toString(), path.toString());
                        }
                        // overwrite checkpoint to preserve latest value
                        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE,
                                StandardOpenOption.TRUNCATE_EXISTING)) {
//                        for (String id : ids) {
                            SolaceSparkPartitionCheckpoint solaceSparkPartitionCheckpoint = new SolaceSparkPartitionCheckpoint(processedMessageIDs, this.solaceInputPartition.getId());
                            CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> solaceSparkPartitionCheckpoints = new CopyOnWriteArrayList<>();
                            solaceSparkPartitionCheckpoints.add(solaceSparkPartitionCheckpoint);
                            // Publish state to checkpoint. On commit the state is published to Solace LVQ.
                            writer.write(new Gson().toJson(solaceSparkPartitionCheckpoints));
                            writer.newLine();
                            log.info("SolaceSparkConnector - Checkpoint {} stored in file path {}", new Gson().toJson(solaceSparkPartitionCheckpoints), path.toString());
                            SolaceMessageTracker.removeProcessedMessagesIDs(this.solaceInputPartition.getId());
                            //                        }
                        }
                    } catch (IOException e) {
                        log.error("SolaceSparkConnector - Exception when writing checkpoint to path {}", this.checkpointLocation, e);
                        this.solaceBroker.close();
                        throw new RuntimeException(e);
                    }
                } else {
                    log.info("SolaceSparkConnector - No processed message id's available for input partition {} and nothing is written to checkpoint {}", this.solaceInputPartition.getId(), this.checkpointLocation);
                }

                log.info("SolaceSparkConnector - Total time taken by executor is {} ms for Task {}", context.taskMetrics().executorRunTime(), uniqueId);

                if (closeReceiversOnPartitionClose) {
                    solaceBroker.closeReceivers();
                }
            }
        });
    }

    private void createNewConnection(String inputPartitionId, boolean ackLastProcessedMessages) {
        log.info("SolaceSparkConnector - Solace Connection Details Host : {}, VPN : {}, Username : {}", properties.get(SolaceSparkStreamingProperties.HOST), properties.get(SolaceSparkStreamingProperties.VPN), properties.get(SolaceSparkStreamingProperties.USERNAME));
        try {
            solaceBroker = new SolaceBroker(properties, "consumer");
            solaceBroker.initProducer();
            createReceiver(inputPartitionId, ackLastProcessedMessages);
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Exception Initializing Solace Broker", this.solaceBroker.getException() != null ? this.solaceBroker.getException() : e);
            solaceBroker.close();
            throw new SolaceConsumerException(e);
        }
    }

    private void createReceiver(String inputPartitionId, boolean ackLastProcessedMessages) {
        EventListener eventListener = new EventListener(inputPartitionId);
        if (ackLastProcessedMessages) {
            log.info("SolaceSparkConnector - Ack last processed messages is set to true, connector will match incoming messages with checkpoint and auto acknowledge");
//            List<String> messageIDs = Arrays.stream(this.lastKnownOffset.split(",")).collect(Collectors.toList());
            eventListener = new EventListener(inputPartitionId, this.checkpoints, this.properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT));
        }
        // Initialize connection to Solace Broker
        solaceBroker.addReceiver(eventListener);
        SolaceConnectionManager.addConnection(inputPartitionId, solaceBroker);
    }

    private void checkException() {
        if (this.solaceBroker != null && this.solaceBroker.isException()) {
            log.error("SolaceSparkConnector - Exception encountered, stopping input partition {}", this.solaceInputPartition.getId(), this.solaceBroker.getException());
            this.solaceBroker.close();
            throw new SolaceSessionException(this.solaceBroker.getException());
        }
    }
}
