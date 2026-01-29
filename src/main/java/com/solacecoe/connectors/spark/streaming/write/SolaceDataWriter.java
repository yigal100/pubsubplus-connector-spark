package com.solacecoe.connectors.spark.streaming.write;

import com.google.gson.Gson;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkSchemaProperties;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolacePublishAbortException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolacePublishAckInterruptedException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolacePublishException;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceAbortMessage;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolacePublishStatus;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;

public class SolaceDataWriter implements DataWriter<InternalRow>, Serializable {
    private static final Logger log = LoggerFactory.getLogger(SolaceDataWriter.class);
    private String topic;
    private String messageId;
    private final StructType schema;
    private final Map<String, String> properties;
    private SolaceBroker solaceBroker;
    private final transient UnsafeProjection projection;
    private final Map<String, SolaceDataWriterCommitMessage> commitMessages;
    private final Map<String, SolaceAbortMessage> abortedMessages;
    private Exception exception;
    private final boolean includeHeaders;
    private final boolean hasDefaultTopic;
    private final boolean hasDefaultMessageId;
    private int publishedMessages = 0;
    public SolaceDataWriter(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
        this.includeHeaders = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.INCLUDE_HEADERS, SolaceSparkStreamingProperties.INCLUDE_HEADERS_DEFAULT));
        this.topic = properties.getOrDefault(SolaceSparkStreamingProperties.TOPIC, null);
        this.messageId = properties.getOrDefault(SolaceSparkStreamingProperties.MESSAGE_ID, null);
        hasDefaultTopic = this.topic != null;
        hasDefaultMessageId = this.messageId != null;
        try {
            this.solaceBroker = new SolaceBroker(properties, "producer");
            this.solaceBroker.initProducer(getJCSMPStreamingPublishCorrelatingEventHandler());
        } catch (Exception e) {
            if(this.solaceBroker != null) {
                this.solaceBroker.close();
            }
            throw new SolacePublishException(e.getCause());
        }

        this.projection = createProjection();
        this.commitMessages = new HashMap<>();
        this.abortedMessages = new HashMap<>();
    }

    private void publishMessages(UnsafeRow projectedRow) {
        if(!hasDefaultTopic) {
            this.topic = projectedRow.getUTF8String(3).toString();
        }

        if(!hasDefaultMessageId) {
            this.messageId = projectedRow.getUTF8String(0).toString();
        }
        byte[] payload;
        if(projectedRow.getBinary(1) != null) {
            payload = projectedRow.getBinary(1);
        } else {
            throw new SolacePublishException("SolaceSparkConnector - Payload Column is not present in data frame.");
        }
        long timestamp = 0L;
        if(projectedRow.get(4, DataTypes.TimestampType) != null) {
            // TimestampType always returns long. So safe to use getLong
            timestamp = projectedRow.getLong(4);
        }
        UnsafeMapData headersMap = new UnsafeMapData();
        if(projectedRow.numFields() > 5 && projectedRow.getMap(5) != null) {
            headersMap = projectedRow.getMap(5);
        }
        String partitionKey = "";
        if(projectedRow.getUTF8String(2) != null) {
            partitionKey = projectedRow.getUTF8String(2).toString();
        }
        try {
            this.solaceBroker.publishMessage(this.messageId, this.topic,
                    partitionKey, payload, timestamp, headersMap);
            publishedMessages++;
        } catch (Exception e) {
            this.solaceBroker.close();
            throw new SolacePublishException(e.getCause());
        }
    }

    @Override
    public void write(InternalRow row) throws IOException {
        try {
            checkForException();
            UnsafeRow projectedRow = this.projection.apply(row);
            publishMessages(projectedRow);
            checkForException();
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String sStackTrace = sw.toString();
            SolaceAbortMessage abortMessage = new SolaceAbortMessage(SolacePublishStatus.FAILED, sStackTrace);
            abortedMessages.put(this.messageId != null ? this.messageId : row.getUTF8String(0).toString(), abortMessage);
            exception = e;
            Gson gson = new Gson();
            String exMessage = gson.toJson(abortedMessages, Map.class);
            abortedMessages.clear();
            throw new SolacePublishException(exMessage);
        }
    }

    @Override
    public WriterCommitMessage commit() {
        checkForException();
        if(this.commitMessages.size() < publishedMessages) {
            try {
                log.info("SolaceSparkConnector - Expected acknowledgements {}, Actual acknowledgements {}", this.properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT), this.commitMessages.size());
                log.info("SolaceSparkConnector - Sleeping for 3000ms to check for pending acknowledgments");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SolacePublishAckInterruptedException("SolaceSparkConnector - Interrupted while waiting for pending acknowledgments", e);
            }
        }
        checkForException();
        return new SolaceDataWriterCommitMessage(SolacePublishStatus.SUCCESS, "");
    }

    @Override
    public void abort() {
        log.error("SolaceSparkConnector - Publishing to Solace aborted", exception);
        Gson gson = new Gson();
        String exMessage = gson.toJson(abortedMessages, Map.class);
        throw new SolacePublishAbortException(exMessage);
    }

    @Override
    public void close() {
        log.info("SolaceSparkConnector - SolaceDataWriter Closed");
        commitMessages.clear();
        abortedMessages.clear();
        this.solaceBroker.close();
    }

    private UnsafeProjection createProjection() {
        List<Attribute> attributeList = new ArrayList<>();
        this.schema.foreach(field -> attributeList.add(DataTypeUtils.toAttribute(field)));
        Seq<Attribute> attributes = JavaConverters.asScalaIteratorConverter(attributeList.iterator()).asScala().toSeq();

        return UnsafeProjection.create(JavaConverters.asScalaIteratorConverter(Arrays.stream(getExpressions(attributes)).iterator()).asScala().toSeq(),
                JavaConverters.asScalaIteratorConverter(attributeList.iterator()).asScala().toSeq()
        );
    }

    private Expression[] getExpressions(Seq<Attribute> attributes) {

        Expression headerExpression = new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.headers().name(), SolaceSparkSchemaProperties.headers().dataType(), null, false).getExpression();
        if(!this.includeHeaders) {
            return new Expression[] {
                    // DataTypeUtils.toAttribute(new StructField("Id", DataTypes.StringType, true, Metadata.empty()))
                    new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.id().name(), SolaceSparkSchemaProperties.id().dataType(), null, (this.messageId == null)).getExpression(),
                    new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.payload().name(), SolaceSparkSchemaProperties.payload().dataType(), null, false).getExpression(),
                    new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.partitionKey().name(), SolaceSparkSchemaProperties.partitionKey().dataType(), null, false).getExpression(),
                    new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.topic().name(), SolaceSparkSchemaProperties.topic().dataType(), null, (this.topic == null)).getExpression(),
                    new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.timestamp().name(), SolaceSparkSchemaProperties.timestamp().dataType(), null, false).getExpression(),
            };
        }
        return new Expression[] {
                // DataTypeUtils.toAttribute(new StructField("Id", DataTypes.StringType, true, Metadata.empty()))
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.id().name(), SolaceSparkSchemaProperties.id().dataType(), null, (this.messageId == null)).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.payload().name(), SolaceSparkSchemaProperties.payload().dataType(), null, false).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.partitionKey().name(), SolaceSparkSchemaProperties.partitionKey().dataType(), null, false).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.topic().name(), SolaceSparkSchemaProperties.topic().dataType(), null, (this.topic == null)).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.timestamp().name(), SolaceSparkSchemaProperties.timestamp().dataType(), null, false).getExpression(),
                headerExpression
        };
    }

    private JCSMPStreamingPublishCorrelatingEventHandler getJCSMPStreamingPublishCorrelatingEventHandler() {
        return new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object o) {
                log.info("SolaceSparkConnector - Message published successfully to Solace");
                SolaceDataWriterCommitMessage solaceWriterCommitMessage = new SolaceDataWriterCommitMessage(SolacePublishStatus.SUCCESS, "");
                commitMessages.put(o.toString(), solaceWriterCommitMessage);
            }

            @Override
            public void handleErrorEx(Object o, JCSMPException e, long l) {
                log.error("SolaceSparkConnector - Exception when publishing message to Solace", e);
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                String sStackTrace = sw.toString();
                SolaceAbortMessage abortMessage = new SolaceAbortMessage(SolacePublishStatus.FAILED, sStackTrace);
                abortedMessages.put(o.toString(), abortMessage);
                exception = e;
            }
        };
    }

    private void checkForException() {
        if(exception != null) {
            Gson gson = new Gson();
            String exMessage = gson.toJson(abortedMessages, Map.class);
            abortedMessages.clear();
            throw new SolacePublishException(exMessage);
        }
    }
}
