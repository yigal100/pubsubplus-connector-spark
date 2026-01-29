package com.solacecoe.connectors.spark;

import com.github.dockerjava.api.model.Ulimit;
import com.solace.semp.v2.config.ApiException;
import com.solace.semp.v2.config.client.model.MsgVpnQueue;
import com.solace.semp.v2.config.client.model.MsgVpnQueueSubscription;
import com.solacecoe.connectors.spark.base.SempV2Api;
import com.solacecoe.connectors.spark.base.SolaceSession;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceConnectionManager;
import com.solacesystems.jcsmp.*;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import static org.apache.spark.sql.functions.*;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SolaceSparkStreamingSinkIT {
    private static final Long SHM_SIZE = (long) Math.pow(1024, 3);
    private SolaceContainer solaceContainer = new SolaceContainer("solace/solace-pubsub-standard:latest").withCreateContainerCmdModifier(cmd ->{
                Ulimit ulimit = new Ulimit("nofile", 2448, 1048576);
                List<Ulimit> ulimitList = new ArrayList<>();
                ulimitList.add(ulimit);
                cmd.getHostConfig()
                        .withShmSize(SHM_SIZE)
                        .withUlimits(ulimitList)
                        .withCpuCount(1l);
            }).withExposedPorts(8080, 55555).withTopic("solace/spark/streaming", Service.SMF)
            .withTopic("random/topic", Service.SMF).withTopic("Spark/Topic/0", Service.SMF)
            .withTopic("solace/spark/connector/offset", Service.SMF);
    private SparkSession sparkSession;
    @BeforeAll
    public void beforeAll() throws ApiException {
        solaceContainer.start();
        if(solaceContainer.isRunning()) {
            sparkSession = SparkSession.builder()
                    .appName("data_source_test")
                    .master("local[*]")
                    .getOrCreate();
//            sparkSession.sparkContext().setLogLevel("INFO");
            SempV2Api sempV2Api = new SempV2Api(String.format("http://%s:%d", solaceContainer.getHost(), solaceContainer.getMappedPort(8080)), "admin", "admin");
            MsgVpnQueue queue = new MsgVpnQueue();
            queue.queueName("Solace/Queue/0");
            queue.accessType(MsgVpnQueue.AccessTypeEnum.NON_EXCLUSIVE);
            queue.permission(MsgVpnQueue.PermissionEnum.DELETE);
            queue.ingressEnabled(true);
            queue.egressEnabled(true);

            MsgVpnQueueSubscription subscription = new MsgVpnQueueSubscription();
            subscription.setSubscriptionTopic("solace/spark/streaming");

            sempV2Api.config().createMsgVpnQueue("default", queue, null, null);
            sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/0", subscription, null, null);

            MsgVpnQueue emptyQueue = new MsgVpnQueue();
            emptyQueue.queueName("Solace/Queue/Empty");
            emptyQueue.accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE);
            emptyQueue.permission(MsgVpnQueue.PermissionEnum.DELETE);
            emptyQueue.ingressEnabled(true);
            emptyQueue.egressEnabled(true);

            sempV2Api.config().createMsgVpnQueue("default", emptyQueue, null, null);
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    @AfterAll
    public void afterAll() {
        solaceContainer.stop();
    }

    @BeforeEach
    public void beforeEach() throws JCSMPException {
        if(solaceContainer.isRunning()) {
            SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
            XMLMessageProducer messageProducer = session.getSession().getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
                @Override
                public void responseReceivedEx(Object o) {
                    // not required in test
                }

                @Override
                public void handleErrorEx(Object o, JCSMPException e, long l) {
                    // not required in test
                }
            });

            for (int i = 0; i < 100; i++) {
                TextMessage textMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                textMessage.setText("Hello Spark!");
                textMessage.setPriority(1);
                textMessage.setDMQEligible(true);
                SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
                sdtMap.putString("custom-string", "custom-value");
                sdtMap.putBoolean("custom-boolean", true);
                sdtMap.putString("null-custom-property", null);
                sdtMap.putString("empty-custom-property", "");
                sdtMap.putInteger("custom-sequence", i);
                textMessage.setProperties(sdtMap);
                Topic topic = JCSMPFactory.onlyInstance().createTopic("solace/spark/streaming");
                messageProducer.send(textMessage, topic);
            }

            messageProducer.close();
            session.getSession().closeSession();
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    @AfterEach
    public void afterEach() throws IOException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path path1 = Paths.get("src", "test", "resources", "spark-checkpoint-2");
        Path path2 = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        if(Files.exists(path)) {
            FileUtils.deleteDirectory(path.toAbsolutePath().toFile());
        }
        if(Files.exists(path1)) {
            FileUtils.deleteDirectory(path1.toAbsolutePath().toFile());
        }
        if(Files.exists(path2)) {
            FileUtils.deleteDirectory(path2.toAbsolutePath().toFile());
        }

        SolaceConnectionManager.closeAllConnections();
    }

    @Test
    @Order(1)
    void Should_ProcessData_And_Publish_As_Stream_To_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");

        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final String[] messageId = {""};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.MESSAGE_ID, "my-default-id")
                .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                .option("checkpointLocation", writePath.toAbsolutePath().toString())
                .format("solace").start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageId[0] = bytesXMLMessage.getApplicationMessageId();
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test
                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Assertions.assertEquals("my-default-id", messageId[0], "MessageId mismatch");
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(2)
    void Should_ProcessData_And_Publish_With_CustomId_To_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final String[] messageId = {""};
        Dataset<Row> dataset = reader.load();
        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            try {
                dataset1.drop("Topic", "PartitionKey", "TimeStamp", "Headers").write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .option(SolaceSparkStreamingProperties.MESSAGE_ID, "my-default-id")
                        .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                        .mode(SaveMode.Append)
                        .format("solace").save();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).option("checkpointLocation", writePath.toAbsolutePath().toString()).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageId[0] = bytesXMLMessage.getApplicationMessageId();
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test
                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Assertions.assertEquals("my-default-id", messageId[0], "MessageId mismatch");
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(3)
    void Should_ProcessData_And_Publish_With_DataFrameId_To_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(4)
    void Should_ProcessData_And_Publish_To_CustomTopic_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
              dataset1.write()
                      .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                      .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                      .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                      .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                      .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                      .option(SolaceSparkStreamingProperties.TOPIC, "Spark/Topic/0")
                      .mode(SaveMode.Append)
                      .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("Spark/Topic/0");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(5)
    void Should_ProcessData_And_Publish_With_Headers_To_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final int[] messageHeader = {4};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                    .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageHeader[0] = bytesXMLMessage.getPriority();
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Assertions.assertEquals(1, messageHeader[0], "Message Priority mismatch");
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(6)
    void Should_ProcessData_And_Publish_Without_Headers_To_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final int[] messageHeader = {4};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageHeader[0] = bytesXMLMessage.getPriority();
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Assertions.assertEquals(4, messageHeader[0], "Message Priority mismatch");
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(7)
    void Should_ProcessData_And_Publish_With_Only_PayloadColumn_To_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1 = dataset1.drop("TimeStamp", "PartitionKey", "Headers");
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(8)
    void Should_ProcessData_And_GetDataFrameCount_And_Publish_To_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        final long[] dataFrameCount = {0};
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataFrameCount[0] = dataFrameCount[0] + dataset1.count();
            dataset1 = dataset1.drop("TimeStamp", "PartitionKey", "Headers");
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, dataFrameCount[0]));
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(9)
    void Should_ProcessData_And_Publish_To_Solace_And_NewBatch_ShouldNotBeTriggered() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        final long[] batchTriggerCount = {0};
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            batchTriggerCount[0]++;
            dataset1 = dataset1.drop("TimeStamp", "PartitionKey", "Headers");
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(2L, batchTriggerCount[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(10)
    void Should_ProcessData_WithSingleConsumer_And_Publish_To_Solace_With_MultipleOperations_On_Dataframe() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        final long[] dataFrameCount = {0};
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            if(!dataset1.isEmpty()) {
                dataFrameCount[0] = dataFrameCount[0] + dataset1.count();
                dataset1 = dataset1.drop("TimeStamp", "PartitionKey", "Headers");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, dataFrameCount[0]));
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(12)
    void Should_ProcessData_WithMultipleConsumer_And_Publish_To_Solace_With_MultipleOperations_On_Dataframe() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        final long[] dataFrameCount = {0};
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option(SolaceSparkStreamingProperties.PARTITIONS, "3")
                .option(SolaceSparkStreamingProperties.QUEUE_RECEIVE_WAIT_TIMEOUT, 1000)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            if(!dataset1.isEmpty()) {
                dataFrameCount[0] = dataFrameCount[0] + dataset1.count();
                dataset1 = dataset1.drop("TimeStamp", "PartitionKey", "Headers");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, dataFrameCount[0]));
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(16)
    void Should_Not_ProcessData_When_QueueIsEmpty() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        final long[] batchTriggerCount = {0};
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/Empty")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            batchTriggerCount[0] = batchTriggerCount[0] + 1;
            System.out.println("Batch Id: " + batchId + " and count is " + batchTriggerCount[0]);
            dataset1 = dataset1.drop("TimeStamp", "PartitionKey", "Headers");
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(1L, batchTriggerCount[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(13)
    void Should_ProcessData_Publish_MicrosAs_SenderTimeStamp_To_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();
        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(14)
    void Should_ProcessData_And_Publish_MillisAs_SenderTimeStamp_To_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();
        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1.withColumn("TimeStamp", unix_millis(current_timestamp()));
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(15)
    void Should_ProcessData_And_Publish_SecondsAs_SenderTimeStamp_To_Solace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();
        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1.withColumn( "TimeStamp", unix_seconds(current_timestamp()));
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    void Should_Fail_With_Publish_Exception_To_Solace() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .option(SolaceSparkStreamingProperties.TOPIC, "publish/deny")
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfMessageIdIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Id");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfMessageTopicIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Topic");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    @Order(11)
    void Should_Fail_Publish_IfMessagePayloadIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfSolaceHostIsInvalid() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, "tcp://invalid-host:55555")
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfMandatoryHostIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
//                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfMandatoryHostIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, "")
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfMandatoryVpnIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
//                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfMandatoryVpnIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, "")
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfMandatoryUsernameIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
//                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfMandatoryUsernameIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, "")
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfMandatoryPasswordIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
//                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_IfMandatoryPasswordIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, "")
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, 0)
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_Stream_IfSolaceHostIsInvalid() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream()
                        .option(SolaceSparkStreamingProperties.HOST, "tcp://invalid-host:55555")
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option("checkpointLocation", writePath.toAbsolutePath().toString())
                        .format("solace").start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryHostIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream()
//                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryHostIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream()
                    .option(SolaceSparkStreamingProperties.HOST, "")
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryVpnIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
//                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryVpnIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, "")
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryUsernameIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
//                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryUsernameIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, "")
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryPasswordIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
//                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryPasswordIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, "")
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
       } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }
}
