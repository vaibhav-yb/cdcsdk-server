package com.yugabyte.cdcsdk.testing;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;

import io.debezium.testing.testcontainers.ConnectorConfiguration;

public class KafkaRestartIT extends CdcsdkTestBase {
  private static final String SINK_CONNECTOR_NAME = "jdbc-sink";

  private static ConnectorConfiguration sinkConfig;

  @BeforeAll
  public static void beforeAll() throws Exception {
    initializeContainers();

    kafkaContainer.start();
    kafkaConnectContainer.start();
    postgresContainer.start();
    Awaitility.await()
            .atMost(Duration.ofSeconds(20))
            .until(() -> postgresContainer.isRunning());

    initHelpers();
  }

  @AfterAll
  public static void afterAll() throws Exception {
    // Stop the running containers
    postgresContainer.stop();
    kafkaConnectContainer.stop();
    kafkaContainer.stop();
  }

}