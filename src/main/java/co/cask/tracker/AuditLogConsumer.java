/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.tracker;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet;
import co.cask.cdap.kafka.flow.KafkaConfigurer;
import co.cask.cdap.kafka.flow.KafkaConsumerConfigurer;
import co.cask.tracker.config.AuditLogKafkaConfig;
import co.cask.tracker.config.TrackerAppConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.apache.twill.kafka.client.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Subscribes to Kafka messages published for the CDAP Platform that contains the Audit log records.
 */
public final class AuditLogConsumer extends Kafka08ConsumerFlowlet<ByteBuffer, String> {
  private static final Logger LOG = LoggerFactory.getLogger(AuditLogConsumer.class);
  private static final Gson GSON = new Gson();

  // TODO: Add a way to reset the offset
  private KeyValueTable offsetStore;

  private OutputEmitter<String> emitter;
  private AuditLogKafkaConfig auditLogKafkaConfig;

  private String offsetDatasetName;

  public AuditLogConsumer(AuditLogKafkaConfig auditLogKafkaConfig) {
    this.offsetDatasetName = auditLogKafkaConfig.getOffsetDataset();
    verifyConfig(auditLogKafkaConfig);
  }

  public AuditLogConsumer() {
    // no-op
  }

  @VisibleForTesting
  static void verifyConfig(AuditLogKafkaConfig auditLogKafkaConfig) {
    // Verify if the configuration is right
    if (Strings.isNullOrEmpty(auditLogKafkaConfig.getBrokerString()) &&
            Strings.isNullOrEmpty(auditLogKafkaConfig.getZookeeperString())) {
      throw new IllegalArgumentException("Should provide either a broker string or a zookeeper string for " +
              "Kafka Audit Log subscription!");
    }

    if (Strings.isNullOrEmpty(auditLogKafkaConfig.getTopic())) {
      throw new IllegalArgumentException("Should provide a Kafka Topic for Kafka Audit Log subscription!");
    }

    if (auditLogKafkaConfig.getNumPartitions() <= 0) {
      throw new IllegalArgumentException("Kafka Partitions should be > 0.");
    }
  }

  @Override
  protected void configure() {
    createDataset(offsetDatasetName, KeyValueTable.class);
  }

  @Override
  protected KeyValueTable getOffsetStore() {
    return offsetStore;
  }

  @Override
  protected void configureKafka(KafkaConfigurer kafkaConfigurer) {
    TrackerAppConfig appConfig = GSON.fromJson(getContext().getApplicationSpecification().getConfiguration(),
            TrackerAppConfig.class);
    auditLogKafkaConfig = appConfig.getAuditLogKafkaConfig();
    LOG.debug("Configuring Audit Log Kafka Consumer : {}", auditLogKafkaConfig);
    offsetStore = getContext().getDataset(auditLogKafkaConfig.getOffsetDataset());
    if (!Strings.isNullOrEmpty(auditLogKafkaConfig.getZookeeperString())) {
      kafkaConfigurer.setZooKeeper(auditLogKafkaConfig.getZookeeperString());
    } else if (!Strings.isNullOrEmpty(auditLogKafkaConfig.getBrokerString())) {
      kafkaConfigurer.setBrokers(auditLogKafkaConfig.getBrokerString());
    }
    setupTopicPartitions(kafkaConfigurer);
  }

  @Override
  protected void handleInstancesChanged(KafkaConsumerConfigurer configurer) {
    setupTopicPartitions(configurer);
  }

  private void setupTopicPartitions(KafkaConsumerConfigurer configurer) {
    int partitions = auditLogKafkaConfig.getNumPartitions();
    int instanceId = getContext().getInstanceId();
    int instances = getContext().getInstanceCount();
    for (int i = 0; i < partitions; i++) {
      if ((i % instances) == instanceId) {
        configurer.addTopicPartition(auditLogKafkaConfig.getTopic(), i);
      }
    }
  }

  @Override
  protected void processMessage(String auditLogKafkaMessage) throws Exception {
    emitter.emit(auditLogKafkaMessage);
  }

  /**
   * Overriding the default Returns the key to be used when persisting offsets into a {@link KeyValueTable}.
   */
  @Override
  protected String getStoreKey(TopicPartition topicPartition) {
    return TrackerApp.APP_NAME + ":" + topicPartition.getTopic() + ":" + topicPartition.getPartition();
  }
}
