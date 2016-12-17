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

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.tracker.config.AuditLogConfig;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Subscribes to TMS messages published by the CDAP Platform that contains the Audit log records.
 */
public final class AuditLogConsumer extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(AuditLogConsumer.class);
  private static final String OFFSET = "tms.offset";

  // TODO: Add a way to reset the offset
  private KeyValueTable offsetStore;
  private OutputEmitter<String> emitter;

  @Property
  private final String offsetDatasetName;
  @Property
  private final String namespace;
  @Property
  private final String topic;
  @Property
  private final int limit;

  private Stopwatch stopwatch;
  private MessageFetcher messageFetcher;
  private long timeout;
  private boolean emptyIterator;

  public AuditLogConsumer(AuditLogConfig auditLogConfig) {
    this.offsetDatasetName = auditLogConfig.getOffsetDataset();
    this.namespace = auditLogConfig.getNamespace();
    this.topic = auditLogConfig.getTopic();
    this.limit = auditLogConfig.getLimit();
  }

  @Override
  protected void configure() {
    createDataset(offsetDatasetName, KeyValueTable.class);
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    offsetStore = context.getDataset(offsetDatasetName);
    String shortTxTimeout = context.getRuntimeArguments().get("data.tx.timeout");
    if (shortTxTimeout == null) {
      // If custom tx timeout is not used, assume it is 30 secs
      shortTxTimeout = "30";
    }

    // Reduce 10s from the tx timeout
    timeout = Long.parseLong(shortTxTimeout) - 10;
    stopwatch = new Stopwatch();
    emptyIterator = false;
    messageFetcher = getContext().getMessageFetcher();
  }

  @Tick(delay = 1L, unit = TimeUnit.SECONDS)
  protected void pollAuditTopic() throws Exception {
    String newOffset = null;
    byte[] logOffset = offsetStore.read(OFFSET);
    String fromOffset = null;

    if (logOffset != null) {
      fromOffset = Bytes.toString(logOffset);
    }

    // Keep fetching in batches of 'limit' number of messages until, no messages are left or
    // the stopWatch timeout expires
    stopwatch.reset();
    stopwatch.start();
    do {
      emptyIterator = true;
      try (CloseableIterator<Message> auditMessages = messageFetcher.fetch(namespace, topic, limit, fromOffset)) {
        while (auditMessages.hasNext()) {
          Message message = auditMessages.next();
          newOffset = message.getId();
          emitter.emit(message.getPayloadAsString());
          emptyIterator = false;
        }
      } catch (TopicNotFoundException ex) {
        LOG.warn("Audit Topic {} was not found.", topic, ex);
      } finally {
        if (!emptyIterator) {
          // If some messages were fetched in this loop, update the new offset and
          // set fromOffset to the last fetched messageId
          offsetStore.write(OFFSET, newOffset);
          fromOffset = newOffset;
        }
      }

      // If no messages were found in this iteration, then break out of the loop
      if (emptyIterator) {
        break;
      }
    } while (stopwatch.elapsedTime(TimeUnit.SECONDS) < timeout);
    stopwatch.stop();
  }
}
