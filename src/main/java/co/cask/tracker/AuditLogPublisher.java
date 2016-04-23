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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.tracker.entity.AuditLogTable;
import co.cask.tracker.entity.AuditMetricsCube;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A flowlet to write Audit Log data to a dataset.
 */
public final class AuditLogPublisher extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(AuditLogPublisher.class);
  private static final Gson GSON = new GsonBuilder()
        .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
        .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
        .create();

  @UseDataSet(TrackerApp.AUDIT_LOG_DATASET_NAME)
  private AuditLogTable auditLog;
  @UseDataSet(TrackerApp.AUDIT_METRICS_DATASET_NAME)
  private AuditMetricsCube auditLogMetrics;

  @ProcessInput
  public void process(StreamEvent event) {
    process(Bytes.toString(event.getBody()));
  }

  @ProcessInput
  public void process(String event) {
    if (event.length() > 0) {
      AuditMessage message = GSON.fromJson(event, AuditMessage.class);
      try {
        auditLog.write(message);
      } catch (IOException e) {
        LOG.warn("Ignored writing audit event to log {} due to exception", event, e);
      }
      try {
        auditLogMetrics.write(message);
      } catch (IOException e) {
        LOG.warn("Ignored writing audit event to metrics {} due to exception", event, e);
      }
    }
  }
}
