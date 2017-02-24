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

import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.tracker.config.TrackerAppConfig;

import javax.annotation.Nullable;

/**
 * Defines the flow for reading from TMS and writing to the Audit Log Dataset.
 */
public class AuditLogFlow extends AbstractFlow {
  public static final String FLOW_NAME = "AuditLogFlow";

  private final TrackerAppConfig trackerAppConfig;

  public AuditLogFlow(@Nullable TrackerAppConfig trackerAppConfig) {
    this.trackerAppConfig = (trackerAppConfig == null) ? new TrackerAppConfig() : trackerAppConfig;
  }

  @Override
  public void configure() {
    setName(FLOW_NAME);
    setDescription("Flow that subscribes to TMS audit messages and stores them in the AuditLog");
    addFlowlet("auditLogConsumer", new AuditLogConsumer(trackerAppConfig.getAuditLogConfig()), 1);
    addFlowlet("auditLogPublisher", new AuditLogPublisher());
    connect("auditLogConsumer", "auditLogPublisher");
  }
}
