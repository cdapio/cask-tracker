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

import co.cask.cdap.api.service.AbstractService;
import co.cask.tracker.config.TrackerAppConfig;

/**
 * A service for accessing the Tracker endpoints through a RESTful API.
 */
public class TrackerService extends AbstractService {
  public static final String SERVICE_NAME = "TrackerService";
  private String zookeeperString;

  public TrackerService(TrackerAppConfig trackerAppConfig) {
    this.zookeeperString = trackerAppConfig.getAuditLogKafkaConfig().getZookeeperString();
  }

  public TrackerService() {
    this.zookeeperString = null;
  }

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("A service that exposes the Tracker endpoints as an API.");
    addHandler(new AuditLogHandler());
    addHandler(new AuditMetricsHandler());
    addHandler(new TrackerMeterHandler());
    addHandler(new AuditTagsHandler(zookeeperString));
    addHandler(new DataDictionaryHandler(zookeeperString));
  }
}
