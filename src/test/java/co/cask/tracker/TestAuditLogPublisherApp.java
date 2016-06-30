
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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.tracker.entity.AuditLogTable;
import co.cask.tracker.entity.AuditMetricsCube;
import co.cask.tracker.entity.AuditTagsTable;
import co.cask.tracker.entity.EntityLatestTimestampTable;

import java.util.concurrent.TimeUnit;

/**
 * This app is used to test the AuditLog flowlet.
 */
public class TestAuditLogPublisherApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("TestAuditLogPublisherApp");
    setDescription("A temp app to test the AuditLogPublisher flowlet");
    createDataset(TrackerApp.AUDIT_LOG_DATASET_NAME, AuditLogTable.class);
    String resolutions = String.format("%s,%s,%s,%s",
            TimeUnit.MINUTES.toSeconds(1L),
            TimeUnit.HOURS.toSeconds(1L),
            TimeUnit.DAYS.toSeconds(1L),
            TimeUnit.DAYS.toSeconds(365L));
    createDataset(TrackerApp.AUDIT_METRICS_DATASET_NAME,
            AuditMetricsCube.class,
            DatasetProperties.builder()
                    .add("dataset.cube.resolutions", resolutions)
                    .add("dataset.cube.aggregation.agg1.dimensions",
                            "namespace,entity_type,entity_name,audit_type")
                    .add("dataset.cube.aggregation.agg2.dimensions",
                            "namespace,entity_type,entity_name,audit_type,program_name,app_name,program_type")
                    .build());
    createDataset(TrackerApp.ENTITY_LATEST_TIMESTAMP_DATASET_NAME, EntityLatestTimestampTable.class);
    createDataset(TrackerApp.AUDIT_TAGS_DATASET_NAME, AuditTagsTable.class);
    addFlow(new StreamToAuditLogFlow());
    addService(new TrackerService());
  }
}
