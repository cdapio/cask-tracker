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
import co.cask.tracker.config.TrackerAppConfig;
import co.cask.tracker.entity.AuditLogTable;
import co.cask.tracker.entity.AuditMetricsCube;
import co.cask.tracker.entity.AuditTagsTable;
import co.cask.tracker.entity.EntityLatestTimestampTable;

import java.util.concurrent.TimeUnit;


/**
 * A CDAP Extension that provides the ability to track data ingested either through Cask Hydrator or Custom
 * CDAP Application and provide input to data governance process on cluster.
 */
public class TrackerApp extends AbstractApplication<TrackerAppConfig> {
  public static final String APP_NAME = "Tracker";
  public static final String AUDIT_LOG_DATASET_NAME = "_auditLog";
  public static final String AUDIT_METRICS_DATASET_NAME = "_auditMetrics";
  public static final String ENTITY_LATEST_TIMESTAMP_DATASET_NAME = "_timeSinceTable";
  public static final String AUDIT_TAGS_DATASET_NAME = "_auditTagsTable";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("A CDAP Extension that provides the ability to track data throughout the CDAP platform.");
    createDataset(AUDIT_LOG_DATASET_NAME, AuditLogTable.class);
    String resolutions = String.format("%s,%s,%s,%s",
            TimeUnit.MINUTES.toSeconds(1L),
            TimeUnit.HOURS.toSeconds(1L),
            TimeUnit.DAYS.toSeconds(1L),
            TimeUnit.DAYS.toSeconds(365L));
    DatasetProperties prop =  DatasetProperties.builder()
            .add("dataset.cube.resolutions", resolutions)
            .add("dataset.cube.aggregation.agg1.dimensions",
                    "namespace,entity_type,entity_name,audit_type")
            .add("dataset.cube.aggregation.agg2.dimensions",
                    "namespace,entity_type,entity_name,audit_type," +
                      "program_name,app_name,program_type,accessor_namespace")
            .build();
    createDataset(AUDIT_METRICS_DATASET_NAME, AuditMetricsCube.class, prop);
    createDataset(ENTITY_LATEST_TIMESTAMP_DATASET_NAME, EntityLatestTimestampTable.class);
    createDataset(AUDIT_TAGS_DATASET_NAME, AuditTagsTable.class);
    addFlow(new AuditLogFlow(getConfig()));
    addService(new TrackerService());

  }
}
