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

import java.util.concurrent.TimeUnit;


/**
 * A CDAP Extension that provides the ability to track data ingested either through Cask Hydrator or Custom
 * CDAP Application and provide input to data governance process on cluster.
 */
public class TrackerApp extends AbstractApplication<TrackerAppConfig> {
  public static final String APP_NAME = "Tracker";
  public static final String AUDIT_LOG_DATASET_NAME = "AuditLog";
  public static final String AUDIT_METRICS_DATASET_NAME = "AuditMetrics";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("A CDAP Extension that provides the ability to track data throughout the CDAP platform.");
    createDataset(AUDIT_LOG_DATASET_NAME, AuditLogTable.class);
    String resolutions = String.format("%s,%s,%s,%s",
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 497f160... Fixed indentation
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
      TimeUnit.MINUTES.toSeconds(1L),
      TimeUnit.HOURS.toSeconds(1L),
      TimeUnit.DAYS.toSeconds(1L),
      TimeUnit.DAYS.toSeconds(365L));
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
    DatasetProperties prop =  DatasetProperties.builder()
                                     .add("dataset.cube.resolutions", resolutions)
                                     .add("dataset.cube.aggregation.agg1.dimensions",
                                          "namespace,entity_type,entity_name,audit_type")
                                     .add("dataset.cube.aggregation.agg2.dimensions",
                                          "namespace,entity_type,entity_name,audit_type,program_name,app_name")
                                     .build();
<<<<<<< HEAD
=======
            TimeUnit.MINUTES.toSeconds(1L),
            TimeUnit.HOURS.toSeconds(1L),
            TimeUnit.DAYS.toSeconds(1L),
            TimeUnit.DAYS.toSeconds(365L));
=======
>>>>>>> 497f160... Fixed indentation
    DatasetProperties prop =  DatasetProperties.builder()
<<<<<<< HEAD
      .add("dataset.cube.resolutions", resolutions)
      .add("dataset.cube.aggregation.agg2.dimensions",
        "namespace,entity_type,entity_name,app_name,audit_type,program_name")
            /*.add("dataset.cube.aggregation.agg3.dimensions",
                    "entity_type,entity_name,audit_type,program_name,app_name") */
<<<<<<< HEAD
            .build();
>>>>>>> 8fa1a08... Time since last update/truncate/program_read/program_write/metadata_change implemented
=======
      .build();
>>>>>>> 497f160... Fixed indentation
=======
                                     .add("dataset.cube.resolutions", resolutions)
                                     .add("dataset.cube.aggregation.agg1.dimensions",
                                          "namespace,entity_type,entity_name,audit_type")
                                     .add("dataset.cube.aggregation.agg2.dimensions",
                                          "namespace,entity_type,entity_name,audit_type,program_name,app_name")
                                     .build();
>>>>>>> 3402650... Addressed code review comments
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
    createDataset(AUDIT_METRICS_DATASET_NAME, AuditMetricsCube.class, prop);

    addFlow(new AuditLogFlow(getConfig()));
    addService(new AuditLogService());
    addService(new AuditMetricsService());

  }
}
