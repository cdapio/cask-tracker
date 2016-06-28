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

import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.tracker.entity.AuditMetricsCube;

import co.cask.tracker.entity.EntityLatestTimestampTable;
import co.cask.tracker.entity.TimeSinceResult;
import co.cask.tracker.entity.TruthMeterResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * This class handles requests to the Tracker services API
 */
public final class TruthMeterHandler extends AbstractHttpServiceHandler {

  private AuditMetricsCube auditMetricsCube;
  private EntityLatestTimestampTable eltTable;
  private String namespace;
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    namespace = context.getNamespace();
    auditMetricsCube = context.getDataset(TrackerApp.AUDIT_METRICS_DATASET_NAME);
    eltTable = context.getDataset(TrackerApp.ENTITY_LATEST_TIMESTAMP_DATASET_NAME);
  }

  @Path("v1/truth-meter/{entity-type}")
  @GET
  public void TruthValue(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("entity-type") String entityType,
                            @QueryParam("entityName") List<String> entityNameList) {
    responder.sendJson(new TruthMeterResult(entityType, getTruthValueMap(entityType, entityNameList)));
  }

  private Map<String, Integer> getTruthValueMap(String entityType, List<String> entityNameList) {
    long totalProgramsCount = auditMetricsCube.getTotalProgramsCount(namespace);
    long totalActivity = auditMetricsCube.getTotalActivity(namespace) - totalProgramsCount;

    for (String entityName : entityNameList) {
      long programsCount = auditMetricsCube.getTotalProgramsCount(namespace, entityType, entityName);
      long activity = auditMetricsCube.getTotalActivity(namespace, entityType, entityName) - programsCount;
      TimeSinceResult timeSinceResult = eltTable.read(namespace, entityType, entityName);
      long timeSinceRead = timeSinceResult.getTimeSinceEvents().get("read");
    }
    return new HashMap<>();
  }
}
