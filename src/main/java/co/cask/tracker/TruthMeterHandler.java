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

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    auditMetricsCube = context.getDataset(TrackerApp.AUDIT_METRICS_DATASET_NAME);
  }

  @Path("v1/truth-meter/{entity-type}")
  @GET
  public void TruthValue(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("entity-type") String entityType,
                            @QueryParam("entityName") List<String> entityName) {
    Map<String, Integer> values = new HashMap<>();
    for (String entity : entityName) {
      values.put(entity, getTruthValue(entity, entityType));
    }
    TruthMeterResult result = new TruthMeterResult(entityType, values);
    responder.sendJson(result);
  }

  private int getTruthValue(String entityName, String entityType) {
    return 0;
  }
}
