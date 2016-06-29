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
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.tracker.entity.AuditMetricsCube;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import co.cask.tracker.entity.EntityLatestTimestampTable;
import co.cask.tracker.entity.TimeSinceResult;
  import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * This class handles requests to the Tracker TruthMeter API
 */
public final class TruthMeterHandler extends AbstractHttpServiceHandler {

  private AuditMetricsCube auditMetricsCube;
  private EntityLatestTimestampTable eltTable;
  private String namespace;

  private static final Type STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new GsonBuilder().create();
  // Err
  private static String NO_INPUT_RECEIVED = "Empty Request Body Received";

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    namespace = context.getNamespace();
    auditMetricsCube = context.getDataset(TrackerApp.AUDIT_METRICS_DATASET_NAME);
    eltTable = context.getDataset(TrackerApp.ENTITY_LATEST_TIMESTAMP_DATASET_NAME);
  }

  @Path("v1/truth-meter")
  @POST
  public void TruthValue(HttpServiceRequest request, HttpServiceResponder responder) {

    ByteBuffer requestContents = request.getContent();
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), NO_INPUT_RECEIVED);
      return;
    }
    String tags = StandardCharsets.UTF_8.decode(requestContents).toString();
    Map<String, String> requestMap = GSON.fromJson(tags, STRING_MAP);
    responder.sendJson(getTruthValueMap(requestMap));

  }

  private Map<String, Integer> getTruthValueMap(Map<String, String> requestMap) {
    long totalProgramsCount = auditMetricsCube.getTotalProgramsCount(namespace);
    long totalActivity = auditMetricsCube.getTotalActivity(namespace) - totalProgramsCount;

    for (Map.Entry<String, String> entry : requestMap.entrySet()) {
      long programsCount = auditMetricsCube.getTotalProgramsCount(namespace, entry.getKey(), entry.getValue());
      long activity = auditMetricsCube.getTotalActivity(namespace, entry.getKey(), entry.getValue()) - programsCount;
      TimeSinceResult timeSinceResult = eltTable.read(namespace, entry.getKey(), entry.getValue());
      long timeSinceRead = timeSinceResult.getTimeSinceEvents().get("read");
    }
    return new HashMap<>();
  }
}
