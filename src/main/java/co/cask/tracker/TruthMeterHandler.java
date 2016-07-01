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
import co.cask.cdap.proto.element.EntityType;
import co.cask.tracker.entity.AuditMetricsCube;
import co.cask.tracker.entity.EntityLatestTimestampTable;
import co.cask.tracker.entity.TimeSinceResult;
import co.cask.tracker.entity.TruthMeterRequest;
import co.cask.tracker.entity.TruthMeterResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
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

  private static final Gson GSON = new GsonBuilder().create();
  // Err
  private static final String NO_INPUT_RECEIVED = "Empty Request Body Received";

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    namespace = context.getNamespace();
    auditMetricsCube = context.getDataset(TrackerApp.AUDIT_METRICS_DATASET_NAME);
    eltTable = context.getDataset(TrackerApp.ENTITY_LATEST_TIMESTAMP_DATASET_NAME);
  }

  @Path("v1/truth-meter")
  @POST
  public void truthValue(HttpServiceRequest request, HttpServiceResponder responder) {

    ByteBuffer requestContents = request.getContent();
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), NO_INPUT_RECEIVED);
      return;
    }
    String tags = StandardCharsets.UTF_8.decode(requestContents).toString();
    TruthMeterRequest truthMeterRequest = GSON.fromJson(tags, TruthMeterRequest.class);
    responder.sendJson(getTruthValueMap(truthMeterRequest));

  }

  private TruthMeterResult getTruthValueMap(TruthMeterRequest truthMeterRequest) {
    List<String> datasets = truthMeterRequest.getDatasets();
    List<String> streams = truthMeterRequest.getStreams();

    long totalProgramsCount = auditMetricsCube.getTotalProgramsCount(namespace);
    long totalActivity = auditMetricsCube.getTotalActivity(namespace) - totalProgramsCount;

    return new TruthMeterResult(truthValueHelper(datasets, EntityType.DATASET.name().toLowerCase(),
                                                 totalActivity, totalProgramsCount),
                                truthValueHelper(streams, EntityType.STREAM.name().toLowerCase(),
                                                 totalActivity, totalProgramsCount));
  }

  private Map<String, Integer> truthValueHelper(List<String> entityNameList, String entityType,
                                                long totalActivity, long totalProgramsCount) {
    Map<String, Integer> resultMap = new HashMap<>();
    for (String entityName : entityNameList) {
      long datasetProgramCount = auditMetricsCube.getTotalProgramsCount(namespace, entityType, entityName);
      long datasetActivity = auditMetricsCube.getTotalActivity(namespace, entityType, entityName) - datasetProgramCount;
      Map<String, Long> map = eltTable.read(namespace, entityType, entityName).getTimeSinceEvents();
      long timeSinceRead;
      if (map.containsKey("read")) {
        timeSinceRead = map.get("read");
      } else {
        timeSinceRead = -1;
      }
      float logScore = ((float) datasetActivity / (float) totalActivity) * 100;
      float programScore = ((float) datasetProgramCount / (float) totalProgramsCount) * 100;
      int score = (int) ((logScore + programScore) / 2);
      resultMap.put(entityName, score);
      // Check if there has ever been a read
    }
    return resultMap;
  }
}
