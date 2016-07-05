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
import co.cask.cdap.proto.element.EntityType;
import co.cask.tracker.entity.AuditMetricsCube;
import co.cask.tracker.entity.EntityLatestTimestampTable;
import co.cask.tracker.entity.TruthMeterRequest;
import co.cask.tracker.entity.TruthMeterResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
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
    // program read activity is analyzed independently, so subtracting it here to avoid
    long totalActivity = auditMetricsCube.getTotalActivity(namespace) - totalProgramsCount;

    return new TruthMeterResult(truthValueHelper(datasets, EntityType.DATASET.name().toLowerCase(),
                                                 totalActivity, totalProgramsCount),
                                truthValueHelper(streams, EntityType.STREAM.name().toLowerCase(),
                                                 totalActivity, totalProgramsCount));
  }

  private Map<String, Integer> truthValueHelper(List<String> entityNameList, String entityType,
                                                long totalActivity, long totalProgramsCount) {
    Map<String, Integer> resultMap = new HashMap<>();
    Map<String, Long> timeMap = new HashMap<>();
    for (String entityName : entityNameList) {
      long entityProgramCount = auditMetricsCube.getTotalProgramsCount(namespace, entityType, entityName);
      long entityActivity = auditMetricsCube.getTotalActivity(namespace, entityType, entityName) - entityProgramCount;
      Map<String, Long> map = eltTable.read(namespace, entityType, entityName).getTimeSinceEvents();
      // Check if there has ever been a read on the entity
      if (map.containsKey("read")) {
        timeMap.put(entityName, map.get("read"));
      }
      // Activity and programs count determine 40% each of the final score
      float logScore = ((float) entityActivity / (float) totalActivity) * 40;
      float programScore = ((float) entityProgramCount / (float) totalProgramsCount) * 40;
      int score = (int) (logScore + programScore);
      resultMap.put(entityName, score);
    }
    // This does not scale properly, so will likely be replaced
    Map<String, Long> sortedTimeMap = sortMapByValue(timeMap);
    int size = sortedTimeMap.size();
    int rank = size;
    for (Map.Entry<String, Long> entry : sortedTimeMap.entrySet()) {
      String entityName = entry.getKey();
      int newScore = resultMap.get(entityName) + (int) ((float) rank / (float) size * 20.0);
      resultMap.put(entityName, newScore);
      rank -= 1;
    }
    return resultMap;
  }

  private static Map<String, Long> sortMapByValue(Map<String, Long> map) {
    List<Map.Entry<String, Long>> list = new LinkedList<>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
      @Override
      public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
        return o2.getValue().compareTo(o1.getValue());
      }
    });
    Map<String, Long> result = new HashMap<>();
    for (Map.Entry<String, Long> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
