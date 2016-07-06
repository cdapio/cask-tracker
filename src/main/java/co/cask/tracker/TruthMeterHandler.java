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
import co.cask.tracker.entity.UniqueEntityHolder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
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

  // Score parameter
  private static final float LOG_MESSAGES_WEIGHT = 40.0f;
  private static final float UNIQUE_PROGRAM_WEIGHT = 40.0f;
  private static final float TIME_SINCE_READ_WEIGHT = 20.0f;

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
    String entityList = StandardCharsets.UTF_8.decode(requestContents).toString();
    TruthMeterRequest truthMeterRequest = GSON.fromJson(entityList, TruthMeterRequest.class);
    responder.sendJson(getTruthValueMap(truthMeterRequest));
  }

  // Gets the score and modifies the result to the format expected by the UI
  private TruthMeterResult getTruthValueMap(TruthMeterRequest truthMeterRequest) {
    List<String> datasets = truthMeterRequest.getDatasets();
    List<String> streams = truthMeterRequest.getStreams();

    long totalProgramsCount = auditMetricsCube.getTotalProgramsCount(namespace);
    // program read activity is analyzed independently, so subtracting it here
    long totalActivity = auditMetricsCube.getTotalActivity(namespace) - totalProgramsCount;
    List<UniqueEntityHolder> requestList = getUniqueEntityList(datasets, "dataset");
    requestList.addAll(getUniqueEntityList(streams, "stream"));

    Map<UniqueEntityHolder, Integer> scoreMap = truthValueHelper(requestList, totalActivity, totalProgramsCount);
    Map<String, Integer> datasetMap = new HashMap<>();
    Map<String, Integer> streamMap = new HashMap<>();
    for (Map.Entry<UniqueEntityHolder, Integer> entry : scoreMap.entrySet()) {
      if (entry.getKey().getEntityType().equals("dataset")) {
        datasetMap.put(entry.getKey().getEntityName(), entry.getValue());
      } else {
        streamMap.put(entry.getKey().getEntityName(), entry.getValue());
      }
    }
    return new TruthMeterResult(datasetMap, streamMap);
  }

  // Calculates score for each entity
  private Map<UniqueEntityHolder, Integer> truthValueHelper(List<UniqueEntityHolder> requestList,
                                                            long totalActivity, long totalProgramsCount) {
    Map<UniqueEntityHolder, Integer> resultMap = new HashMap<>();
    for (UniqueEntityHolder uniqueEntity : requestList) {
      long entityProgramCount =
        auditMetricsCube.getTotalProgramsCount(namespace, uniqueEntity.getEntityType(), uniqueEntity.getEntityName());
      long entityActivity =
        auditMetricsCube.getTotalActivity(namespace, uniqueEntity.getEntityType(), uniqueEntity.getEntityName())
          - entityProgramCount;

      // Activity and programs count determine following % each of the final score
      float logScore = ((float) entityActivity / (float) totalActivity) * LOG_MESSAGES_WEIGHT;
      float programScore = ((float) entityProgramCount / (float) totalProgramsCount) * UNIQUE_PROGRAM_WEIGHT;
      int score = (int) (logScore + programScore);
      resultMap.put(uniqueEntity, score);
    }

    /*
     * Score calculation using time since last read
     */
    // Get a list of all datasets and streams stored so far
    List<UniqueEntityHolder> metricsQuery =
      getUniqueEntityList(auditMetricsCube.getEntities(namespace, "dataset"), "dataset");
    metricsQuery.addAll(getUniqueEntityList(auditMetricsCube.getEntities(namespace, "stream"), "stream"));

    // Get a map of time since read stamps for each of them to determine an entity's rank
    Map<UniqueEntityHolder, Long> sortedTimeMap = sortMapByValue(eltTable.getReadTimestamps(namespace, metricsQuery));
    int size = sortedTimeMap.size();
    int rank = size;
    for (Map.Entry<UniqueEntityHolder, Long> entry : sortedTimeMap.entrySet()) {
      // Updates score for entities for which score was requested
      if (resultMap.containsKey(entry.getKey())) {
        UniqueEntityHolder uniqueEntity = entry.getKey();
        int newScore = resultMap.get(uniqueEntity) + (int) ((float) rank / (float) size * TIME_SINCE_READ_WEIGHT);
        resultMap.put(uniqueEntity, newScore);
      }
      rank -= 1;
    }
    return resultMap;
  }

  private static List<UniqueEntityHolder> getUniqueEntityList(List<String> entityList, String entityType) {
    List<UniqueEntityHolder> resultList = new LinkedList<>();
    for (String entity : entityList) {
      resultList.add(new UniqueEntityHolder(entityType, entity));
    }
    return resultList;
  }

  private static Map<UniqueEntityHolder, Long> sortMapByValue(Map<UniqueEntityHolder, Long> map) {
    List<Map.Entry<UniqueEntityHolder, Long>> list = new LinkedList<>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<UniqueEntityHolder, Long>>() {
      @Override
      public int compare(Map.Entry<UniqueEntityHolder, Long> o1, Map.Entry<UniqueEntityHolder, Long> o2) {
        return o2.getValue().compareTo(o1.getValue());
      }
    });
    Map<UniqueEntityHolder, Long> result = new HashMap<>();
    for (Map.Entry<UniqueEntityHolder, Long> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
