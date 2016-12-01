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
import co.cask.tracker.entity.Entity;
import co.cask.tracker.entity.LatestEntityTable;
import co.cask.tracker.entity.TrackerMeterRequest;
import co.cask.tracker.entity.TrackerMeterResult;
import com.google.gson.Gson;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * This class handles requests to the Tracker TruthMeter API
 */
public final class TrackerMeterHandler extends AbstractHttpServiceHandler {

  private AuditMetricsCube auditMetricsCube;
  private LatestEntityTable latestEntityTable;
  private String namespace;

  // Score % parameters
  private static final float LOG_MESSAGES_WEIGHT = 40.0f;
  private static final float UNIQUE_PROGRAM_WEIGHT = 40.0f;
  private static final float TIME_SINCE_READ_WEIGHT = 20.0f;

  private static final Gson GSON = new Gson();
  private static final String DATASET = EntityType.DATASET.name().toLowerCase();
  private static final String STREAM = EntityType.STREAM.name().toLowerCase();

  private static final String EMPTY_REQUEST = "Request body was empty. " +
    "At least one dataset or stream name must be present";

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    namespace = context.getNamespace();
    auditMetricsCube = context.getDataset(TrackerApp.AUDIT_METRICS_DATASET_NAME);
    latestEntityTable = context.getDataset(TrackerApp.ENTITY_LATEST_TIMESTAMP_DATASET_NAME);
  }

  @Path("v1/tracker-meter")
  @POST
  public void trackerMeter(HttpServiceRequest request, HttpServiceResponder responder) {
    ByteBuffer requestContents = request.getContent();
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), EMPTY_REQUEST);
      return;
    }
    TrackerMeterRequest trackerMeterRequest = GSON.fromJson(StandardCharsets.UTF_8.decode(requestContents).toString(),
                                                            TrackerMeterRequest.class);
    if (trackerMeterRequest.getStreams().size() == 0 && trackerMeterRequest.getDatasets().size() == 0) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), EMPTY_REQUEST);
      return;
    }
    responder.sendJson(getTrackerScoreMap(trackerMeterRequest));
  }
  // Gets the score and modifies the result to the format expected by the UI
  private TrackerMeterResult getTrackerScoreMap(TrackerMeterRequest truthMeterRequest) {
    long totalProgramsCount = auditMetricsCube.getTotalProgramsCount(namespace);
    // program read activity is analyzed independently, so subtracting it here
    long totalActivity = auditMetricsCube.getTotalActivity(namespace) - totalProgramsCount;

    List<Entity> requestList = getUniqueEntityList(truthMeterRequest.getDatasets(), DATASET);
    requestList.addAll(getUniqueEntityList(truthMeterRequest.getStreams(), STREAM));

    // Get the score map, and divide entities into datasets and streams
    Map<Entity, Integer> scoreMap = trackerMeterHelper(requestList, totalActivity, totalProgramsCount);
    Map<String, Integer> datasetMap = new HashMap<>();
    Map<String, Integer> streamMap = new HashMap<>();
    for (Map.Entry<Entity, Integer> entry : scoreMap.entrySet()) {
      if (entry.getKey().getEntityType().equals(DATASET)) {
        datasetMap.put(entry.getKey().getEntityName(), entry.getValue());
      } else {
        streamMap.put(entry.getKey().getEntityName(), entry.getValue());
      }
    }
    return new TrackerMeterResult(datasetMap, streamMap);
  }

  // Calculates score for each entity
  private Map<Entity, Integer> trackerMeterHelper(List<Entity> requestList,
                                                  long totalActivity, long totalProgramsCount) {
    Map<Entity, Integer> resultMap = new HashMap<>();
    for (Entity uniqueEntity : requestList) {
      long entityProgramCount =
        auditMetricsCube.getTotalProgramsCount(namespace, uniqueEntity.getEntityType(), uniqueEntity.getEntityName());
      long entityActivity = auditMetricsCube.getTotalActivity(namespace, uniqueEntity.getEntityType(),
                                                              uniqueEntity.getEntityName()) - entityProgramCount;
      float logScore = 0;
      float programScore = 0;
      // Activity and programs count determine following % each of the final score
      if (totalActivity != 0) {
        logScore = ((float) entityActivity / (float) totalActivity) * LOG_MESSAGES_WEIGHT;
      }
      if (totalProgramsCount != 0) {
        programScore = ((float) entityProgramCount / (float) totalProgramsCount) * UNIQUE_PROGRAM_WEIGHT;
      }
      int score = (int) (logScore + programScore);
      resultMap.put(uniqueEntity, score);
    }

    /*
     * Score calculation using time since last read
     */
    // Get a list of all datasets and streams stored so far
    List<Entity> metricsQuery =
      getUniqueEntityList(auditMetricsCube.getEntities(namespace, DATASET), DATASET);
    metricsQuery.addAll(getUniqueEntityList(auditMetricsCube.getEntities(namespace, STREAM), STREAM));

    Map<Entity, Integer> rankMap
      = getRankMap(sortMapByValue(latestEntityTable.getReadTimestamps(namespace, metricsQuery)));
    int size = rankMap.size();
    int rank = size;
    for (Map.Entry<Entity, Integer> entry : rankMap.entrySet()) {
      // Updates score for entities for which score was requested
      if (resultMap.containsKey(entry.getKey())) {
        Entity entity = entry.getKey();
        int newScore = resultMap.get(entity) + (int) ((float) rank / (float) size * TIME_SINCE_READ_WEIGHT);
        resultMap.put(entity, newScore);
      }
    }
    return resultMap;
  }

  // Returns same rank for entities with equal timestamp.
  private Map<Entity, Integer> getRankMap(Map<Entity, Long> sortedTimeMap) {
    Map<Entity, Integer> resultMap = new LinkedHashMap<>();
    // If two entities with same timestamp is given a rank n, the entity after them should have rank n-2 and not
    // n-1. rankChangeStep stores the number of consecutive equal timestamps and fixes this.
    int rank = sortedTimeMap.size();
    int rankChangeStep = 1;
    long lastTimestamp = -1L;
    for (Map.Entry<Entity, Long> entry : sortedTimeMap.entrySet()) {
      resultMap.put(entry.getKey(), rank);
      if (lastTimestamp == entry.getValue()) {
        rankChangeStep += 1;
      } else {
        // Reset rankChangeStep to 1 after subtracting rank to its correct value
        lastTimestamp = entry.getValue();
        rank -= rankChangeStep;
        rankChangeStep = 1;
      }
    }
    return resultMap;
  }

  private static List<Entity> getUniqueEntityList(List<String> entityList, String entityType) {
    List<Entity> resultList = new LinkedList<>();
    for (String entity : entityList) {
      resultList.add(new Entity(entityType, entity));
    }
    return resultList;
  }

  private static Map<Entity, Long> sortMapByValue(Map<Entity, Long> map) {
    List<Map.Entry<Entity, Long>> list = new LinkedList<>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<Entity, Long>>() {
      @Override
      public int compare(Map.Entry<Entity, Long> o1, Map.Entry<Entity, Long> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });
    Map<Entity, Long> result = new LinkedHashMap<>();
    for (Map.Entry<Entity, Long> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
