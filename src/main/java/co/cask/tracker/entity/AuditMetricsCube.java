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

package co.cask.tracker.entity;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.audit.payload.access.AccessType;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespacedId;
import co.cask.tracker.utils.EntityIdHelper;

import com.google.common.base.Strings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.TimeUnit;


/**
 * An OLAP Cube to store metrics about the AuditLog.
 */
public class AuditMetricsCube extends AbstractDataset {
<<<<<<< HEAD
<<<<<<< HEAD
  private final Cube auditMetrics;
=======
    private final Cube auditMetrics;
<<<<<<< HEAD
    private Map<String, TimeSinceResult> timeSinceMap = new HashMap<>();

>>>>>>> 7812549... Changes to TopNEntities, Tests, Tests Data, Handler
=======
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format
=======
  private final Cube auditMetrics;
>>>>>>> 497f160... Fixed indentation

  public AuditMetricsCube(DatasetSpecification spec,
                          @EmbeddedDataset("auditMetrics") Cube auditMetrics) {
    super(spec.getName(), auditMetrics);
    this.auditMetrics = auditMetrics;
  }

  /**
   * Updates cube metrics based on information in the audit message
   * @param auditMessage the message to update the stats for
   * @throws IOException if for some reason, it cannot find the name of the entity
   */
  public void write(AuditMessage auditMessage) throws IOException {
    EntityId entityId = auditMessage.getEntityId();
    if (entityId instanceof NamespacedId) {
      String namespace = ((NamespacedId) entityId).getNamespace();
      EntityType entityType = entityId.getEntity();
      String type = entityType.name().toLowerCase();
      String name = EntityIdHelper.getEntityName(entityId);

      long ts = System.currentTimeMillis() / 1000;
      CubeFact fact = new CubeFact(ts);

<<<<<<< HEAD
<<<<<<< HEAD
  /**
   * Updates cube metrics based on information in the audit message
   * @param auditMessage the message to update the stats for
   * @throws IOException if for some reason, it cannot find the name of the entity
   */
  public void write(AuditMessage auditMessage) throws IOException {
    EntityId entityId = auditMessage.getEntityId();
    if (entityId instanceof NamespacedId) {
      String namespace = ((NamespacedId) entityId).getNamespace();
      EntityType entityType = entityId.getEntity();
      String type = entityType.name().toLowerCase();
      String name = EntityIdHelper.getEntityName(entityId);

      long ts = System.currentTimeMillis() / 1000;
      CubeFact fact = new CubeFact(ts);

=======
>>>>>>> 497f160... Fixed indentation
      fact.addDimensionValue("namespace", namespace);
      fact.addDimensionValue("entity_type", type);
      fact.addDimensionValue("entity_name", name);
      fact.addDimensionValue("audit_type", auditMessage.getType().name().toLowerCase());
      if (auditMessage.getPayload() instanceof AccessPayload) {
        AccessPayload accessPayload = ((AccessPayload) auditMessage.getPayload());
        String programName = EntityIdHelper.getEntityName(accessPayload.getAccessor());
        String appName = EntityIdHelper.getApplicationName(accessPayload.getAccessor());
        if (appName.length() != 0) {
          fact.addDimensionValue("app_name", appName);
<<<<<<< HEAD
=======
    /**
     * Updates cube metrics based on information in the audit message
     * @param auditMessage the message to update the stats for
     * @throws IOException if for some reason, it cannot find the name of the entity
     */
    public void write(AuditMessage auditMessage) throws IOException {
        EntityId entityId = auditMessage.getEntityId();
        if (entityId instanceof NamespacedId) {
            String namespace = ((NamespacedId) entityId).getNamespace();
            EntityType entityType = entityId.getEntity();
            String type = entityType.name().toLowerCase();
            String name = EntityIdHelper.getEntityName(entityId);

            long ts = System.currentTimeMillis() / 1000;
            CubeFact fact = new CubeFact(ts);

            fact.addDimensionValue("entity_type", type);
            fact.addDimensionValue("namespace", namespace);
            fact.addDimensionValue("entity_name", name);
            fact.addDimensionValue("audit_type", auditMessage.getType().name().toLowerCase());
            if (auditMessage.getPayload() instanceof AccessPayload) {
                AccessPayload accessPayload = ((AccessPayload) auditMessage.getPayload());
                String programName = EntityIdHelper.getEntityName(accessPayload.getAccessor());
                String appName = EntityIdHelper.getApplicationName(accessPayload.getAccessor());
                if (appName.length() != 0) {
                    fact.addDimensionValue("app_name", appName);
                }
                fact.addDimensionValue("program_name", programName);

                fact.addMeasurement(accessPayload.getAccessType().name().toLowerCase(), MeasureType.COUNTER, 1L);
            }
            fact.addMeasurement("count", MeasureType.COUNTER, 1L);

            auditMetrics.add(fact);
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0779d97... Some changes to AuditLogTable. Updated tests.
=======
            handleTimeSince(namespace, type, name, auditMessage.getType().name().toLowerCase());

=======
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format
=======
>>>>>>> 497f160... Fixed indentation
        }
        fact.addDimensionValue("program_name", programName);

        fact.addMeasurement(accessPayload.getAccessType().name().toLowerCase(), MeasureType.COUNTER, 1L);
      }
      fact.addMeasurement("count", MeasureType.COUNTER, 1L);

      auditMetrics.add(fact);
    }
  }

<<<<<<< HEAD
<<<<<<< HEAD

<<<<<<< HEAD
<<<<<<< HEAD
    public TimeSinceResult getTimeSinceResult(String namespace, String type, String name) {
        String entityKey = String.format("%s-%s-%s", namespace.toLowerCase(), type.toLowerCase(), name.toLowerCase());
        if (timeSinceMap.containsKey(entityKey)) {
            return timeSinceMap.get(entityKey);
        } else {
            return new TimeSinceResult(namespace, type, name);
>>>>>>> 7812549... Changes to TopNEntities, Tests, Tests Data, Handler
        }
        fact.addDimensionValue("program_name", programName);

        fact.addMeasurement(accessPayload.getAccessType().name().toLowerCase(), MeasureType.COUNTER, 1L);
      }
      fact.addMeasurement("count", MeasureType.COUNTER, 1L);
=======
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format

      auditMetrics.add(fact);
    }
  }

<<<<<<< HEAD
=======
>>>>>>> 497f160... Fixed indentation
  /**
   * Returns the top N datasets with the most audit messages
   *
   * @return A list of entities and their stats sorted in DESC order by count
   */
  public List<TopEntitiesResult> getTopNDatasets(int topN, long startTime, long endTime) {
    CubeQuery datasetQuery = CubeQuery.builder()
      .select()
      .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
      .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
<<<<<<< HEAD
<<<<<<< HEAD
      .from()
=======
      .from("agg2")
>>>>>>> 497f160... Fixed indentation
=======
      .from()
>>>>>>> 3402650... Addressed code review comments
      .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
      .where()
      .dimension("entity_type", EntityType.DATASET.name().toLowerCase())
      .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
      .timeRange(startTime, endTime)
      .groupBy()
      .dimension("entity_name")
      .limit(1000)
      .build();

    CubeQuery streamQuery = CubeQuery.builder()
      .select()
      .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
      .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
<<<<<<< HEAD
<<<<<<< HEAD
      .from()
=======
      .from("agg2")
>>>>>>> 497f160... Fixed indentation
=======
      .from()
>>>>>>> 3402650... Addressed code review comments
      .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
      .where()
      .dimension("entity_type", EntityType.STREAM.name().toLowerCase())
      .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
      .timeRange(startTime, endTime)
      .groupBy()
      .dimension("entity_name")
      .limit(1000)
      .build();

    try {
      Map<String, TopEntitiesResult> auditStats = transformTopNDatasetResult(auditMetrics.query(datasetQuery),
        new HashMap<String, TopEntitiesResult>());
      auditStats = transformTopNDatasetResult(auditMetrics.query(streamQuery), auditStats);
      List<TopEntitiesResult> resultList = new ArrayList<>(auditStats.values());
      Collections.sort(resultList);
      return (topN >= resultList.size()) ? resultList : resultList.subList(0, topN);
    } catch (IllegalArgumentException e) {
      return new ArrayList<>();
<<<<<<< HEAD
=======
=======
>>>>>>> 2d45a5b... trying to implement apps and programs. hopeless bug -_-
=======
>>>>>>> 9d0d1c4... style changes
    /**
     * Returns the top N datasets with the most audit messages
     *
     * @return A list of entities and their stats sorted in DESC order by count
     */
    public List<TopEntitiesResult> getTopNDatasets(int topN, long startTime, long endTime) {
        CubeQuery datasetQuery = CubeQuery.builder()
                .select()
                .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
                .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
                .from("agg2")
                .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
                .where()
                .dimension("entity_type", EntityType.DATASET.name().toLowerCase())
                .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
                .timeRange(startTime, endTime)
                .groupBy()
                .dimension("entity_name")
                .limit(1000)
                .build();

        CubeQuery streamQuery = CubeQuery.builder()
                .select()
                .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
                .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
                .from("agg2")
                .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
                .where()
                .dimension("entity_type", EntityType.STREAM.name().toLowerCase())
                .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
                .timeRange(startTime, endTime)
                .groupBy()
                .dimension("entity_name")
                .limit(1000)
                .build();

        try {
            Map<String, TopEntitiesResult> auditStats = transformTopNDatasetResult(auditMetrics.query(datasetQuery),
                    new HashMap<String, TopEntitiesResult>());
            auditStats = transformTopNDatasetResult(auditMetrics.query(streamQuery), auditStats);
            List<TopEntitiesResult> resultList = new ArrayList<>(auditStats.values());
            Collections.sort(resultList);
            return (topN >= resultList.size()) ? resultList : resultList.subList(0, topN);
        } catch (IllegalArgumentException e) {
            return new ArrayList<>();
        }
=======
>>>>>>> 497f160... Fixed indentation
    }
  }

  private Map<String, TopEntitiesResult> transformTopNDatasetResult(Collection<TimeSeries> results,
                                                                    Map<String, TopEntitiesResult> resultsMap) {
    for (TimeSeries t : results) {
      String entityName = t.getDimensionValues().get("entity_name");
      if (!resultsMap.containsKey(entityName)) {
        resultsMap.put(entityName, new TopEntitiesResult(entityName));
      }
      resultsMap.get(entityName).addAccessType(t.getMeasureName(),
        String.valueOf(t.getTimeValues().get(0).getValue()));
    }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD

    /**
     * Returns the top N Applications with the most dataset access
     * @return A list of apps and their stats sorted in DESC order by count
     */
    public List<TopEntitiesResult> getTopNApplications(int topN, Long startTime, Long endTime) {

        CubeQuery query = CubeQuery.builder()
                .select()
                .measurement("count", AggregationFunction.SUM)
                .from()
                .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
                .where()
                //.dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
                .dimension("program_type", EntityType.APPLICATION.name().toLowerCase())
=======

    public List<TopEntitiesResult> getTopNPrograms(int topN, long startTime, long endTime) {
        CubeQuery programQuery = CubeQuery.builder()
                .select()
                .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
                .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
                .from("agg2")
                .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
                .where()
                .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
>>>>>>> 2d45a5b... trying to implement apps and programs. hopeless bug -_-
                .timeRange(startTime, endTime)
                .groupBy()
                .dimension("program_name")
                .limit(1000)
                .build();
<<<<<<< HEAD
<<<<<<< HEAD
        Collection<TimeSeries> results = auditMetrics.query(query);
<<<<<<< HEAD
        List<TopEntitiesResult> auditStats = transformTopNDatasetResult(results);
        if (auditStats.size() <= topN) {
            return auditStats;
        } else {
            return auditStats.subList(0, topN);
        }
>>>>>>> 8fa1a08... Time since last update/truncate/program_read/program_write/metadata_change implemented
    }
  }

<<<<<<< HEAD
  private Map<String, TopEntitiesResult> transformTopNDatasetResult(Collection<TimeSeries> results,
                                                                    Map<String, TopEntitiesResult> resultsMap) {
    for (TimeSeries t : results) {
      String entityName = t.getDimensionValues().get("entity_name");
      if (!resultsMap.containsKey(entityName)) {
        resultsMap.put(entityName, new TopEntitiesResult(entityName));
      }
      resultsMap.get(entityName).addAccessType(t.getMeasureName(),
        String.valueOf(t.getTimeValues().get(0).getValue()));
    }
    return resultsMap;
  }

=======
    return resultsMap;
  }

>>>>>>> 497f160... Fixed indentation
  public List<TopEntitiesResult> getTopNPrograms(int topN, long startTime, long endTime) {
    CubeQuery programQuery = CubeQuery.builder()
      .select()
      .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
      .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
<<<<<<< HEAD
<<<<<<< HEAD
      .from()
=======
      .from("agg2")
>>>>>>> 497f160... Fixed indentation
=======
      .from()
>>>>>>> 3402650... Addressed code review comments
      .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
      .where()
      .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
      .timeRange(startTime, endTime)
      .groupBy()
      .dimension("program_name")
      .limit(1000)
      .build();
    try {
      Map<String, TopEntitiesResult> auditStats = transformTopNProgramResult(auditMetrics.query(programQuery));
      List<TopEntitiesResult> resultList = new ArrayList<>(auditStats.values());
      Collections.sort(resultList);
      return (topN >= resultList.size()) ? resultList : resultList.subList(0, topN);
    } catch (IllegalArgumentException e) {
      return new ArrayList<>();
<<<<<<< HEAD
    }
  }

  public List<TopEntitiesResult> getTopNPrograms(int topN, long startTime, long endTime,
                                                 String namespace, String entityType, String entityName) {
    CubeQuery programQuery = CubeQuery.builder()
      .select()
      .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
      .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
      .from()
      .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
      .where()
      .dimension("namespace", namespace)
      .dimension("entity_name", entityName)
      .dimension("entity_type", entityType)
      .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
      .timeRange(startTime, endTime)
      .groupBy()
      .dimension("program_name")
      .limit(1000)
      .build();
    try {
      Map<String, TopEntitiesResult> auditStats = transformTopNProgramResult(auditMetrics.query(programQuery));
      List<TopEntitiesResult> resultList = new ArrayList<>(auditStats.values());
      Collections.sort(resultList);
      return (topN >= resultList.size()) ? resultList : resultList.subList(0, topN);
    } catch (IllegalArgumentException e) {
      return new ArrayList<>();
=======
        List<TopEntitiesResult> auditStats = transformTopNApplicationResult(results);
        return (topN >= auditStats.size()) ? auditStats : auditStats.subList(0, topN);
>>>>>>> e418da7... Updated interfaces
=======
        try {
            Collection<TimeSeries> results = auditMetrics.query(query);
            List<TopEntitiesResult> auditStats = transformTopNApplicationResult(results);
            return (topN >= auditStats.size()) ? auditStats : auditStats.subList(0, topN);
        } catch (IllegalArgumentException e) {
            return new ArrayList<>();
        }
>>>>>>> 7812549... Changes to TopNEntities, Tests, Tests Data, Handler
    }
  }

<<<<<<< HEAD
  private Map<String, TopEntitiesResult> transformTopNProgramResult(Collection<TimeSeries> results) {
    HashMap<String, TopEntitiesResult> resultsMap = new HashMap<>();
    for (TimeSeries t : results) {
      String programName = t.getDimensionValues().get("program_name");
      if (Strings.isNullOrEmpty(programName)) {
        continue;
      }
      if (!resultsMap.containsKey(programName)) {
        resultsMap.put(programName, new TopEntitiesResult(programName));
      }
      resultsMap.get(programName).addAccessType(t.getMeasureName(),
        String.valueOf(t.getTimeValues().get(0).getValue()));
    }
    return resultsMap;
  }

  public List<TopEntitiesResult> getTopNApplications(int topN, long startTime, long endTime,
                                                     String namespace, String entityType, String entityName) {
    CubeQuery applicationQuery = CubeQuery.builder()
      .select()
      .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
      .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
      .from()
      .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
      .where()
      .dimension("namespace", namespace)
      .dimension("entity_name", entityName)
      .dimension("entity_type", entityType)
      .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
      .timeRange(startTime, endTime)
      .groupBy()
      .dimension("app_name")
      .limit(1000)
      .build();
    try {
      Map<String, TopEntitiesResult> auditStats
        = transformTopNApplicationResult(auditMetrics.query(applicationQuery));
      List<TopEntitiesResult> resultList = new ArrayList<>(auditStats.values());
      Collections.sort(resultList);
      return (topN >= resultList.size()) ? resultList : resultList.subList(0, topN);
    } catch (IllegalArgumentException e) {
      return new ArrayList<>();
    }
  }

  public List<TopEntitiesResult> getTopNApplications(int topN, long startTime, long endTime) {
    CubeQuery applicationQuery = CubeQuery.builder()
      .select()
      .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
      .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
      .from()
      .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
      .where()
      .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
      .timeRange(startTime, endTime)
      .groupBy()
      .dimension("app_name")
      .limit(1000)
      .build();
    try {
      Map<String, TopEntitiesResult> auditStats
        = transformTopNApplicationResult(auditMetrics.query(applicationQuery));
      List<TopEntitiesResult> resultList = new ArrayList<>(auditStats.values());
      Collections.sort(resultList);
      return (topN >= resultList.size()) ? resultList : resultList.subList(0, topN);
    } catch (IllegalArgumentException e) {
      return new ArrayList<>();
    }
  }


  private Map<String, TopEntitiesResult> transformTopNApplicationResult(Collection<TimeSeries> results) {
    HashMap<String, TopEntitiesResult> resultsMap = new HashMap<>();
    for (TimeSeries t : results) {
      String appName = t.getDimensionValues().get("app_name");
      if (Strings.isNullOrEmpty(appName)) {
        continue;
      }
      if (!resultsMap.containsKey(appName)) {
        resultsMap.put(appName, new TopEntitiesResult(appName));
      }
      resultsMap.get(appName).addAccessType(t.getMeasureName(),
        String.valueOf(t.getTimeValues().get(0).getValue()));
    }
    return resultsMap;
  }
=======
    private List<TopEntitiesResult> transformTopNDatasetResult(Collection<TimeSeries> results) {
=======

    /**
     * Returns the top N Applications with the most access for a given dataset
     * @return A list of apps and their stats sorted in DESC order by count
     */
    public List<TopEntitiesResult> getTopNApplicationsByDataset (int topN, Long startTime,
                                                                  Long endTime, String nameSpace, String entityType,
                                                                  String entityName) {
        CubeQuery query = CubeQuery.builder()
                .select()
                .measurement("count", AggregationFunction.SUM)
                .from()
                .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
                .where()
                .dimension("namespace", nameSpace)
                .dimension("entity_type", entityType)
                .dimension("entity_name", entityName)
                .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
                .dimension("program_type", EntityType.APPLICATION.name().toLowerCase())
                .timeRange(startTime, endTime)
                .groupBy()
                .limit(1000)
                .build();
        Collection<TimeSeries> results = auditMetrics.query(query);
        List<TopEntitiesResult> auditStats = transformTopNApplicationResult(results);
        return (topN >= auditStats.size()) ? auditStats : auditStats.subList(0, topN);
    }


    private List<TopEntitiesResult> transformTopNApplicationResult(Collection<TimeSeries> results) {
>>>>>>> e418da7... Updated interfaces
        Map<String, TopEntitiesResult> resultsMap = new HashMap<>();
        for (TimeSeries t : results) {
            String programName = t.getDimensionValues().get("program_name");

            if (!resultsMap.containsKey(programName)) {
                resultsMap.put(programName, new TopEntitiesResult(programName));
            }
            resultsMap.get(programName).addAccessType(t.getMeasureName(), t.getTimeValues().get(0).getValue());
        }
        List<TopEntitiesResult> auditStats = new ArrayList<>(resultsMap.values());
        Collections.sort(auditStats);
        return auditStats;
    }

<<<<<<< HEAD
    public List<TopEntitiesResult> getTopNPrograms (int topN) {
        throw new RuntimeException("Method not implemented");
    }

    public List<TopEntitiesResult> getTopNApplications (int topN) {
        throw new RuntimeException("Method not implemented");
    }


>>>>>>> 8fa1a08... Time since last update/truncate/program_read/program_write/metadata_change implemented
=======

    /**
     * Returns the top N Applications with the most access for a given dataset
     * @return A list of apps and their stats sorted in DESC order by count
     */
    public List<TopEntitiesResult> getAuditLog(Long bucketSize, Long startTime, Long endTime, String nameSpace,
                            String entityType, String entityName)  {
        CubeQuery query = CubeQuery.builder()
                .select()
                .measurement("count", AggregationFunction.SUM)
                .from()
                .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS) //Fix
                .where()
                .dimension("namespace", nameSpace)
                .dimension("entity_type", entityType)
                .dimension("entity_name", entityName)
                .timeRange(startTime, endTime)
                .groupBy()
                .limit(1000)
                .build();
        Collection<TimeSeries> results = auditMetrics.query(query);
        List<TopEntitiesResult> auditStats = transformTopNApplicationResult(results);
        return auditStats;

    }
>>>>>>> e418da7... Updated interfaces
=======
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format
=======
        try {
            Map<String, TopEntitiesResult> auditStats = transformTopNProgramResult(auditMetrics.query(programQuery));
            List<TopEntitiesResult> resultList = new ArrayList<>(auditStats.values());
            Collections.sort(resultList);
            return (topN >= resultList.size()) ? resultList : resultList.subList(0, topN);
        } catch (IllegalArgumentException e) {
            return new ArrayList<>();
        }
=======
>>>>>>> 497f160... Fixed indentation
    }
  }

  public List<TopEntitiesResult> getTopNPrograms(int topN, long startTime, long endTime,
                                                 String namespace, String entityType, String entityName) {
    CubeQuery programQuery = CubeQuery.builder()
      .select()
      .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
      .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
      .from()
      .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
      .where()
      .dimension("namespace", namespace)
      .dimension("entity_name", entityName)
      .dimension("entity_type", entityType)
      .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
      .timeRange(startTime, endTime)
      .groupBy()
      .dimension("program_name")
      .limit(1000)
      .build();
    try {
      Map<String, TopEntitiesResult> auditStats = transformTopNProgramResult(auditMetrics.query(programQuery));
      List<TopEntitiesResult> resultList = new ArrayList<>(auditStats.values());
      Collections.sort(resultList);
      return (topN >= resultList.size()) ? resultList : resultList.subList(0, topN);
    } catch (IllegalArgumentException e) {
      return new ArrayList<>();
    }
  }

  private Map<String, TopEntitiesResult> transformTopNProgramResult(Collection<TimeSeries> results) {
    HashMap<String, TopEntitiesResult> resultsMap = new HashMap<>();
    for (TimeSeries t : results) {
      String programName = t.getDimensionValues().get("program_name");
      if (Strings.isNullOrEmpty(programName)) {
        continue;
      }
      if (!resultsMap.containsKey(programName)) {
        resultsMap.put(programName, new TopEntitiesResult(programName));
      }
      resultsMap.get(programName).addAccessType(t.getMeasureName(),
        String.valueOf(t.getTimeValues().get(0).getValue()));
    }
    return resultsMap;
  }

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d45a5b... trying to implement apps and programs. hopeless bug -_-
=======
    private Map<String, TopEntitiesResult> transformTopNApplicationResult(Collection<TimeSeries> results) {
        HashMap<String, TopEntitiesResult> resultsMap = new HashMap<>();
        for (TimeSeries t : results) {
            String appName = t.getDimensionValues().get("app_name");
            if (!resultsMap.containsKey(appName)) {
                resultsMap.put(appName, new TopEntitiesResult(appName));
            }
            resultsMap.get(appName).addAccessType(t.getMeasureName(),
                    String.valueOf(t.getTimeValues().get(0).getValue()));
        }
        return resultsMap;
    }
<<<<<<< HEAD

>>>>>>> 35030f2... Style, updated based on code review
=======
>>>>>>> 9d0d1c4... style changes
=======
  public List<TopEntitiesResult> getTopNApplications(int topN, long startTime, long endTime,
                                                     String namespace, String entityType, String entityName) {
    CubeQuery applicationQuery = CubeQuery.builder()
      .select()
      .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
      .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
      .from()
      .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
      .where()
      .dimension("namespace", namespace)
      .dimension("entity_name", entityName)
      .dimension("entity_type", entityType)
      .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
      .timeRange(startTime, endTime)
      .groupBy()
      .dimension("app_name")
      .limit(1000)
      .build();
    try {
      Map<String, TopEntitiesResult> auditStats
        = transformTopNApplicationResult(auditMetrics.query(applicationQuery));
      List<TopEntitiesResult> resultList = new ArrayList<>(auditStats.values());
      Collections.sort(resultList);
      return (topN >= resultList.size()) ? resultList : resultList.subList(0, topN);
    } catch (IllegalArgumentException e) {
      return new ArrayList<>();
    }
  }

  public List<TopEntitiesResult> getTopNApplications(int topN, long startTime, long endTime) {
    CubeQuery applicationQuery = CubeQuery.builder()
      .select()
      .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
      .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
      .from()
      .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
      .where()
      .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
      .timeRange(startTime, endTime)
      .groupBy()
      .dimension("app_name")
      .limit(1000)
      .build();
    try {
      Map<String, TopEntitiesResult> auditStats
        = transformTopNApplicationResult(auditMetrics.query(applicationQuery));
      List<TopEntitiesResult> resultList = new ArrayList<>(auditStats.values());
      Collections.sort(resultList);
      return (topN >= resultList.size()) ? resultList : resultList.subList(0, topN);
    } catch (IllegalArgumentException e) {
      return new ArrayList<>();
    }
  }


  private Map<String, TopEntitiesResult> transformTopNApplicationResult(Collection<TimeSeries> results) {
    HashMap<String, TopEntitiesResult> resultsMap = new HashMap<>();
    for (TimeSeries t : results) {
      String appName = t.getDimensionValues().get("app_name");
      if (Strings.isNullOrEmpty(appName)) {
        continue;
      }
      if (!resultsMap.containsKey(appName)) {
        resultsMap.put(appName, new TopEntitiesResult(appName));
      }
      resultsMap.get(appName).addAccessType(t.getMeasureName(),
        String.valueOf(t.getTimeValues().get(0).getValue()));
    }
    return resultsMap;
  }
>>>>>>> 497f160... Fixed indentation
}
