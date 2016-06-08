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
    private final Cube auditMetrics;
    private Map<String, TimeSinceResult> timeSinceMap = new HashMap<>();


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
            long ts = System.currentTimeMillis() / 1000;
            CubeFact fact = new CubeFact(ts);

            fact.addDimensionValue("namespace", namespace);

            EntityType entityType = entityId.getEntity();
            String type = entityType.name().toLowerCase();
            fact.addDimensionValue("entity_type", type);

            String name = EntityIdHelper.getEntityName(entityId);
            fact.addDimensionValue("entity_name", name);
            fact.addDimensionValue("audit_type", auditMessage.getType().name().toLowerCase());
            fact.addMeasurement("count", MeasureType.COUNTER, 1L);

            fact.addMeasurement(auditMessage.getType().name().toLowerCase(), MeasureType.COUNTER, 1L);
            if (auditMessage.getPayload() instanceof AccessPayload) {
                AccessPayload accessPayload = ((AccessPayload) auditMessage.getPayload());
                String programName = EntityIdHelper.getEntityName(accessPayload.getAccessor());
                fact.addDimensionValue("program_name", programName);
                fact.addDimensionValue("program_type", accessPayload.getAccessor().getEntity().name().toLowerCase());
                // Adds column for READ/WRITE/UNKNOWN access
                fact.addMeasurement(accessPayload.getAccessType().name().toLowerCase(), MeasureType.COUNTER, 1L);
            }
            auditMetrics.add(fact);
            handleTimeSince(namespace, type, name, auditMessage.getType().name().toLowerCase());

        }
    }

    private void handleTimeSince(String namespace, String type, String name, String auditType) {
        String entityKey = String.format("%s-%s-%s", namespace.toLowerCase(), type.toLowerCase(), name.toLowerCase());
        if (!timeSinceMap.containsKey(entityKey)) {
            timeSinceMap.put(entityKey, new TimeSinceResult(namespace, type, name));
        }
        timeSinceMap.get(entityKey).addEventTime(auditType, System.currentTimeMillis());
    }

    public TimeSinceResult getTimeSinceResult(String namespace, String type, String name) {
        String entityKey = String.format("%s-%s-%s", namespace.toLowerCase(), type.toLowerCase(), name.toLowerCase());
        if (timeSinceMap.containsKey(entityKey)) {
            return timeSinceMap.get(entityKey);
        } else {
            return new TimeSinceResult(namespace, type, name);
        }
    }

    /**
     * Returns the top N datasets with the most audit messages
     * @return A list of entities and their stats sorted in DESC order by count
     */
    public List<TopEntitiesResult> getTopNDatasets(int topN, Long startTime, Long endTime) {
        CubeQuery datasetQuery = CubeQuery.builder()
                .select()
                .measurement("count", AggregationFunction.SUM)
                .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
                .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
                .from()
                .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
                .where()
                .dimension("entity_type", EntityType.DATASET.name().toLowerCase())
                //.dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
                .timeRange(startTime, endTime)
                .groupBy()
                .dimension("namespace")
                .dimension("entity_type")
                .dimension("entity_name")
                .limit(1000)
                .build();

        CubeQuery streamQuery = CubeQuery.builder()
                .select()
                .measurement("count", AggregationFunction.SUM)
                .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
                .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
                .from()
                .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
                .where()
                .dimension("entity_type", EntityType.STREAM.name().toLowerCase())
                //.dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
                .timeRange(startTime, endTime)
                .groupBy()
                .dimension("namespace")
                .dimension("entity_type")
                .dimension("entity_name")
                .limit(1000)
                .build();

        try {
            Collection<TimeSeries> datasetResults = auditMetrics.query(datasetQuery);
            Collection<TimeSeries> streamResults = auditMetrics.query(streamQuery);
            List<TopEntitiesResult> auditStats = transformTopNDatasetResult(datasetResults, streamResults);
            return (topN >= auditStats.size()) ? auditStats : auditStats.subList(0, topN);
        } catch (IllegalArgumentException e) {
            return new ArrayList<>();

        }
    }

    private List<TopEntitiesResult> transformTopNDatasetResult(Collection<TimeSeries> datasetResults,
                                                               Collection<TimeSeries> streamResults) {
        Map<String, TopEntitiesResult> resultsMap = new HashMap<>();
        for (TimeSeries t : datasetResults) {
            String namespace = t.getDimensionValues().get("namespace");
            String entityType = t.getDimensionValues().get("entity_type");
            String entityName = t.getDimensionValues().get("entity_name");
            String key = String.format("%s-%s-%s", namespace.toLowerCase(), entityType.toLowerCase(),
                    entityName.toLowerCase());
            if (!resultsMap.containsKey(key)) {
                resultsMap.put(key, new TopEntitiesResult(entityName));
            }
            resultsMap.get(key).addAccessType(t.getMeasureName(), t.getTimeValues().get(0).getValue());
        }

        for (TimeSeries t : streamResults) {
            String namespace = t.getDimensionValues().get("namespace");
            String entityType = t.getDimensionValues().get("entity_type");
            String entityName = t.getDimensionValues().get("entity_name");
            String key = String.format("%s-%s-%s", namespace.toLowerCase(), entityType.toLowerCase(),
                    entityName.toLowerCase());
            if (!resultsMap.containsKey(key)) {
                resultsMap.put(key, new TopEntitiesResult(entityName));
            }
            resultsMap.get(key).addAccessType(t.getMeasureName(), t.getTimeValues().get(0).getValue());
        }
        List<TopEntitiesResult> auditStats = new ArrayList<>(resultsMap.values());
        Collections.sort(auditStats);
        return auditStats;
    }

    /**
     * Returns the top N programs with the most dataset access
     * @return A list of entities and their stats sorted in DESC order by count
     */
    public List<TopEntitiesResult> getTopNPrograms(int topN, Long startTime, Long endTime) {
        CubeQuery query = CubeQuery.builder()
                .select()
                .measurement("count", AggregationFunction.SUM)
                .from()
                .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
                .where()
                //.dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
                .dimension("program_type", EntityType.PROGRAM.name().toLowerCase())
                .timeRange(startTime, endTime)
                .groupBy()
                .dimension("program_name")
                .limit(1000)
                .build();
        try {
            Collection<TimeSeries> results = auditMetrics.query(query);
            List<TopEntitiesResult> auditStats = transformTopNProgramResult(results);
            return (topN >= auditStats.size()) ? auditStats : auditStats.subList(0, topN);
        } catch (IllegalArgumentException e) {
            return new ArrayList<>();
        }
    }


    /**
     * Returns the top N programs with the most access for the given dataset
     * @return A list of entities and their stats sorted in DESC order by count
     */
    public List<TopEntitiesResult> getTopNProgramsByDataset (int topN, Long startTime,
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
                //.dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
                .dimension("program_type", EntityType.PROGRAM.name().toLowerCase())
                .timeRange(startTime, endTime)
                .groupBy()
                .limit(1000)
                .build();
        Collection<TimeSeries> results = auditMetrics.query(query);
        List<TopEntitiesResult> auditStats = transformTopNProgramResult(results);
        return (topN >= auditStats.size()) ? auditStats : auditStats.subList(0, topN);
    }

    private List<TopEntitiesResult> transformTopNProgramResult(Collection<TimeSeries> results) {
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
                .timeRange(startTime, endTime)
                .groupBy()
                .dimension("program_name")
                .limit(1000)
                .build();
        try {
            Collection<TimeSeries> results = auditMetrics.query(query);
            List<TopEntitiesResult> auditStats = transformTopNApplicationResult(results);
            return (topN >= auditStats.size()) ? auditStats : auditStats.subList(0, topN);
        } catch (IllegalArgumentException e) {
            return new ArrayList<>();
        }
    }


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
}
