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
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * An OLAP Cube to store metrics about the AuditLog.
 */
public class AuditMetricsCube extends AbstractDataset {
    private final Cube auditMetrics;

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
        }
    }

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
        return resultsMap;
    }

    public List<TopEntitiesResult> getTopNPrograms(int topN, long startTime, long endTime) {
        CubeQuery programQuery = CubeQuery.builder()
                .select()
                .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
                .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
                .from("agg2")
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
        }
    }

    private Map<String, TopEntitiesResult> transformTopNProgramResult(Collection<TimeSeries> results) {
        HashMap<String, TopEntitiesResult> resultsMap = new HashMap<>();
        for (TimeSeries t : results) {
            String programName = t.getDimensionValues().get("program_name");
            if (!resultsMap.containsKey(programName)) {
                resultsMap.put(programName, new TopEntitiesResult(programName));
            }
            resultsMap.get(programName).addAccessType(t.getMeasureName(),
                    String.valueOf(t.getTimeValues().get(0).getValue()));
        }
        return resultsMap;
    }

    public List<TopEntitiesResult> getTopNApplications(int topN, long startTime, long endTime) {
        CubeQuery applicationQuery = CubeQuery.builder()
                .select()
                .measurement(AccessType.READ.name().toLowerCase(), AggregationFunction.SUM)
                .measurement(AccessType.WRITE.name().toLowerCase(), AggregationFunction.SUM)
                .from("agg2")
                .resolution(TimeUnit.DAYS.toSeconds(365L), TimeUnit.SECONDS)
                .where()
                .dimension("audit_type", AuditType.ACCESS.name().toLowerCase())
                .timeRange(startTime, endTime)
                .groupBy()
                .dimension("app_name")
                .limit(1000)
                .build();
        try {
            Map<String, TopEntitiesResult> auditStats = transformTopNApplicationResult(auditMetrics.query(applicationQuery));
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
            if (!resultsMap.containsKey(appName)) {
                resultsMap.put(appName, new TopEntitiesResult(appName));
            }
            resultsMap.get(appName).addAccessType(t.getMeasureName(),
                    String.valueOf(t.getTimeValues().get(0).getValue()));
        }
        return resultsMap;
    }
}
