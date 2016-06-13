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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.tracker.entity.AuditMetricsCube;
<<<<<<< HEAD
<<<<<<< HEAD
import co.cask.tracker.entity.TopEntitiesResultWrapper;
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7812549... Changes to TopNEntities, Tests, Tests Data, Handler
=======
import co.cask.tracker.entity.TopEntitiesResultWrapper;
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format
import com.google.common.base.Strings;
=======
>>>>>>> 2d45a5b... trying to implement apps and programs. hopeless bug -_-
=======
import com.google.common.base.Strings;
>>>>>>> 497f160... Fixed indentation
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;


/**
 * This class handles requests to the AuditLog API.
 */
public final class AuditMetricsHandler extends AbstractHttpServiceHandler {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  private AuditMetricsCube auditMetricsCube;
  private String namespace;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    namespace = context.getNamespace();
    auditMetricsCube = context.getDataset(TrackerApp.AUDIT_METRICS_DATASET_NAME);
  }

  @Path("v1/auditmetrics/topEntities/datasets")
  @GET
  public void topNDatasets(HttpServiceRequest request, HttpServiceResponder responder,
                           @QueryParam("limit") @DefaultValue("5") int limit,
                           @QueryParam("startTime") @DefaultValue("0") long startTime,
                           @QueryParam("endTime") @DefaultValue("0") long endTime) {
    if (!isLimitValid(limit)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
      return;
    }
    endTime = setEndTime(endTime);
    if (!isTimeFrameValid(startTime, endTime)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "Invalid timeframe");
      return;
    }
    responder.sendJson(HttpResponseStatus.OK.getCode(),
                       new TopEntitiesResultWrapper(auditMetricsCube.getTopNDatasets(limit, startTime, endTime)));
  }

  @Path("v1/auditmetrics/topEntities/programs")
  @GET
  public void topNPrograms(HttpServiceRequest request, HttpServiceResponder responder,
                           @QueryParam("limit") @DefaultValue("5") int limit,
                           @QueryParam("startTime") @DefaultValue("0") long startTime,
                           @QueryParam("endTime") @DefaultValue("0") long endTime,
                           @QueryParam("entityType") @DefaultValue("") String entityType,
                           @QueryParam("entityName") @DefaultValue("") String entityName) {
    if (!isLimitValid(limit)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
      return;
    }
    endTime = setEndTime(endTime);

    if (!isTimeFrameValid(startTime, endTime)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "Invalid timeframe");
      return;
    }
    TopEntitiesResultWrapper result;
    if (!isDatasetSpecified(entityType, entityName)) {
      result = new TopEntitiesResultWrapper(auditMetricsCube.getTopNPrograms(limit, startTime, endTime));
    } else {
      result = new TopEntitiesResultWrapper(auditMetricsCube.getTopNPrograms(limit, startTime, endTime,
                                            namespace, entityType, entityName));
    }
    result.formatDataByTotal();
    responder.sendJson(HttpResponseStatus.OK.getCode(), result);
  }


  @Path("v1/auditmetrics/topEntities/applications")
  @GET
  public void topNApplications(HttpServiceRequest request, HttpServiceResponder responder,
                               @QueryParam("limit") @DefaultValue("5") int limit,
                               @QueryParam("startTime") @DefaultValue("0") long startTime,
                               @QueryParam("endTime") @DefaultValue("0") long endTime,
                               @QueryParam("entityType") @DefaultValue("") String entityType,
                               @QueryParam("entityName") @DefaultValue("") String entityName) {
    if (!isLimitValid(limit)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
      return;
    }
    endTime = setEndTime(endTime);

    if (!isTimeFrameValid(startTime, endTime)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "Invalid timeframe");
      return;
    }
    TopEntitiesResultWrapper result;
    if (!isDatasetSpecified(entityType, entityName)) {
      result = new TopEntitiesResultWrapper(auditMetricsCube.getTopNApplications(limit, startTime, endTime));
    } else {
      result = new TopEntitiesResultWrapper(auditMetricsCube.getTopNApplications(limit, startTime, endTime,
                                            namespace, entityType, entityName));
    }
    result.formatDataByTotal();
    responder.sendJson(HttpResponseStatus.OK.getCode(), result);
  }

  private Boolean isLimitValid (int limit) {
    return (limit >= 0);
  }

  private Boolean isTimeFrameValid (long startTime, long endTime) {
      return (startTime < endTime);
    }

  private boolean isDatasetSpecified (String entityType, String entityName) {
    return (!Strings.isNullOrEmpty(entityType) && !Strings.isNullOrEmpty(entityName));
  }

  private long setEndTime(long endTime) {
    if (endTime == 0) {
      return (System.currentTimeMillis() / 1000);
    }
    return endTime;
  }
=======
    @UseDataSet(TrackerApp.AUDIT_METRICS_DATASET_NAME)
    private AuditMetricsCube auditMetricsCube;
    private String namespace;
=======
  @UseDataSet(TrackerApp.AUDIT_METRICS_DATASET_NAME)
=======
>>>>>>> 3402650... Addressed code review comments
  private AuditMetricsCube auditMetricsCube;
  private String namespace;
>>>>>>> 35030f2... Style, updated based on code review

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    namespace = context.getNamespace();
    auditMetricsCube = context.getDataset(TrackerApp.AUDIT_METRICS_DATASET_NAME);
  }

<<<<<<< HEAD
    @Path("v1/auditmetrics/topEntities/datasets")
    @GET
    public void topNDatasets(HttpServiceRequest request, HttpServiceResponder responder,
                      @QueryParam("limit") @DefaultValue("5") int limit,
                             @QueryParam("startTime") @DefaultValue("0") Long startTime,
                             @QueryParam("endTime") @DefaultValue("0") Long endTime) {
        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
            return;
        }
        if (endTime == 0) {
            endTime = System.currentTimeMillis() / 1000;
        }
<<<<<<< HEAD
<<<<<<< HEAD
        responder.sendJson(200, auditMetricsCube.getTopNDatasets(limit, startTime, endTime));
    }

    @Path("v1/auditmetrics/topEntities/programs")
    @GET
    public void topNPrograms(HttpServiceRequest request, HttpServiceResponder responder,
                             @QueryParam("limit") @DefaultValue("10") int limit,
                             @QueryParam("startTime") @DefaultValue("0") Long startTime,
                             @QueryParam("endTime") @DefaultValue("0") Long endTime) {
        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
            return;
        }
        if (endTime == 0) {
            endTime = System.currentTimeMillis() / 1000;
        }
        responder.sendJson(200, auditMetricsCube.getTopNPrograms(limit, startTime, endTime));
    }


    @Path("v1/auditmetrics/topEntities/applications")
    @GET
    public void topNApplications(HttpServiceRequest request, HttpServiceResponder responder,
                                 @QueryParam("limit") @DefaultValue("10") int limit,
                                 @QueryParam("startTime") @DefaultValue("0") Long startTime,
                                 @QueryParam("endTime") @DefaultValue("0") Long endTime) {
        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
            return;
        }
        if (endTime == 0) {
            endTime = System.currentTimeMillis() / 1000;
        }
        responder.sendJson(200, auditMetricsCube.getTopNApplications(limit, startTime, endTime));
    }

    @Path("v1/auditmetrics/topEntities/applications/byDataset")
    @GET
    public void topNapplicationsByDataset(HttpServiceRequest request, HttpServiceResponder responder,
                                          @QueryParam("limit") @DefaultValue("10") int limit,
                                          @QueryParam("startTime") @DefaultValue("0") Long startTime,
                                          @QueryParam("endTime") @DefaultValue("0") Long endTime,
                                          @QueryParam("entityType") String entityType,
                                          @QueryParam("entityName") String entityName) {
        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
            return;
        }
        if (endTime == 0) {
            endTime = System.currentTimeMillis() / 1000;
        }
        responder.sendJson(200, auditMetricsCube.getTopNApplicationsByDataset(limit, startTime, endTime, namespace,
                entityType, entityName));
    }


    @Path("v1/auditmetrics/topEntities/programs/byDataset")
    @GET
    public void topProgramsByDataset(HttpServiceRequest request, HttpServiceResponder responder,
                                          @QueryParam("limit") @DefaultValue("10") int limit,
                                          @QueryParam("startTime") @DefaultValue("0") Long startTime,
                                          @QueryParam("endTime") @DefaultValue("0") Long endTime,
                                          @QueryParam("entityType") String entityType,
                                          @QueryParam("entityName") String entityName) {
        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
            return;
        }
        if (endTime == 0) {
            endTime = System.currentTimeMillis() / 1000;
        }
        responder.sendJson(200, auditMetricsCube.getTopNProgramsByDataset(limit, startTime, endTime, namespace,
                entityType, entityName));
    }

    @Path("v1/auditmetrics/timeSince")
    @GET
    public void timeSinceChange(HttpServiceRequest request, HttpServiceResponder responder,
                                        @QueryParam("entityType") String entityType,
                                        @QueryParam("entityName") String entityName) {
        if (Strings.isNullOrEmpty(entityName) || Strings.isNullOrEmpty(entityType)) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "EntityName or EntityType cannot be empty");
        }
        responder.sendJson(200, auditMetricsCube.getTimeSinceResult(namespace, entityType, entityName));
    }
<<<<<<< HEAD
<<<<<<< HEAD

    @Path("v1/auditmetrics/timeSince/program_write")
    @GET
    public void timeSinceProgramWrite(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendJson(200, auditLogTable.timeSinceProgramWrite());
    }

    @Path("v1/auditmetrics/timeSince/update")
    @GET
    public void timeSinceUpdate(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendJson(200, auditLogTable.timeSinceUpdate());
    }

    @Path("v1/auditmetrics/timeSince/truncate")
    @GET
    public void timeSinceTruncate(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendJson(200, auditLogTable.timeSinceTruncate());
    }

    @Path("v1/auditmetrics/timeSince/metadata_change")
    @GET
    public void timeSinceMetadataChange(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendJson(200, auditLogTable.timeSinceMetadataChange());
    }
>>>>>>> 8fa1a08... Time since last update/truncate/program_read/program_write/metadata_change implemented

=======
>>>>>>> 0779d97... Some changes to AuditLogTable. Updated tests.
=======
    @Path("v1/auditmetrics/auditLog")
    @GET
    public void auditLog(HttpServiceRequest request, HttpServiceResponder responder,
                         @QueryParam("bucketSize") @DefaultValue("1") Long bucketSize,
                         @QueryParam("startTime") @DefaultValue("0") Long startTime,
                         @QueryParam("endTime") @DefaultValue("0") Long endTime,
                         @QueryParam("entityType") String entityType,
                         @QueryParam("entityName") String entityName) {
        if (endTime == 0) {
            endTime = System.currentTimeMillis() / 1000;
        }
        responder.sendJson(200, auditMetricsCube.getAuditLog(bucketSize, startTime, endTime, namespace, entityType,
                entityName));
=======
=======
        if (startTime > endTime) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "Invalid timeframe");
            return;
        }
>>>>>>> b6204e5... Style changes
        responder.sendJson(200,
                new TopEntitiesResultWrapper(auditMetricsCube.getTopNDatasets(limit, startTime, endTime)));
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format
    }
<<<<<<< HEAD
>>>>>>> e418da7... Updated interfaces
=======
=======
  @Path("v1/auditmetrics/topEntities/datasets")
  @GET
  public void topNDatasets(HttpServiceRequest request, HttpServiceResponder responder,
                           @QueryParam("limit") @DefaultValue("5") int limit,
                           @QueryParam("startTime") @DefaultValue("0") long startTime,
                           @QueryParam("endTime") @DefaultValue("0") long endTime) {
    if (!isLimitValid(limit)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
      return;
    }
    endTime = setEndTime(endTime);
    if (!isTimeFrameValid(startTime, endTime)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "Invalid timeframe");
      return;
    }
    responder.sendJson(HttpResponseStatus.OK.getCode(),
                       new TopEntitiesResultWrapper(auditMetricsCube.getTopNDatasets(limit, startTime, endTime)));
  }
>>>>>>> 35030f2... Style, updated based on code review

  @Path("v1/auditmetrics/topEntities/programs")
  @GET
  public void topNPrograms(HttpServiceRequest request, HttpServiceResponder responder,
                           @QueryParam("limit") @DefaultValue("5") int limit,
                           @QueryParam("startTime") @DefaultValue("0") long startTime,
                           @QueryParam("endTime") @DefaultValue("0") long endTime,
                           @QueryParam("entityType") @DefaultValue("") String entityType,
                           @QueryParam("entityName") @DefaultValue("") String entityName) {
    if (!isLimitValid(limit)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
      return;
    }
    endTime = setEndTime(endTime);

    if (!isTimeFrameValid(startTime, endTime)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "Invalid timeframe");
      return;
    }
    TopEntitiesResultWrapper result;
    if (!isDatasetSpecified(entityType,entityName)) {
      result = new TopEntitiesResultWrapper(auditMetricsCube.getTopNPrograms(limit, startTime, endTime));
    } else {
      result = new TopEntitiesResultWrapper(auditMetricsCube.getTopNPrograms(limit, startTime, endTime,
                                            namespace, entityType, entityName));
    }
    result.formatDataByTotal();
    responder.sendJson(HttpResponseStatus.OK.getCode(), result);
  }


  @Path("v1/auditmetrics/topEntities/applications")
  @GET
  public void topNApplications(HttpServiceRequest request, HttpServiceResponder responder,
                               @QueryParam("limit") @DefaultValue("5") int limit,
                               @QueryParam("startTime") @DefaultValue("0") long startTime,
                               @QueryParam("endTime") @DefaultValue("0") long endTime,
                               @QueryParam("entityType") @DefaultValue("") String entityType,
                               @QueryParam("entityName") @DefaultValue("") String entityName) {
    if (!isLimitValid(limit)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
      return;
    }
    endTime = setEndTime(endTime);

    if (!isTimeFrameValid(startTime, endTime)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "Invalid timeframe");
      return;
    }
    TopEntitiesResultWrapper result;
    if (!isDatasetSpecified(entityType, entityName)) {
      result = new TopEntitiesResultWrapper(auditMetricsCube.getTopNApplications(limit, startTime, endTime));
    } else {
      result = new TopEntitiesResultWrapper(auditMetricsCube.getTopNApplications(limit, startTime, endTime,
                                            namespace, entityType, entityName));
    }
    result.formatDataByTotal();
    responder.sendJson(HttpResponseStatus.OK.getCode(), result);
  }

  private Boolean isLimitValid (int limit) {
    return (limit >= 0);
  }
<<<<<<< HEAD
<<<<<<< HEAD


>>>>>>> 2d45a5b... trying to implement apps and programs. hopeless bug -_-
=======
>>>>>>> 497f160... Fixed indentation
=======

  private Boolean isTimeFrameValid (long startTime, long endTime) {
      return (startTime < endTime);
    }

  private boolean isDatasetSpecified (String entityType, String entityName) {
    return (!Strings.isNullOrEmpty(entityType) && !Strings.isNullOrEmpty(entityName));
  }

  private long setEndTime(long endTime) {
    if (endTime == 0) {
      return (System.currentTimeMillis() / 1000);
    }
    return endTime;
  }

>>>>>>> 3402650... Addressed code review comments
}
