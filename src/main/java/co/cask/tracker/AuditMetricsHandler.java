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
import co.cask.tracker.entity.TopEntitiesResultWrapper;
import com.google.common.base.Strings;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;


/**
 * This class handles requests to the AuditLog API.
 */
public final class AuditMetricsHandler extends AbstractHttpServiceHandler {
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

}
