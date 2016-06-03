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
import co.cask.tracker.entity.AuditLogTable;
import co.cask.tracker.entity.AuditMetricsCube;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;


/**
 * This class handles requests to the AuditLog API.
 */
public final class AuditMetricsHandler extends AbstractHttpServiceHandler {
    @UseDataSet(TrackerApp.AUDIT_METRICS_DATASET_NAME)
    private AuditMetricsCube auditMetricsCube;
    @UseDataSet(TrackerApp.AUDIT_LOG_DATASET_NAME)
    private AuditLogTable auditLogTable;
    private String namespace;

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
        super.initialize(context);
        namespace = context.getNamespace();
    }

    @Path("v1/auditmetrics/topEntities/datasets")
    @GET
    public void topNDatasets(HttpServiceRequest request, HttpServiceResponder responder,
                      @QueryParam("limit") @DefaultValue("10") int limit) {
        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
            return;
        }
        responder.sendJson(200, auditMetricsCube.getTopNDatasets(limit));
    }

    @Path("v1/auditmetrics/topEntities/programs")
    @GET
    public void topNPrograms(HttpServiceRequest request, HttpServiceResponder responder, @QueryParam("limit") @DefaultValue("10") int limit) {
        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
            return;
        }
        responder.sendJson(200, auditLogTable.getTopNPrograms(limit));
    }


    @Path("v1/auditmetrics/topEntities/applications")
    @GET
    public void topNApplications(HttpServiceRequest request, HttpServiceResponder responder, @QueryParam("limit") @DefaultValue("10") int limit) {
        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
            return;
        }
        responder.sendJson(200, auditLogTable.getTopNApplications(limit));
    }

    @Path("v1/auditmetrics/timeSince/program_read")
    @GET
    public void timeSinceProgramRead(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendJson(200,auditLogTable.timeSinceProgramRead());
    }

    @Path("v1/auditmetrics/timeSince/program_write")
    @GET
    public void timeSinceProgramWrite(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendJson(200,auditLogTable.timeSinceProgramWrite());
    }

    @Path("v1/auditmetrics/timeSince/update")
    @GET
    public void timeSinceUpdate(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendJson(200,auditLogTable.timeSinceUpdate());
    }

    @Path("v1/auditmetrics/timeSince/truncate")
    @GET
    public void timeSinceTruncate(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendJson(200,auditLogTable.timeSinceTruncate());
    }

    @Path("v1/auditmetrics/timeSince/metadata_change")
    @GET
    public void timeSinceMetadataChange(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendJson(200,auditLogTable.timeSinceMetadataChange());
    }

}
