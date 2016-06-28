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
import co.cask.tracker.entity.AuditTagsTable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * This class handles requests to the Tracker services API
 */
public final class AuditTagsHandler extends AbstractHttpServiceHandler {
  private AuditTagsTable auditTagsTable;


  private static final Gson GSON = new GsonBuilder().create();
  private static final Type STRING_LIST = new TypeToken<List<String>>() { }.getType();

  // Error messages
  private static final String NO_TAGS_RECEIVED = "No Tags Received";
  private static final String INVALID_TYPE_PARAMETER = "Invalid parameter for 'type' query";

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    auditTagsTable = context.getDataset(TrackerApp.AUDIT_TAGS_DATASET_NAME);
  }

  @Path("v1/tags/demote")
  @POST
  public void demoteTag(HttpServiceRequest request, HttpServiceResponder responder) {
    ByteBuffer requestContents = request.getContent();
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), NO_TAGS_RECEIVED);
      return;
    }
    String tags = StandardCharsets.UTF_8.decode(requestContents).toString();
    List<String> tagsList = GSON.fromJson(tags, STRING_LIST);
    responder.sendJson(auditTagsTable.demoteTag(tagsList));
  }

  @Path("v1/tags/preferred")
  @DELETE
  public void deleteTagsWithoutEntities(HttpServiceRequest request, HttpServiceResponder responder,
                                        @QueryParam("tag") String tag) {
    if (auditTagsTable.deleteTag(tag)) {
      responder.sendStatus(HttpResponseStatus.OK.getCode());
    } else {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST.getCode());
    }
  }

  @Path("v1/tags/promote")
  @POST
  public void addPreferredTags(HttpServiceRequest request, HttpServiceResponder responder) {
    ByteBuffer requestContents = request.getContent();
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), NO_TAGS_RECEIVED);
      return;
    }

    String tags = StandardCharsets.UTF_8.decode(requestContents).toString();
    List<String> tagsList = GSON.fromJson(tags, STRING_LIST);
    auditTagsTable.addPreferredTags(tagsList);
    responder.sendStatus(HttpResponseStatus.OK.getCode());
  }

  @Path("v1/tags/validate")
  @POST
  public void validateTags(HttpServiceRequest request, HttpServiceResponder responder) {
    ByteBuffer requestContents = request.getContent();
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), NO_TAGS_RECEIVED);
      return;
    }
    String tags = StandardCharsets.UTF_8.decode(requestContents).toString();
    List<String> tagsList = GSON.fromJson(tags, STRING_LIST);
    responder.sendJson(HttpResponseStatus.OK.getCode(), auditTagsTable.validateTags(tagsList));
  }

  @Path("v1/tags")
  @GET
  public void getTags(HttpServiceRequest request, HttpServiceResponder responder,
                      @QueryParam("type") @DefaultValue("default") String type,
                      @QueryParam("prefix") @DefaultValue("") String prefix) {
    if (type.equals("user")) {
      responder.sendJson(HttpResponseStatus.OK.getCode(), auditTagsTable.getUserTags(prefix));
    } else if (type.equals("preferred")) {
      responder.sendJson(HttpResponseStatus.OK.getCode(), auditTagsTable.getPreferredTags(prefix));
    } else if (type.equals("default")) {
      responder.sendJson(HttpResponseStatus.OK.getCode(), auditTagsTable.getTags(prefix));
    } else {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), INVALID_TYPE_PARAMETER);
    }
  }

}
