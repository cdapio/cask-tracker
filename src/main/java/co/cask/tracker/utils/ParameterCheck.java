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

package co.cask.tracker.utils;

import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.tracker.AuditLogFlow;
import co.cask.tracker.TrackerApp;
import co.cask.tracker.TrackerService;
import com.google.common.base.Strings;

import java.util.concurrent.TimeUnit;

/**
 * A static class to hold static parameter error checking functions and corresponding error messages.
 */

public class ParameterCheck {

  // Error messages
  public static final String LIMIT_INVALID = "limit cannot be negative or zero.";
  public static final String OFFSET_INVALID = "offset cannot be negative.";
  public static final String STARTTIME_GREATER_THAN_ENDTIME = "startTime cannot be greater than endTime.";
  public static final String INVALID_TIME_FORMAT = "startTime or endTime was not in the correct format. " +
    "Use unix timestamps or date mathematics such as now-1h.";
  public static final String INVALID_TOP_ENTITY_REQUEST = "Invalid request for top entities: path not recognized.";
  public static final String SPECIFY_ENTITY_NAME_AND_TYPE = "entityName and entityType must be specified.";

  public static final String TRACKER_APP_DISPLAY_NAME = "_Tracker";

  public static boolean isLimitValid(int limit) {
    return (limit > 0);
  }

  public static boolean isOffsetValid(int offset) {
    return (offset >= 0);
  }

  public static boolean isDatasetSpecified(String entityType, String entityName) {
    return (!Strings.isNullOrEmpty(entityType) && !Strings.isNullOrEmpty(entityName));
  }

  public static boolean isTimeFrameValid(long startTime, long endTime) {
    return (startTime < endTime);
  }

  public static boolean isTimeFormatValid(long startTime, long endTime) {
    return (startTime != -1 && endTime != -1);
  }

  public static long parseTime(String time) {
    long timeStamp;
    if (time != null) {
      try {
        timeStamp = TimeMathParser.parseTime(time, TimeUnit.SECONDS);
      } catch (IllegalArgumentException e) {
        timeStamp = -1;
      }
    } else {
      timeStamp = -1;
    }
    return timeStamp;
  }

  public static boolean isTrackerDataset(EntityId entityId) {
    if (entityId.getEntity() != EntityType.DATASET) {
      return false;
    }
    switch (EntityIdHelper.getEntityName(entityId)) {
      case TrackerApp.AUDIT_LOG_DATASET_NAME:
      case TrackerApp.AUDIT_METRICS_DATASET_NAME:
      case TrackerApp.AUDIT_TAGS_DATASET_NAME:
      case TrackerApp.ENTITY_LATEST_TIMESTAMP_DATASET_NAME:
        return true;
      default:
        return false;
    }
  }

  public static boolean isTrackerEntity(EntityId entityId) {
    if (EntityIdHelper.getParentApplicationName(entityId).equals(TrackerApp.APP_NAME)) {
      return true;
    }
    return false;
  }
}
