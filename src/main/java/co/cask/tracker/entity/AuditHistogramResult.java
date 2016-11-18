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

import co.cask.cdap.api.dataset.lib.cube.TimeValue;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A POJO to hold the results for the TopN query.
 */
public class AuditHistogramResult {
  private final Collection<TimeValue> results;
  private final String bucketInterval;

  public AuditHistogramResult(String bucketInterval, Collection<TimeValue> results) {
    this.bucketInterval = bucketInterval;
    this.results = results;
  }

  // Empty result
  public AuditHistogramResult() {
    this("DAY", new ArrayList<TimeValue>());
  }

  public Collection<TimeValue> getResults() {
    return results;
  }

  public String getBucketInterval() {
    return bucketInterval;
  }
}
