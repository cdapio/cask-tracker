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

import java.util.List;

/**
 *  A POJO to hold the request for Truth Meter
 */
public final class TruthMeterRequest {

  private final List<String> datasets;
  private final List<String> streams;

  public TruthMeterRequest(List<String> datasets, List<String> streams) {
    this.datasets = datasets;
    this.streams = streams;
  }

  public List<String> getDatasets() {
    return datasets;
  }

  public List<String> getStreams() {
    return streams;
  }
}
