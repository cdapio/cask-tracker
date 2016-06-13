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
 * A POJO to hold the results for the TopN query in format expected by the api
 */
<<<<<<< HEAD
<<<<<<< HEAD
public class TopEntitiesResultWrapper {
  private final List<TopEntitiesResult> results;
  private final int total;

  public TopEntitiesResultWrapper(List<TopEntitiesResult> resultList) {
    this.results = resultList;
    this.total = resultList.size();
  }

  public int getTotal() {
    return total;
  }

  public List<TopEntitiesResult> getResultList() {
    return results;
  }

  public void formatDataByTotal() {
    for (TopEntitiesResult result: results) {
      result.formatDataByTotal();
    }
  }
=======
public class TopEntitiesResultWrapper  {
    private List<TopEntitiesResult> results;
    private int total;
=======
public class TopEntitiesResultWrapper {
<<<<<<< HEAD
    private final List<TopEntitiesResult> results;
    private final int total;
>>>>>>> 35030f2... Style, updated based on code review
=======
  private final List<TopEntitiesResult> results;
  private final int total;
>>>>>>> 497f160... Fixed indentation

  public TopEntitiesResultWrapper(List<TopEntitiesResult> resultList) {
    this.results = resultList;
    this.total = resultList.size();
  }

  public int getTotal() {
    return total;
  }

<<<<<<< HEAD
    public List<TopEntitiesResult> getResultList() {
        return results;
    }
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format
=======
  public List<TopEntitiesResult> getResultList() {
    return results;
  }
>>>>>>> 497f160... Fixed indentation

  public void formatDataByTotal() {
    for (TopEntitiesResult result: results) {
      result.formatDataByTotal();
    }
  }

}
