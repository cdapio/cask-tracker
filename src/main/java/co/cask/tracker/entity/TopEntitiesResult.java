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

import java.util.HashMap;
import java.util.Map;

/**
 * A POJO to hold the results for the TopN query.
 */
public class TopEntitiesResult implements Comparable<TopEntitiesResult> {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 497f160... Fixed indentation
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
  private final Map<String, String> columnValues;

  public TopEntitiesResult(String entityName) {
    this.columnValues = new HashMap<>();
    columnValues.put("label", entityName);
    columnValues.put("read", "0");
    columnValues.put("write", "0");
  }

  public Map<String, String> getColumnValues() {
    return columnValues;
  }

  public void addAccessType(String type, String value) {
    this.columnValues.put(type, value);
  }

  public void formatDataByTotal() {
    columnValues.put("value",
      String.valueOf(Long.parseLong(columnValues.get("read")) + Long.parseLong(columnValues.get("write"))));
    columnValues.remove("read");
    columnValues.remove("write");
  }

  @Override
  public int compareTo(TopEntitiesResult o) {
    Long thisTotal = Long.parseLong(columnValues.get("read")) + Long.parseLong(columnValues.get("write"));
    Long thatTotal = Long.parseLong(o.getColumnValues().get("read"))
      + Long.parseLong(o.getColumnValues().get("write"));
    return thatTotal.compareTo(thisTotal);
  }
<<<<<<< HEAD
<<<<<<< HEAD
=======
    private final String entityName;
    private final Map<String, Long> columnValues;
=======
    private final Map<String, String> columnValues;
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format

    public TopEntitiesResult(String entityName) {
        this.columnValues = new HashMap<>();
        columnValues.put("label", entityName);
        columnValues.put("read", "0");
        columnValues.put("write", "0");
    }

    public Map<String, String> getColumnValues() {
        return columnValues;
    }

    public void addAccessType(String type, String value) {
        this.columnValues.put(type, value);
    }

    public void formatDataByTotal() {
        columnValues.put("value",
                String.valueOf(Long.parseLong(columnValues.get("read")) + Long.parseLong(columnValues.get("write"))));
                columnValues.remove("read");
                columnValues.remove("write");
    }

    @Override
    public int compareTo(TopEntitiesResult o) {
        Long thisTotal = Long.parseLong(columnValues.get("read")) + Long.parseLong(columnValues.get("write"));
        Long thatTotal = Long.parseLong(o.getColumnValues().get("read"))
                + Long.parseLong(o.getColumnValues().get("write"));
        return thatTotal.compareTo(thisTotal);
    }
>>>>>>> 7812549... Changes to TopNEntities, Tests, Tests Data, Handler
=======
>>>>>>> 497f160... Fixed indentation
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
}
