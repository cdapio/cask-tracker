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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *  A POJO to hold the result of the get tags request
 */
public final class TagsResult {

  private int preferred;
  private int user;
  private Map<String, Integer> preferredTags;
  private Map<String, Integer> userTags;

  public TagsResult(int preferred, Map<String, Integer> preferredTags, int user, Map<String, Integer> userTags) {
    this.preferred = preferred;
    this.preferredTags = preferredTags;
    this.user = user;
    this.userTags = userTags;
  }

  public TagsResult() {
    this.preferred = 0;
    this.preferredTags = new HashMap<>();
    this.user = 0;
    this.userTags = new HashMap<>();
  }

  public void setPreferred(int preferred) {
    this.preferred = preferred;
  }

  public void setUser(int user) {
    this.user = user;
  }

  public void setPreferredTags(Map<String, Integer> preferredTags) {
    this.preferredTags = sortByComparator(preferredTags);
  }

  public void setUserTags(Map<String, Integer> userTags) {
    this.userTags = sortByComparator(userTags);
  }

  public int getPreferred() {
    return preferred;
  }

  public int getUser() {
    return user;
  }

  public Map<String, Integer> getPreferredTags() {
    return preferredTags;
  }

  public Map<String, Integer> getUserTags() {
    return userTags;
  }

  private static Map<String, Integer> sortByComparator(Map<String, Integer> unsortedMap) {
    List<Entry<String, Integer>> list = new LinkedList<>(unsortedMap.entrySet());
    Collections.sort(list, new Comparator<Entry<String, Integer>>() {
      @Override
      public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
        int comp = o2.getValue().compareTo(o1.getValue());
        if (comp == 0) {
          return o1.getKey().toLowerCase().compareTo(o2.getKey().toLowerCase());
        }
        return comp;
      }
    });

    Map<String, Integer> sortedMap = new LinkedHashMap<>();
    for (Entry<String, Integer> entry : list) {
      sortedMap.put(entry.getKey(), entry.getValue());
    }
    return sortedMap;
  }
}
