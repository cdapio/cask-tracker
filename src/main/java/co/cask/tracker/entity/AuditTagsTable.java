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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.Id;
import co.cask.tracker.utils.DiscoveryMetadataClient;

import com.google.common.base.CharMatcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 *  Table dataset holding the preferred and the user tags
 */
public final class AuditTagsTable extends AbstractDataset {

  private final Table preferredTagsTable;
  //  private final Table userTagsTable;
  private static final byte[] TOTAL_ENTITIES = Bytes.toBytes("total_entities");
  private static final byte[] DEFAULT_TOTAL_ENTITIES = Bytes.toBytes(0);
  private static final int MAX_TAG_LENGTH = 50;
  private final DiscoveryMetadataClient disClient = new DiscoveryMetadataClient();

  private static final CharMatcher TAG_MATCHER = CharMatcher.inRange('A', 'Z')
    .or(CharMatcher.inRange('a', 'z'))
    .or(CharMatcher.inRange('0', '9'))
    .or(CharMatcher.is('_'))
    .or(CharMatcher.is('-'));

  public AuditTagsTable(DatasetSpecification spec, @EmbeddedDataset("preferredTagsTable") Table preferredTagsTable,
                        @EmbeddedDataset("userTagsTable") Table userTagsTable) {
    super(spec.getName(), preferredTagsTable, userTagsTable);
    this.preferredTagsTable = preferredTagsTable;
//    this.userTagsTable = userTagsTable;
  }

  public TagsResult getUserTags(String prefix, Id.Namespace namespace) throws IOException, UnauthenticatedException,
    NotFoundException, BadRequestException {
    Map<String, Integer> tagMap = new HashMap<>();
    Set<String> userSet = disClient.getTags(namespace);
    for (String usertag : userSet) {
      if (preferredTagsTable.get(usertag.getBytes()).isEmpty()) {
        if (usertag.toLowerCase().startsWith(prefix.toLowerCase())) {
          tagMap.put(usertag, disClient.getEntityNum(usertag, namespace));
        }
      }
    }
    TagsResult result = new TagsResult();
    result.setUser(tagMap.size());
    result.setUserTags(tagMap);
    return result;
  }

  public TagsResult getPreferredTags(String prefix, Id.Namespace namespace) throws IOException, NotFoundException,
    UnauthenticatedException, BadRequestException {
    Map<String, Integer> tagMap = new HashMap<>();
    Scanner scanner = preferredTagsTable.scan(null, null);
    try {
      Row row;
      while ((row = scanner.next()) != null) {
        String tag = Bytes.toString(row.getRow());
        if (tag.toLowerCase().startsWith(prefix.toLowerCase())) {
//          tagMap.put(tag, row.getInt(TOTAL_ENTITIES));
          tagMap.put(tag, disClient.getEntityNum(tag, namespace));
        }
      }
    } finally {
      scanner.close();
    }
    TagsResult result = new TagsResult();
    result.setPreferred(tagMap.size());
    result.setPreferredTags(tagMap);
    return result;
  }


  public TagsResult getTags(String prefix, Id.Namespace namespace) throws IOException, NotFoundException,
    UnauthenticatedException, BadRequestException {
    TagsResult userResult = getUserTags(prefix, namespace);
    TagsResult preferredResult = getPreferredTags(prefix, namespace);
    preferredResult.setUser(userResult.getUser());
    preferredResult.setUserTags(userResult.getUserTags());
    return preferredResult;
  }

  public ValidateTagsResult demoteTag(List<String> tagList) {
    List<String> valid = new LinkedList<>();
    List<String> invalid = new LinkedList<>();
    for (String tag : tagList) {
      Row row = preferredTagsTable.get(tag.getBytes());
      if (!row.isEmpty()) {
        preferredTagsTable.delete(tag.getBytes());
        valid.add(tag);
      } else {
        invalid.add(tag);
      }
    }
    return new ValidateTagsResult(valid.size(), invalid.size(), valid, invalid);
  }

  public boolean deleteTag(String tag) {
    if (!preferredTagsTable.get(tag.getBytes()).isEmpty()) {
      preferredTagsTable.delete(tag.getBytes());
      return true;
    }
    return false;
  }

  public ValidateTagsResult addPreferredTags(List<String> tagList) {
    List<String> valid = new LinkedList<>();
    List<String> invalid = new LinkedList<>();
    for (String tag : tagList) {
      Row row = preferredTagsTable.get(tag.getBytes());
      if (row.isEmpty() && isValid(tag)) {
        valid.add(tag);
        preferredTagsTable.put(tag.getBytes(), TOTAL_ENTITIES, DEFAULT_TOTAL_ENTITIES);
      }
      else {
        invalid.add(tag);
      }
    }
    return new ValidateTagsResult(valid.size(), invalid.size(), valid, invalid);
  }

//  public void addPreferredTags(List<String> tagList) {
//    for (String tag : tagList) {
//
//      preferredTagsTable.put(tag.getBytes(), TOTAL_ENTITIES, DEFAULT_TOTAL_ENTITIES);
//    }
//  }

  public ValidateTagsResult validateTags (List<String> tagList) {
    List<String> validList = new LinkedList<>();
    List<String> invalidList = new LinkedList<>();
    int validCount = 0;
    int invalidCount = 0;

    for (String tag : tagList) {
      if (isValid(tag)) {
        validList.add(tag);
        validCount += 1;
      } else {
        invalidList.add(tag);
        invalidCount += 1;
      }
    }
    return new ValidateTagsResult(validCount, invalidCount, validList, invalidList);
  }

  private boolean isValid(String tag) {
    if (!TAG_MATCHER.matchesAllOf(tag) || tag.length() > MAX_TAG_LENGTH) {
      return false;
    }
    return true;
  }
}
