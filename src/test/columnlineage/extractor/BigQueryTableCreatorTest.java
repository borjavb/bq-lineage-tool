/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package extractor;

import static com.borjav.data.extractor.BigQueryTableCreator.fromLegacyTableName;
import static com.borjav.data.extractor.BigQueryTableCreator.usingBestEffort;
import static org.junit.Assert.assertEquals;

import com.borjav.data.model.BigQueryTableEntity;
import org.junit.Test;


public final class BigQueryTableCreatorTest {

  @Test
  public void fromLegacyTableName_projectIdWithHyphens_valid() {
    assertEquals(fromLegacyTableName("column-lineage:temp.lineage"),
        BigQueryTableEntity.create(
            /*projectId=*/ "column-lineage",
            /*dataset=*/ "temp",
            /*table=*/ "lineage"));
  }

  @Test
  public void usingBestEffort_standardSqlName_valid() {
    assertEquals(usingBestEffort("column-lineage.temp.lineage"),
        BigQueryTableEntity.create(
            /*projectId=*/ "column-lineage",
            /*dataset=*/ "temp",
            /*table=*/ "lineage"));
  }
}
