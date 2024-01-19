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

package utils;

import com.borjav.data.service.BigQueryServiceFactory;
import com.google.api.services.bigquery.Bigquery;

public final class FakeBigQueryServiceFactory implements BigQueryServiceFactory {

  private final String[] tableSchemas;

  public FakeBigQueryServiceFactory(String[] tableSchemas) {
    this.tableSchemas = tableSchemas;
  }

  public static FakeBigQueryServiceFactory forTableSchemas(String... tableSchemas) {
    return new FakeBigQueryServiceFactory(tableSchemas);
  }

  public static BigQueryServiceFactory forStub(FakeBigquery fakeService) {
    return () -> fakeService;
  }

  @Override
  public Bigquery buildService() {
    return FakeBigquery.forTableSchemas(tableSchemas);
  }
}
