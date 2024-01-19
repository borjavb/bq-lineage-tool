

package com.borjav.data.service;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.io.Serializable;

public interface BigQueryServiceFactory extends Serializable {

  Bigquery buildService() throws IOException;

  static BigQueryServiceFactory defaultFactory() {
    return () ->
        new Bigquery.Builder(
            new NetHttpTransport(),
            new JacksonFactory(),
            new HttpCredentialsAdapter(GoogleCredentials
                .getApplicationDefault()
                .createScoped(BigqueryScopes.all())))
            .setApplicationName("column-lineage-extraction")
            .build();
  }
}
