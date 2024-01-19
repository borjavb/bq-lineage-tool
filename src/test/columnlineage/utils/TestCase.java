package utils;

import com.borjav.data.output.OutputModel;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TestCase {

  @JsonProperty("query")
  public String query;
  @JsonProperty("expected_output")
  public OutputModel.Model expected_output;
}