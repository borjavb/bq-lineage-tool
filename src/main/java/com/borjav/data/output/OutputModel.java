package com.borjav.data.output;

import com.borjav.data.model.ResolvedColumnExtended;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.zetasql.resolvedast.ResolvedJoinScanEnums;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class OutputModel {

  public static class Model {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("name")
    public String name;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("output_columns")
    public List<OutputColumn> output_columns = new ArrayList<>();
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("joins")
    public HashSet<Join> joins;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("filters")
    public HashSet<Column>  filters;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("aggregations")
    public HashSet<Column>   aggregations;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("other_used_columns")
    public HashSet<Column> other_used_columns;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("tables")
    public List<Table> tables;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("type")
    public String type;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("selected_tables")
    public HashSet<String> selected_tables = new HashSet<>();
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("error")
    public String error;


  }

  public static class Join {
    @JsonProperty("join_type")
    public String join_type;
    @JsonProperty("left_columns")
    public List<OutputColumn> left_columns = new ArrayList<>();
    @JsonProperty("right_columns")
    public List<OutputColumn> right_columns = new ArrayList<>();

  }


  public static class Table {

    @JsonProperty("name")
    public String name;
    @JsonProperty("columns")
    public HashSet<Column> columns = new HashSet<>();
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("other_used_columns")
    public HashSet<Column> other_used_columns;

  }

  public static class OutputColumn {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("name")
    public String name;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("references")
    public HashSet<Column> references;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OutputColumn that = (OutputColumn) o;
      return Objects.equals(name, that.name) && Objects.equals(references, that.references);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, references);
    }
  }

  public static class Column {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("project_name")
    public String project_name;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("dataset_name")
    public String dataset_name;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("table_name")
    public String table_name;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("name")
    public String name;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("references")
    public HashSet<Column> references;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("literal_value")
    public List<String> literal_value;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("used_for")
    public HashSet<ResolvedColumnExtended.EXTRACOLUMNS> used_for;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("join_type")
    public ResolvedJoinScanEnums.JoinType join_type;


    public void setNameSplit(String table_name, String name) {
      String[] split = table_name.split("\\.");

      if (split.length == 3) {
        this.project_name = split[split.length - 3];
        this.dataset_name = split[split.length - 2];
      }
      this.table_name = split[split.length - 1];
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Column column = (Column) o;
      return Objects.equals(table_name, column.table_name) && Objects.equals(name, column.name)
             && Objects.equals(dataset_name, column.dataset_name)
             && Objects.equals(references, column.references) && Objects.equals(literal_value,
          column.literal_value) && Objects.equals(used_for,
          column.used_for) && Objects.equals(join_type,
          column.join_type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(table_name, name, references);
    }
  }
}
