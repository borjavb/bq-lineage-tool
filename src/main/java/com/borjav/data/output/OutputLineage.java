package com.borjav.data.output;

import com.borjav.data.model.ResolvedColumnExtended;
import com.borjav.data.model.ResolvedJoinExtended;
import com.borjav.data.model.ResolvedNodeExtended;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class OutputLineage {

  public OutputModel.Model toModel(ResolvedNodeExtended table, String tableName, String error,
                                   boolean printLeafs) {
    OutputModel.Model model = new OutputModel.Model();
    model.name = tableName;
    if (error == null) {
      model.type = table.type;
      for (ResolvedColumnExtended column : table.columns) {
        if (printLeafs || !column.name.equals("_literal_")) {
          OutputModel.OutputColumn outputColumn = new OutputModel.OutputColumn();
          outputColumn.name = column.name;

          for (var leaf : getAllLeafs(column, 0, printLeafs)) {
            if (outputColumn.references == null) {
              outputColumn.references = new HashSet<>();
            }
            OutputModel.Column newColumn = new OutputModel.Column();
            newColumn.setNameSplit(leaf.tableName, leaf.name);
            if (leaf.name.equals("_literal_") && leaf.literalValue != null
                && !leaf.literalValue.equals("")) {
              newColumn.literal_value = ImmutableList.of(leaf.literalValue);
            }
            if (newColumn.used_for == null && column.usedFor != null && column.usedFor.size() > 0) {
              newColumn.used_for = new HashSet<>();
              newColumn.used_for.addAll(column.usedFor);

            }
            outputColumn.references.add(newColumn);
            if (!leaf.name.equals("_literal_")) {
              model.selected_tables.add(leaf.tableName);
            }
          }
          model.output_columns.add(outputColumn);
        }
      }

      for (ResolvedColumnExtended column : table.extra_columns) {
        if (printLeafs || !column.name.equals("_literal_")) {
          OutputModel.Column outputColumn = new OutputModel.Column();
          outputColumn.name = "_" + column.name + "_";
          for (ResolvedColumnExtended leaf : getAllLeafs(column, 0, printLeafs)) {
            if (outputColumn.references == null) {
              outputColumn.references = new HashSet<>();
            }
            OutputModel.Column newColumn = new OutputModel.Column();
            newColumn.setNameSplit(leaf.tableName, leaf.name);
            outputColumn.references.add(newColumn);
            if (leaf.name.equals("_literal_") && leaf.literalValue != null
                && !leaf.literalValue.equals("")) {
              newColumn.literal_value = ImmutableList.of(leaf.literalValue);
            }
            if (newColumn.used_for == null && column.usedFor != null && column.usedFor.size() > 0) {
              newColumn.used_for = new HashSet<>();
              newColumn.used_for.addAll(column.usedFor);

            }
            if (column.joinType != null) {
              newColumn.join_type = column.joinType;
            }
            if (!leaf.name.equals("_literal_")) {
              model.selected_tables.add(leaf.tableName);
            }
          }
          if (model.filters_groupbys_and_other_columns == null) {
            model.filters_groupbys_and_other_columns = new HashSet<>();
          }
          //we export the joins in a different struct
          if (column.joinType == null) {
            model.filters_groupbys_and_other_columns.add(outputColumn);
          }
        }
      }

      ///joins
      for (ResolvedJoinExtended join : table.joins) {
        OutputModel.Join outputJoin = new OutputModel.Join();
        outputJoin.join_type = join.join_type.name();
        if (join.join_type != ResolvedJoinExtended.JOIN_TYPE.CROSS) {
          for (ResolvedColumnExtended column : join.left) {

            OutputModel.OutputColumn outputColumn = new OutputModel.OutputColumn();
            outputColumn.name = column.name;

            for (var leaf : getAllLeafs(column, 0, printLeafs)) {
              if (outputColumn.references == null) {
                outputColumn.references = new HashSet<>();
              }
              OutputModel.Column newColumn = new OutputModel.Column();
              newColumn.setNameSplit(leaf.tableName, leaf.name);
              if (leaf.name.equals("_literal_") && leaf.literalValue != null
                  && !leaf.literalValue.equals("")) {
                newColumn.literal_value = ImmutableList.of(leaf.literalValue);
              }
              if (newColumn.used_for == null && column.usedFor != null
                  && column.usedFor.size() > 0) {
                newColumn.used_for = new HashSet<>();
                newColumn.used_for.addAll(column.usedFor);

              }
              outputColumn.references.add(newColumn);

            }
            outputJoin.left_columns.add(outputColumn);
          }
          for (ResolvedColumnExtended column : join.right) {

            OutputModel.OutputColumn outputColumn = new OutputModel.OutputColumn();
            outputColumn.name = column.name;

            for (var leaf : getAllLeafs(column, 0, printLeafs)) {
              if (outputColumn.references == null) {
                outputColumn.references = new HashSet<>();
              }
              OutputModel.Column newColumn = new OutputModel.Column();
              newColumn.setNameSplit(leaf.tableName, leaf.name);
              if (leaf.name.equals("_literal_") && leaf.literalValue != null
                  && !leaf.literalValue.equals("")) {
                newColumn.literal_value = ImmutableList.of(leaf.literalValue);
              }
              if (newColumn.used_for == null && column.usedFor != null
                  && column.usedFor.size() > 0) {
                newColumn.used_for = new HashSet<>();
                newColumn.used_for.addAll(column.usedFor);

              }
              outputColumn.references.add(newColumn);

            }
            outputJoin.right_columns.add(outputColumn);
          }
        }else{
          for (ResolvedColumnExtended column : join.left) {
            OutputModel.OutputColumn outputColumn = new OutputModel.OutputColumn();
            outputColumn.name = "_cross_join_all_columns_";
            HashSet<String> sources = new HashSet<>();
            for (var leaf : getAllLeafs(column, 0, printLeafs)) {
              sources.add(leaf.tableName);
            }
            for (String ele : sources) {
              OutputModel.Column newColumn = new OutputModel.Column();
              newColumn.setNameSplit(ele,null);
              if (outputColumn.references == null) {
                outputColumn.references = new HashSet<>();
              }
              outputColumn.references.add(newColumn);
            }
            outputJoin.left_columns.add(outputColumn);
          }
          for (ResolvedColumnExtended column : join.right) {
            OutputModel.OutputColumn outputColumn = new OutputModel.OutputColumn();
            outputColumn.name = "_cross_join_all_columns_";
            HashSet<String> sources = new HashSet<>();
            for (var leaf : getAllLeafs(column, 0, printLeafs)) {
              sources.add(leaf.tableName);
            }
            for (String ele : sources) {
              OutputModel.Column newColumn = new OutputModel.Column();
              newColumn.setNameSplit(ele,null);
              if (outputColumn.references == null) {
                outputColumn.references = new HashSet<>();
              }
              outputColumn.references.add(newColumn);
            }
            outputJoin.right_columns.add(outputColumn);
          }

        }

        if (model.joins == null) {
          model.joins = new HashSet<>();
        }
        model.joins.add(outputJoin);
      }

    } else {
      model = new OutputModel.Model();
      model.name = tableName;
      model.error = error;
    }

    return model;
  }


  public String toYaml(ResolvedNodeExtended table, String tableName, boolean printLeafs) {
    YAMLFactory yf = new YAMLFactory();
    ObjectMapper mapper = new ObjectMapper(yf);
    try {
      return mapper.writerWithDefaultPrettyPrinter()
          .writeValueAsString(toModel(table, tableName, null, printLeafs));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public String toYaml(OutputModel.Model model)
      throws JsonProcessingException {
    YAMLFactory yf = new YAMLFactory();
    ObjectMapper mapper = new ObjectMapper(yf);
    return mapper.writerWithDefaultPrettyPrinter()
        .writeValueAsString(model);
  }

  public String toJson(ResolvedNodeExtended table, String tableName, boolean printLeafs)
      throws JsonProcessingException {

    ObjectMapper mapper = new ObjectMapper();
    return mapper.writerWithDefaultPrettyPrinter()
        .writeValueAsString(toModel(table, tableName, null, printLeafs));
  }


  public static List<ResolvedColumnExtended> getAllLeafs(ResolvedColumnExtended currentObject,
                                                         int depth, boolean printLeafs) {
    if (depth > 2000) {
      currentObject.columnsReferenced = new ArrayList<>();
    }
    //get all child-URIs for the category
    List<ResolvedColumnExtended> objectChildren = currentObject.columnsReferenced;
    List<ResolvedColumnExtended> leaves = new ArrayList<>();

    if (objectChildren.size() == 0) {
      if (printLeafs || (!currentObject.tableName.equals("_literal_")
                         && !currentObject.tableName.equals(
          "$array_offset"))) {
        if (!currentObject.tableName.equals("_literal_") || (currentObject.literalValue != null
                                                             && !currentObject.literalValue.equals(
            ""))) {
          leaves.add(currentObject);
        }
      }
    } else {
      for (ResolvedColumnExtended childObj : objectChildren) {

        if (childObj != null) {

          if (childObj.columnsReferenced.size() > 0) {
            leaves.addAll(getAllLeafs(childObj, depth + 1, printLeafs));
          } else {
            if (printLeafs || (!childObj.tableName.equals("_literal_")
                               && !childObj.tableName.equals("$array_offset"))) {
              if (!childObj.tableName.equals("_literal_") || (childObj.literalValue != null
                                                              && !childObj.literalValue.equals(
                  ""))) {
                leaves.add(childObj);
              }
            }
          }
        }
      }
    }
    return leaves;
  }
}
