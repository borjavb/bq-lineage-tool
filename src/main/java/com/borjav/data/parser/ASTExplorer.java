package com.borjav.data.parser;


import static com.borjav.data.utils.UtilsParser.getLiteral;

import com.borjav.data.model.ResolvedColumnExtended;
import com.borjav.data.model.ResolvedJoinExtended;
import com.borjav.data.model.ResolvedNodeExtended;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.ArrayType;
import com.google.zetasql.Function;
import com.google.zetasql.FunctionSignature;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.StructType;
import com.google.zetasql.ZetaSQLFunctions;
import com.google.zetasql.ZetaSQLType;
import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedJoinScanEnums;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SQL parser to identify lineage relations using ZetaSQL engine.
 */
public class ASTExplorer {

  private final HashMap<String, ResolvedNodeExtended> allTables = new HashMap<>();
  private final SimpleCatalog catalog;

  private final AtomicInteger anonymFieldCounter = new AtomicInteger(0);
  private final List<ResolvedJoinExtended> joins = new ArrayList<>();

  public ASTExplorer(SimpleCatalog catalog) {
    this.catalog = catalog;
  }

  private ResolvedNodeExtended checkNodeType(ResolvedNode node) {
    return switch (node.nodeKind()) {
      case RESOLVED_TABLE_SCAN -> resolveTableScan((ResolvedNodes.ResolvedTableScan) node);
      case RESOLVED_PROJECT_SCAN -> resolveProjectScan((ResolvedNodes.ResolvedProjectScan) node);
      case RESOLVED_ANALYTIC_SCAN ->
          resolvedAnalyticScan((ResolvedNodes.ResolvedAnalyticScan) node);
      case RESOLVED_ANALYTIC_FUNCTION_GROUP ->
          resolvedAnalyticFunctionGroup((ResolvedNodes.ResolvedAnalyticFunctionGroup) node);
      case RESOLVED_FILTER_SCAN -> resolvedFilterScan((ResolvedNodes.ResolvedFilterScan) node);
      case RESOLVED_WINDOW_PARTITIONING ->
          resolvedWindowPartitioning((ResolvedNodes.ResolvedWindowPartitioning) node);
      case RESOLVED_WINDOW_ORDERING ->
          resolvedWindowOrdering((ResolvedNodes.ResolvedWindowOrdering) node);
      case RESOLVED_WITH_REF_SCAN -> resolvedWithRefScan((ResolvedNodes.ResolvedWithRefScan) node);
      case RESOLVED_JOIN_SCAN -> resolvedJoin((ResolvedNodes.ResolvedJoinScan) node);
      case RESOLVED_SET_OPERATION_SCAN ->
          resolvedUnion((ResolvedNodes.ResolvedSetOperationScan) node);
      case RESOLVED_AGGREGATE_SCAN -> resolvedAggregate((ResolvedNodes.ResolvedAggregateScan) node);
      case RESOLVED_COLUMN_REF -> resolvedColumnRef((ResolvedNodes.ResolvedColumnRef) node);
      case RESOLVED_AGGREGATE_FUNCTION_CALL ->
          resolvedAggregateFunctionCall((ResolvedNodes.ResolvedAggregateFunctionCall) node);
      case RESOLVED_FUNCTION_CALL ->
          resolvedFunctionCall((ResolvedNodes.ResolvedFunctionCall) node);
      case RESOLVED_COMPUTED_COLUMN ->
          resolvedComputedColumn((ResolvedNodes.ResolvedComputedColumn) node);
      case RESOLVED_CAST -> resolvedCast((ResolvedNodes.ResolvedCast) node);
      case RESOLVED_WITH_SCAN -> resolvedWithScan((ResolvedNodes.ResolvedWithScan) node);
      case RESOLVED_WITH_ENTRY -> resolvedWithEntry((ResolvedNodes.ResolvedWithEntry) node);
      case RESOLVED_LITERAL -> resolvedLiteral((ResolvedNodes.ResolvedLiteral) node);
      case RESOLVED_ORDER_BY_SCAN -> resolvedOrderByScan((ResolvedNodes.ResolvedOrderByScan) node);
      case RESOLVED_ORDER_BY_ITEM ->
          resolvedOrderByScanItem((ResolvedNodes.ResolvedOrderByItem) node);
      case RESOLVED_LIMIT_OFFSET_SCAN ->
          resolvedLimitOffset((ResolvedNodes.ResolvedLimitOffsetScan) node);
      case RESOLVED_ARRAY_SCAN -> resolvedArrayScan((ResolvedNodes.ResolvedArrayScan) node);
      case RESOLVED_MAKE_STRUCT -> resolvedMakeStruct((ResolvedNodes.ResolvedMakeStruct) node);
      case RESOLVED_GET_STRUCT_FIELD ->
          resolvedGetStruct((ResolvedNodes.ResolvedGetStructField) node);
      case RESOLVED_ANALYTIC_FUNCTION_CALL ->
          resolvedAnalyticFunctionCall((ResolvedNodes.ResolvedAnalyticFunctionCall) node);
      case RESOLVED_SUBQUERY_EXPR -> resolvedSubqueryExp((ResolvedNodes.ResolvedSubqueryExpr) node);
      case RESOLVED_QUERY_STMT -> ResolvedQueryStmt((ResolvedNodes.ResolvedQueryStmt) node);
      case RESOLVED_CREATE_TABLE_AS_SELECT_STMT -> ResolvedCreateTableAsSelectStmt(
          (ResolvedNodes.ResolvedCreateTableAsSelectStmt) node);
      case RESOLVED_MERGE_STMT -> ResolvedMergeStmt((ResolvedNodes.ResolvedMergeStmt) node);
      case RESOLVED_CREATE_VIEW_STMT ->
          ResolvedCreateViewStmt((ResolvedNodes.ResolvedCreateViewStmt) node);
      case RESOLVED_CREATE_FUNCTION_STMT ->
          resolveFunctions((ResolvedNodes.ResolvedCreateFunctionStmt) node);
      case RESOLVED_CREATE_EXTERNAL_TABLE_STMT -> ResolvedCreateExternalTableAsSelectStmt(
          (ResolvedNodes.ResolvedCreateExternalTableStmt) node);
      default -> new ResolvedNodeExtended();
    };
  }

  public ResolvedNodeExtended resolve(ResolvedStatement stmt) {
    ResolvedNodeExtended table = checkNodeType(stmt);
    table.joins = joins;
    return simplifyTable(table);
  }

  public void pruneStructedFields(ResolvedColumnExtended root) {
    if (!root.type.isStruct()) {
      findParent(root, root.originalIndexStructList);
    }
  }


  private ResolvedNodeExtended ResolvedQueryStmt(ResolvedNodes.ResolvedQueryStmt stmt) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();
    stmt.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedQueryStmt node) {
        List<ResolvedColumnExtended> withColumns = new ArrayList<>();
        ResolvedNodeExtended inputTable = checkNodeType(node.getQuery());
        for (ResolvedNodes.ResolvedOutputColumn column : node.getOutputColumnList()) {
          for (ResolvedColumnExtended sourceColumn : inputTable.columns) {
            if (Long.valueOf(column.getColumn().getId()).longValue()
                == sourceColumn.resolvedIndex.longValue()) {
              if (column.getColumn().getType().getKind() == ZetaSQLType.TypeKind.TYPE_STRUCT) {
                List<ResolvedColumnExtended> unnestedColumns =
                    expandStructInput(column.getColumn().getType().asStruct(),
                        column.getColumn(),
                        0L,
                        column.getName());
                for (ResolvedColumnExtended unnestedColumn : unnestedColumns) {
                  ResolvedColumnExtended newDependencies = deepCloner(sourceColumn);
                  unnestedColumn.columnsReferenced = newDependencies.columnsReferenced;
                  pruneStructedFields(unnestedColumn);
                  withColumns.add(unnestedColumn);
                }
              } else if (column.getColumn().getType().getKind()
                         == ZetaSQLType.TypeKind.TYPE_ARRAY) {
                List<ResolvedColumnExtended> unnestedColumns =
                    expandStructInput(column.getColumn().getType().asArray(),
                        column.getColumn(),
                        0L,
                        column.getName(), 0L);
                for (ResolvedColumnExtended unnestedColumn : unnestedColumns) {
                  ResolvedColumnExtended outputColumn =
                      new ResolvedColumnExtended(unnestedColumn.name,
                          column.getColumn().getTableName(),
                          column.getColumn().getId(),
                          (long) node.getOutputColumnList().indexOf(column),
                          column.getColumn().getType(),
                          0L, new HashSet<>(), null);
                  sourceColumn.indexStructList = unnestedColumn.indexStructList;
                  sourceColumn.originalIndexStructList = unnestedColumn.originalIndexStructList;
                  sourceColumn.makeStructIndex = unnestedColumn.makeStructIndex;
                  ResolvedColumnExtended newDependencies = deepCloner(sourceColumn);
                  pruneStructedFields(newDependencies);
                  outputColumn.columnsReferenced.add(newDependencies);
                  withColumns.add(outputColumn);
                }
              } else {
                ResolvedColumnExtended outputColumn =
                    new ResolvedColumnExtended(column.getName(),
                        column.getColumn().getTableName(),
                        column.getColumn().getId(),
                        (long) node.getOutputColumnList().indexOf(column),
                        column.getColumn().getType(),
                        0L,new HashSet<>(), null);
                outputColumn.columnsReferenced.add(deepCloner(sourceColumn));
                withColumns.add(outputColumn);
              }
            }
          }
        }
        table.extra_columns.addAll(inputTable.extra_columns);
        table.columns = withColumns;
        table.type = "select";
      }
    });
    return simplifyTable(table);
  }

  private ResolvedNodeExtended ResolvedCreateExternalTableAsSelectStmt(
      ResolvedNodes.ResolvedCreateExternalTableStmt stmt) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();
    stmt.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedCreateExternalTableStmt node) {
        //Not supported yet - there's no really column creation?
      }
    });
    return simplifyTable(table);
  }

  private ResolvedNodeExtended ResolvedCreateTableAsSelectStmt(
      ResolvedNodes.ResolvedCreateTableAsSelectStmt stmt) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();
    stmt.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedCreateTableAsSelectStmt node) {
        List<ResolvedColumnExtended> withColumns = new ArrayList<>();
        ResolvedNodeExtended inputTable = checkNodeType(node.getQuery());
        for (ResolvedNodes.ResolvedOutputColumn column : node.getOutputColumnList()) {
          for (ResolvedColumnExtended sourceColumn : inputTable.columns) {
            if (Long.valueOf(column.getColumn().getId()).longValue()
                == sourceColumn.resolvedIndex.longValue()) {
              if (column.getColumn().getType().getKind() == ZetaSQLType.TypeKind.TYPE_STRUCT) {
                List<ResolvedColumnExtended> unnestedColumns =
                    expandStructInput(column.getColumn().getType().asStruct(),
                        column.getColumn(),
                        0L,
                        column.getName());
                for (ResolvedColumnExtended unnestedColumn : unnestedColumns) {
                  ResolvedColumnExtended newDependencies = deepCloner(sourceColumn);
                  unnestedColumn.columnsReferenced = newDependencies.columnsReferenced;
                  pruneStructedFields(unnestedColumn);
                  withColumns.add(unnestedColumn);
                }
              } else if (column.getColumn().getType().getKind()
                         == ZetaSQLType.TypeKind.TYPE_ARRAY) {
                List<ResolvedColumnExtended> unnestedColumns =
                    expandStructInput(column.getColumn().getType().asArray(),
                        column.getColumn(),
                        0L,
                        column.getName(), 0L);
                for (ResolvedColumnExtended unnestedColumn : unnestedColumns) {
                  ResolvedColumnExtended outputColumn =
                      new ResolvedColumnExtended(unnestedColumn.name,
                          column.getColumn().getTableName(),
                          column.getColumn().getId(),
                          (long) node.getOutputColumnList().indexOf(column),
                          column.getColumn().getType(),
                          0L,new HashSet<>(), null);
                  sourceColumn.indexStructList = unnestedColumn.indexStructList;
                  sourceColumn.originalIndexStructList = unnestedColumn.originalIndexStructList;
                  sourceColumn.makeStructIndex = unnestedColumn.makeStructIndex;
                  ResolvedColumnExtended newDependencies = deepCloner(sourceColumn);
                  pruneStructedFields(newDependencies);
                  outputColumn.columnsReferenced.add(newDependencies);
                  withColumns.add(outputColumn);
                }
              } else {
                ResolvedColumnExtended outputColumn =
                    new ResolvedColumnExtended(column.getName(),
                        column.getColumn().getTableName(),
                        column.getColumn().getId(),
                        (long) node.getOutputColumnList().indexOf(column),
                        column.getColumn().getType(),
                        0L,new HashSet<>(), null);
                outputColumn.columnsReferenced.add(deepCloner(sourceColumn));
                withColumns.add(outputColumn);
              }
            }
          }
        }
        table.extra_columns.addAll(inputTable.extra_columns);
        table.columns = withColumns;
        table.type = "create";
        table.table_name = node.getNamePath().get(0);
      }
    });
    return simplifyTable(table);
  }

  private ResolvedNodeExtended ResolvedCreateViewStmt(ResolvedNodes.ResolvedCreateViewStmt stmt) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();
    stmt.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedCreateViewStmt node) {
        List<ResolvedColumnExtended> withColumns = new ArrayList<>();
        ResolvedNodeExtended inputTable = checkNodeType(node.getQuery());
        for (ResolvedNodes.ResolvedOutputColumn column : node.getOutputColumnList()) {
          for (ResolvedColumnExtended sourceColumn : inputTable.columns) {
            if (column.getColumn().getType().getKind() == ZetaSQLType.TypeKind.TYPE_STRUCT) {
              List<ResolvedColumnExtended> unnestedColumns =
                  expandStructInput(column.getColumn().getType().asStruct(),
                      column.getColumn(),
                      0L,
                      column.getName());
              for (ResolvedColumnExtended unnestedColumn : unnestedColumns) {
                ResolvedColumnExtended newDependencies = deepCloner(sourceColumn);
                unnestedColumn.columnsReferenced = newDependencies.columnsReferenced;
                pruneStructedFields(unnestedColumn);
                withColumns.add(unnestedColumn);
              }
            } else if (column.getColumn().getType().getKind()
                       == ZetaSQLType.TypeKind.TYPE_ARRAY) {
              List<ResolvedColumnExtended> unnestedColumns =
                  expandStructInput(column.getColumn().getType().asArray(),
                      column.getColumn(),
                      0L,
                      column.getName(), 0L);
              for (ResolvedColumnExtended unnestedColumn : unnestedColumns) {
                ResolvedColumnExtended outputColumn =
                    new ResolvedColumnExtended(unnestedColumn.name,
                        column.getColumn().getTableName(),
                        column.getColumn().getId(),
                        (long) node.getOutputColumnList().indexOf(column),
                        column.getColumn().getType(),
                        0L, new HashSet<>(), null);
                sourceColumn.indexStructList = unnestedColumn.indexStructList;
                sourceColumn.originalIndexStructList = unnestedColumn.originalIndexStructList;
                sourceColumn.makeStructIndex = unnestedColumn.makeStructIndex;
                ResolvedColumnExtended newDependencies = deepCloner(sourceColumn);
                pruneStructedFields(newDependencies);
                outputColumn.columnsReferenced.add(newDependencies);
                withColumns.add(outputColumn);
              }
            } else {
              ResolvedColumnExtended outputColumn =
                  new ResolvedColumnExtended(column.getName(),
                      column.getColumn().getTableName(),
                      column.getColumn().getId(),
                      (long) node.getOutputColumnList().indexOf(column),
                      column.getColumn().getType(),
                      0L,
                      new HashSet<>(), null);
              outputColumn.columnsReferenced.add(deepCloner(sourceColumn));
              withColumns.add(outputColumn);
            }
          }
        }
        table.extra_columns.addAll(inputTable.extra_columns);
        table.columns = withColumns;
        table.type = "create";
        table.table_name = node.getNamePath().get(0);
      }
    });
    return simplifyTable(table);
  }

  private ResolvedNodeExtended ResolvedMergeStmt(ResolvedNodes.ResolvedMergeStmt stmt) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();
    stmt.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedMergeStmt node) {
        ResolvedNodeExtended inputTable = checkNodeType(node.getFromScan());
        table.extra_columns.addAll(inputTable.extra_columns);
        table.columns = inputTable.columns;
        table.type = "merge";
        table.table_name = node.getTableScan().getTable().getName();
      }
    });
    return simplifyTable(table);
  }


  private ResolvedNodeExtended resolvedWithScan(ResolvedNodes.ResolvedWithScan with) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();
    with.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedWithScan node) {
        List<ResolvedNodeExtended> tables = new ArrayList<>();
        for (ResolvedNodes.ResolvedWithEntry entry : node.getWithEntryList()) {
          tables.add(checkNodeType(entry));
        }
        ResolvedNodeExtended finalTable = checkNodeType(node.getQuery());
        table.columns = finalTable.columns;
        table.extra_columns.addAll(finalTable.extra_columns);
        for (ResolvedNodeExtended sourceTable : tables) {
          table.extra_columns.addAll(sourceTable.extra_columns);
        }
        for (ResolvedColumnExtended column : table.columns) {
          connectAllTables(column);
        }
        for (ResolvedColumnExtended column : table.extra_columns) {
          connectAllTables(column);
        }
        for (ResolvedJoinExtended join: joins) {
          for (ResolvedColumnExtended column : join.left ) {
            connectAllTables(column);
          }
          for (ResolvedColumnExtended column : join.right ) {
            connectAllTables(column);
          }
        }
      }
    });
    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedWithEntry(ResolvedNodes.ResolvedWithEntry with) {
//        System.out.println(with.getWithQueryName());
    ResolvedNodeExtended table = new ResolvedNodeExtended();
    List<ResolvedColumnExtended> withColumns = new ArrayList<>();
    with.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedWithEntry node) {
        ResolvedNodeExtended sourceTable = checkNodeType(node.getWithSubquery());
        int index = 0;
        for (ResolvedColumn resolvedColumn : node.getWithSubquery().getColumnList()) {
          ResolvedColumnExtended scanColumn =
              new ResolvedColumnExtended(resolvedColumn, (long) index++);
          for (ResolvedColumnExtended sourceColumn : sourceTable.columns) {
            if (scanColumn.resolvedIndex.longValue() == sourceColumn.resolvedIndex.longValue()) {
              scanColumn.columnsReferenced.add(sourceColumn);
            }
          }
          withColumns.add(scanColumn);
        }
        table.columns = withColumns;
        table.originalColumns = node.getWithSubquery().getColumnList();
        table.name = node.getWithQueryName();
        table.extra_columns.addAll(sourceTable.extra_columns);
        allTables.put(table.name, table);
        for (ResolvedColumnExtended column : table.columns) {
          connectAllTables(column);
        }
        for (ResolvedColumnExtended column : table.extra_columns) {
          connectAllTables(column);
        }
        for (ResolvedJoinExtended join: joins) {
          for (ResolvedColumnExtended column : join.left ) {
            connectAllTables(column);
          }
          for (ResolvedColumnExtended column : join.right ) {
            connectAllTables(column);
          }
        }
      }
    });

    return simplifyTable(table);
  }


  private ResolvedNodeExtended resolveProjectScan(ResolvedNodes.ResolvedScan projectScan) {
    List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
    ResolvedNodeExtended table = new ResolvedNodeExtended();
    projectScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedProjectScan node) {
        ResolvedNodeExtended sourceTable = checkNodeType(node.getInputScan());
        List<ResolvedNodeExtended> tables = new ArrayList<>();

        if (sourceTable != null) {
          if (sourceTable.extra_columns != null
              && sourceTable.extra_columns.size() > 0) {
            table.extra_columns.addAll(sourceTable.extra_columns);
          }

          for (ResolvedNodes.ResolvedComputedColumn column : node.getExprList()) {
            tables.add(checkNodeType(column));
          }

          int index = 0;
          for (ResolvedColumn resolvedColumn : node.getColumnList()) {
            ResolvedColumnExtended scanColumn =
                new ResolvedColumnExtended(resolvedColumn, (long) index++);
            for (ResolvedColumnExtended sourceColumn : sourceTable.columns) {
              if (scanColumn.resolvedIndex.longValue() == sourceColumn.resolvedIndex.longValue()) {
                scanColumn.columnsReferenced.add(sourceColumn);
              }
            }
            scanColumns.add(scanColumn);
          }

          for (ResolvedNodeExtended tableInput : tables) {
            for (ResolvedColumnExtended sourceColumn : tableInput.columns) {
              connectChildrenRefs(sourceColumn, sourceTable.columns);
            }
          }
        }
        for (ResolvedColumnExtended column : scanColumns) {
          for (ResolvedNodeExtended table : tables) {
            for (ResolvedColumnExtended sourceColumn : table.columns) {
              if (column.resolvedIndex.longValue() == sourceColumn.resolvedIndex.longValue()) {
                column.columnsReferenced.add(sourceColumn);

              }
            }
          }
        }

        table.columns = scanColumns;
      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedAnalyticScan(ResolvedNodes.ResolvedScan analyticScan) {
    List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
    ResolvedNodeExtended table = new ResolvedNodeExtended();
    analyticScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedAnalyticScan node) {
        ResolvedNodeExtended sourceTable = checkNodeType(node.getInputScan());

        if (sourceTable.extra_columns != null && sourceTable.extra_columns.size() > 0) {
          table.extra_columns.addAll(sourceTable.extra_columns);
        }
        int index = 0;
        for (ResolvedColumn resolvedColumn : node.getColumnList()) {
          ResolvedColumnExtended scanColumn =
              new ResolvedColumnExtended(resolvedColumn, (long) index++);
          for (ResolvedColumnExtended sourceColumn : sourceTable.columns) {
            if (scanColumn.resolvedIndex.longValue() == sourceColumn.resolvedIndex.longValue()) {
              scanColumn.columnsReferenced.add(sourceColumn);
            }
          }
          scanColumns.add(scanColumn);
        }

        for (ResolvedNodes.ResolvedAnalyticFunctionGroup group : node.getFunctionGroupList()) {
          ResolvedNodeExtended groupByyTable = checkNodeType(group);
          for (ResolvedColumnExtended sourceColumn : groupByyTable.columns) {
            connectChildrenRefs(sourceColumn, sourceTable.columns);
          }
          for (ResolvedColumnExtended resolvedColumn : scanColumns) {
            for (ResolvedColumnExtended sourceColumn : groupByyTable.columns) {
              if (resolvedColumn.resolvedIndex.longValue()
                  == sourceColumn.resolvedIndex.longValue()) {
                resolvedColumn.columnsReferenced.add(sourceColumn);
              }
            }
          }
        }

        for (ResolvedColumnExtended column : scanColumns) {
          updateChildrenRefs(column, node.getColumnList());
        }
        table.columns = scanColumns;
      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolveTableScan(ResolvedNodes.ResolvedScan tableScan) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();
    tableScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedTableScan node) {
        List<ResolvedColumnExtended> sourceColumns = new ArrayList<>();
        Long index = 0L; //we should make struct increment here too
        for (ResolvedColumn resolvedColumn : node.getColumnList()) {
          if (resolvedColumn.getType().isStruct()) {
            final List<ResolvedColumnExtended> resolvedColumnExtendeds =
                expandStructInput(resolvedColumn.getType().asStruct(),
                    resolvedColumn,
                    index,
                    resolvedColumn.getName());
            sourceColumns.addAll(resolvedColumnExtendeds);
          } else if (resolvedColumn.getType().isArray()) {
            final List<ResolvedColumnExtended> refColumns =
                expandStructInput(resolvedColumn.getType().asArray(), resolvedColumn, index,
                    resolvedColumn.getName(), 0L);
            sourceColumns.addAll(refColumns);

            //probAdd here too
          } else {
            ResolvedColumnExtended newColumn = new ResolvedColumnExtended(resolvedColumn, index);
            newColumn.originalIndexStructList.add(index);
            newColumn.indexStructList.add(index);
            newColumn.makeStructIndex.add(index);
            sourceColumns.add(newColumn);
          }
          index++;
        }
        table.name = node.getTable().getName();
        table.columns = sourceColumns;
      }
    });

    return simplifyTable(table);
  }


  private ResolvedNodeExtended resolvedAnalyticFunctionGroup(
      ResolvedNodes.ResolvedAnalyticFunctionGroup projectScan) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();

    projectScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedAnalyticFunctionGroup node) {

        for (ResolvedNodes.ResolvedComputedColumn sourceColumn : node.getAnalyticFunctionList()) {
          ResolvedColumnExtended column =
              new ResolvedColumnExtended(sourceColumn.getColumn(), null);
          column.columnsReferenced.addAll(checkNodeType(sourceColumn).columns);

          if (node.getOrderBy() != null) {
            ResolvedNodeExtended orderByColumns = checkNodeType(node.getOrderBy());
            for (ResolvedColumnExtended resolvedColumn : orderByColumns.columns) {
              resolvedColumn.usedFor.add(ResolvedColumnExtended.EXTRACOLUMNS.PARTITION_BY_ANALYTIC_FUNCTION);
            }
            column.columnsReferenced.addAll(orderByColumns.columns);
          } else {
            column.columnsReferenced.add(
                new ResolvedColumnExtended("_literal_", "_literal_", -1L, -1L, null));
          }
          if (node.getPartitionBy() != null) {
            ResolvedNodeExtended partitionByColumns = checkNodeType(node.getPartitionBy());
            for (ResolvedColumnExtended resolvedColumn : partitionByColumns.columns) {
              resolvedColumn.usedFor.add(ResolvedColumnExtended.EXTRACOLUMNS.PARTITION_BY_ANALYTIC_FUNCTION);
            }
            column.columnsReferenced.addAll(partitionByColumns.columns);
          } else {
            column.columnsReferenced.add(
                new ResolvedColumnExtended("_literal_", "_literal_", -1L, -1L, null));
          }
          table.columns.add(column);
        }
      }

    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedWindowPartitioning(
      ResolvedNodes.ResolvedWindowPartitioning projectScan) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();

    projectScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedWindowPartitioning node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        node.getPartitionByList().forEach(
            column -> scanColumns.add(new ResolvedColumnExtended(column.getColumn(), null)));
        table.columns = scanColumns;
      }

    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedWindowOrdering(
      ResolvedNodes.ResolvedWindowOrdering projectScan) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();

    projectScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedWindowOrdering node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        node.getOrderByItemList().forEach(column -> scanColumns.add(
            new ResolvedColumnExtended(column.getColumnRef().getColumn(), null)));
        table.columns = scanColumns;
      }

    });

    return simplifyTable(table);
  }


  private ResolvedNodeExtended resolvedLimitOffset(
      ResolvedNodes.ResolvedLimitOffsetScan projectScan) {
    List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
    ResolvedNodeExtended table = new ResolvedNodeExtended();

    projectScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedLimitOffsetScan node) {
        ResolvedNodeExtended sourceTable = checkNodeType(node.getInputScan());
        int index = 0;
        for (ResolvedColumn resolvedColumn : node.getColumnList()) {
          ResolvedColumnExtended scanColumn =
              new ResolvedColumnExtended(resolvedColumn, (long) index++);
          for (ResolvedColumnExtended sourceColumn : sourceTable.columns) {
            if (scanColumn.resolvedIndex.longValue() == sourceColumn.resolvedIndex.longValue()) {
              scanColumn.columnsReferenced.add(sourceColumn);
            }
          }
          scanColumns.add(scanColumn);
        }
        table.extra_columns.addAll(sourceTable.extra_columns);
        table.columns.addAll(scanColumns);
      }

    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedArrayScan(ResolvedNodes.ResolvedArrayScan array) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();
    array.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedArrayScan node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        ResolvedNodeExtended sourceTable = new ResolvedNodeExtended();

        if (node.getInputScan() != null) {
          sourceTable = checkNodeType(node.getInputScan());
        }

        int index = 0;
        for (ResolvedColumn resolvedColumn : node.getColumnList()) {
          ResolvedColumnExtended scanColumn =
              new ResolvedColumnExtended(resolvedColumn, (long) index++);
          for (ResolvedColumnExtended sourceColumn : sourceTable.columns) {
            if (scanColumn.resolvedIndex.longValue() == sourceColumn.resolvedIndex.longValue()) {
              scanColumn.columnsReferenced.add(sourceColumn);
            }
          }
          scanColumns.add(scanColumn);
        }

        for (ResolvedColumnExtended potentialColumn :
            scanColumns) {
          if (potentialColumn.resolvedIndex.longValue() == Long.valueOf(
              node.getElementColumn().getId()).longValue()) {
            if (node.getArrayExpr() != null) {
              ResolvedNodeExtended arrayExp = checkNodeType(node.getArrayExpr());
              for (ResolvedColumnExtended arrayColumn : arrayExp.columns) {
                connectChildrenRefs(arrayColumn, sourceTable.columns);
              }
              potentialColumn.columnsReferenced.addAll(arrayExp.columns);
            }
            if (node.getJoinExpr() != null) {
              ResolvedNodeExtended joinExp = checkNodeType(node.getJoinExpr());
              for (ResolvedColumnExtended joinColumn : joinExp.columns) {
                connectChildrenRefs(joinColumn, sourceTable.columns);
              }
              potentialColumn.columnsReferenced.addAll(joinExp.columns);
            }
          }
        }
        table.columns.addAll(scanColumns);
      }
    });
    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedFilterScan(ResolvedNodes.ResolvedFilterScan filterScan) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();

    filterScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedFilterScan node) {
        ResolvedNodeExtended sourceTable = checkNodeType(node.getInputScan());
        ResolvedNodeExtended filterColumns = checkNodeType(node.getFilterExpr());

        for (ResolvedColumnExtended column : filterColumns.columns) {
          column.usedFor.add(ResolvedColumnExtended.EXTRACOLUMNS.FILTER);
          connectChildrenRefs(column, sourceTable.columns);
        }

        table.extra_columns.addAll(filterColumns.columns);
        table.extra_columns.addAll(sourceTable.extra_columns);
        table.columns = sourceTable.columns;
      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedOrderByScan(ResolvedNodes.ResolvedOrderByScan literal) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();

    literal.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedOrderByScan node) {
        ResolvedNodeExtended sourceTable = checkNodeType(node.getInputScan());
        List<ResolvedColumnExtended> orderColumns = new ArrayList<>();
        for (ResolvedNodes.ResolvedOrderByItem item : node.getOrderByItemList()) {
          orderColumns.addAll(checkNodeType(item.getColumnRef()).columns);
        }
        for (ResolvedColumnExtended column : orderColumns) {
          column.usedFor.add(ResolvedColumnExtended.EXTRACOLUMNS.ORDER_BY);
          connectChildrenRefs(column, sourceTable.columns);
        }
        table.columns = sourceTable.columns;
        table.extra_columns.addAll(orderColumns);
        table.extra_columns.addAll(sourceTable.extra_columns);
      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedOrderByScanItem(ResolvedNodes.ResolvedOrderByItem literal) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();

    literal.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedOrderByItem node) {
        table.columns.addAll(checkNodeType(node.getColumnRef()).columns);
      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedWithRefScan(ResolvedNodes.ResolvedWithRefScan filterScan) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();

    filterScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedWithRefScan node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        int index = 0;
        for (ResolvedColumn resolvedColumn : node.getColumnList()) {
          scanColumns.add(new ResolvedColumnExtended(resolvedColumn, (long) index++));
        }
        table.columns = scanColumns;
      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedJoin(ResolvedNodes.ResolvedJoinScan joinScan) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();

    joinScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedJoinScan node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        ResolvedNodeExtended leftJoin = checkNodeType(node.getLeftScan());
        ResolvedNodeExtended rightJoin = checkNodeType(node.getRightScan());

        if (node.getJoinExpr() != null) {
          table.extra_columns.addAll(checkNodeType(node.getJoinExpr()).columns);
        }
        List<ResolvedColumnExtended> left = new ArrayList<>();
        for (ResolvedColumnExtended sourceColumn : table.extra_columns) {
          if (connectChildrenRefs(sourceColumn, leftJoin.columns)){
            sourceColumn.usedFor.add(ResolvedColumnExtended.EXTRACOLUMNS.JOIN_LEFT_TABLE);
            sourceColumn.joinType = node.getJoinType();
            left.add(sourceColumn);
          }
        }
        List<ResolvedColumnExtended> right = new ArrayList<>();
        for (ResolvedColumnExtended sourceColumn : table.extra_columns) {
          if (connectChildrenRefs(sourceColumn, rightJoin.columns)){
            sourceColumn.usedFor.add(ResolvedColumnExtended.EXTRACOLUMNS.JOIN_RIGHT_TABLE);
            sourceColumn.joinType = node.getJoinType();
            right.add(sourceColumn);
          }
        }

        if (left.isEmpty() && right.isEmpty() && node.getJoinType() == ResolvedJoinScanEnums.JoinType.INNER) {
          joins.add(new ResolvedJoinExtended("CROSS",leftJoin.columns, rightJoin.columns));
        } else {
          joins.add(new ResolvedJoinExtended(node.getJoinType().toString(), left, right));
        }

        scanColumns.addAll(leftJoin.columns);
        scanColumns.addAll(rightJoin.columns);
        table.extra_columns.addAll(leftJoin.extra_columns);
        table.extra_columns.addAll(rightJoin.extra_columns);

        table.columns = scanColumns;

      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedUnion(ResolvedNodes.ResolvedSetOperationScan joinScan) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();

    joinScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedSetOperationScan node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        List<ResolvedNodeExtended> tables = new ArrayList<>();
        for (ResolvedNodes.ResolvedSetOperationItem item : node.getInputItemList()) {
          tables.add(checkNodeType(item.getScan()));
        }
        int index = 0;
        for (ResolvedColumn column : node.getColumnList()) {
          ResolvedColumnExtended newColumn = new ResolvedColumnExtended(column, null);
          for (ResolvedNodeExtended unionTable : tables) {
            newColumn.columnsReferenced.add(unionTable.columns.get(index));
          }
          index++;
          scanColumns.add(newColumn);
        }
        table.columns = scanColumns;
        for (ResolvedNodeExtended table_input : tables) {
          table.extra_columns.addAll(table_input.extra_columns);
        }
      }
    });

    return simplifyTable(table);
  }


  private ResolvedNodeExtended resolvedAggregate(
      ResolvedNodes.ResolvedAggregateScan aggregateScan) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();

    aggregateScan.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedAggregateScan node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        List<ResolvedColumnExtended> extra_columns = new ArrayList<>();
        ResolvedNodeExtended groupedColumns = checkNodeType(node.getInputScan());
        for (ResolvedNodes.ResolvedComputedColumn column : node.getGroupByList()) {
          ResolvedNodeExtended groupByColumns = checkNodeType(column);
          for (ResolvedColumnExtended sourceColumn : groupByColumns.columns) {
            sourceColumn.usedFor.add(ResolvedColumnExtended.EXTRACOLUMNS.GROUP_BY);
            connectChildrenRefs(sourceColumn, groupedColumns.columns);
          }

          scanColumns.addAll(groupByColumns.columns);
          extra_columns.addAll(groupByColumns.extra_columns);
          extra_columns.addAll(groupByColumns.columns);
        }
        for (ResolvedNodes.ResolvedComputedColumn column : node.getAggregateList()) {
          ResolvedNodeExtended aggregatedColumns = checkNodeType(column);
          for (ResolvedColumnExtended sourceColumn : aggregatedColumns.columns) {
            connectChildrenRefs(sourceColumn, groupedColumns.columns);
          }
          scanColumns.addAll(aggregatedColumns.columns);
          extra_columns.addAll(aggregatedColumns.extra_columns);

        }
        table.extra_columns.addAll(extra_columns);
        table.extra_columns.addAll(groupedColumns.extra_columns);
        table.columns = scanColumns;
      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedColumnRef(ResolvedNodes.ResolvedColumnRef columnRef) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();

    columnRef.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedColumnRef node) {
        if (node.getColumn() != null) {
          if (node.getColumn().getType().getKind() == ZetaSQLType.TypeKind.TYPE_STRUCT) {
            List<ResolvedColumnExtended> expandedNodes = expandStruct(node.getType().asStruct(),
                node.getColumn().getTableName(), node.getColumn().getId(),
                node.getColumn().getName());
            table.columns.addAll(expandedNodes);
          } else if (node.getColumn().getType().getKind() == ZetaSQLType.TypeKind.TYPE_ARRAY) {
            List<ResolvedColumnExtended> expandedNodes =
                expandStruct(node.getType().asArray(),
                    node.getColumn().getTableName(), node.getColumn().getId(),
                    node.getColumn().getName(), 0L);
            table.columns.addAll(expandedNodes);

          } else {
            table.columns.add(new ResolvedColumnExtended(node.getColumn(), null));
            table.columns.get(0).indexStructList.add(0L);
            table.columns.get(0).originalIndexStructList.add(0L);
          }
        } else {
          table.columns.add(new ResolvedColumnExtended("_literal_", "_literal_", -1L, -1L, null));
        }
      }
    });

    return simplifyTable(table);
  }


  private ResolvedNodeExtended resolvedAggregateFunctionCall(
      ResolvedNodes.ResolvedAggregateFunctionCall aggregateFunctionCall) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();
    aggregateFunctionCall.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedAggregateFunctionCall node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        if (node.getArgumentList() != null && node.getArgumentList().size() != 0) {
          for (ResolvedNodes.ResolvedExpr item : node.getArgumentList()) {
            ResolvedNodeExtended sourceTable = checkNodeType(item);
            if (sourceTable != null && sourceTable.columns.size() > 0) {
              scanColumns.addAll(sourceTable.columns);
            }
          }
          table.columns.addAll(scanColumns);
        } else {
          table.columns.add(new ResolvedColumnExtended("_literal_", "_literal_", -1L, -1L, null));
        }
      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedComputedColumn(
      ResolvedNodes.ResolvedComputedColumn computedColumn) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();

    computedColumn.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedComputedColumn node) {
        ResolvedNodeExtended sourceData = checkNodeType(node.getExpr());
        ResolvedColumnExtended column = new ResolvedColumnExtended(node.getColumn(), null);
        if (sourceData != null && sourceData.columns.size() > 0) { // potentially unnest fields
          column.columnsReferenced.addAll(sourceData.columns);
        } else {
          column = new ResolvedColumnExtended("_literal_", "_literal_", -1L, -1L, null);
        }
        table.columns.add(column);
        if (sourceData != null) {
          table.extra_columns.addAll(sourceData.extra_columns);
        }
      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedFunctionCall(
      ResolvedNodes.ResolvedFunctionCall functionCall) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();

    functionCall.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedFunctionCall node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        if (node.getArgumentList() != null && node.getArgumentList().size() != 0) {
          for (ResolvedNodes.ResolvedExpr item : node.getArgumentList()) {
            ResolvedNodeExtended sourceTable = checkNodeType(item);
            if (sourceTable != null && sourceTable.columns.size() > 0) {
              scanColumns.addAll(sourceTable.columns);
            }
          }
          table.columns.addAll(scanColumns);
        } else {
          table.columns.add(new ResolvedColumnExtended("_literal_", "_literal_", -1L, -1L, null));
        }

      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedSubqueryExp(ResolvedNodes.ResolvedSubqueryExpr subquery) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();
    subquery.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedSubqueryExpr node) {
        ResolvedNodeExtended sourceTable = checkNodeType(node.getSubquery());
        table.extra_columns.addAll(sourceTable.extra_columns);
        table.columns.addAll(sourceTable.columns);
      }
    });
    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedCast(ResolvedNodes.ResolvedCast cast) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();
    cast.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedCast node) {
        // check the columns that are being cast.
        table.columns.addAll(checkNodeType(node.getExpr()).columns);
      }
    });
    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedLiteral(ResolvedNodes.ResolvedLiteral literal) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();
    literal.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedLiteral node) {
        //final leaf it's a literal value, create a new column as a constant
        table.columns.add(new ResolvedColumnExtended("_literal_", "_literal_", -1L, -1L,
            getLiteral(node)));
      }
    });

    return simplifyTable(table);
  }


  private ResolvedNodeExtended resolvedAnalyticFunctionCall(
      ResolvedNodes.ResolvedAnalyticFunctionCall functionCall) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();

    functionCall.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedAnalyticFunctionCall node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        if (node.getArgumentList() != null && node.getArgumentList().size() != 0) {
          for (ResolvedNodes.ResolvedExpr item : node.getArgumentList()) {
            ResolvedNodeExtended sourceTable = checkNodeType(item);
            if (sourceTable != null && sourceTable.columns.size() > 0) {
              scanColumns.addAll(sourceTable.columns);
            }
          }
          table.columns.addAll(scanColumns);
        } else {
          table.columns.add(new ResolvedColumnExtended("_literal_", "_literal_", -1L, -1L, null));
        }

      }
    });

    return simplifyTable(table);
  }

  private ResolvedNodeExtended resolvedMakeStruct(ResolvedNodes.ResolvedMakeStruct struct) {

    ResolvedNodeExtended table = new ResolvedNodeExtended();

    struct.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedMakeStruct node) {
        List<ResolvedColumnExtended> scanColumns = new ArrayList<>();
        if (node.getFieldList() != null && node.getFieldList().size() != 0) {
          long indexStruct = 0L;
          for (ResolvedNodes.ResolvedExpr item : node.getFieldList()) {
            ResolvedNodeExtended sourceTable = checkNodeType(item);
            if (sourceTable != null && sourceTable.columns.size() > 0) {
              for (ResolvedColumnExtended column : sourceTable.columns) {
//                column.indexStructList.add(indexStruct);
                column.makeStructIndex.add(indexStruct);
              }
              scanColumns.addAll(sourceTable.columns);
            }
            indexStruct++;
          }
          table.columns.addAll(scanColumns);
        } else {
          table.columns.add(new ResolvedColumnExtended("_literal_", "_literal_", -1L, -1L, null));
        }

      }
    });

    return simplifyTable(table);
  }


  private ResolvedNodeExtended resolvedGetStruct(ResolvedNodes.ResolvedGetStructField struct) {
    ResolvedNodeExtended table = new ResolvedNodeExtended();

    struct.accept(new ResolvedNodes.Visitor() {
      @Override
      public void visit(ResolvedNodes.ResolvedGetStructField node) {

//        table.columns.addAll(checkNodeType(node.getExpr()).columns);
        //will always return one column only
        List<ResolvedColumnExtended> columns = checkNodeType(node.getExpr()).columns;
        if (node.getFieldIdx() >= 0) {
          // we only keep the reference that was actually used
          // but because of the structs, it can be hidden in a deeper node, so we find the one that
          // actually holds all the references //4324234
          ArrayList<ResolvedColumnExtended> elemsToRetain = new ArrayList<>();
          for (ResolvedColumnExtended column : columns) {
            if (column.indexStructList.getLast() == node.getFieldIdx()) {
              column.indexStructList.removeLast();
              elemsToRetain.add(column);
            }
          }
          columns.retainAll(elemsToRetain);
          table.columns.addAll(columns);
        }
      }
    });

    return simplifyTable(table);
  }


  private ResolvedNodeExtended resolveFunctions(ResolvedStatement stmt) {
    stmt.accept(
        // Visit Create Functions
        new ResolvedNodes.Visitor() {
          @Override
          public void visit(ResolvedNodes.ResolvedCreateFunctionStmt node) {
            ArrayList<FunctionSignature> signatures = new ArrayList<>();
            signatures.add(node.getSignature());
            Function fn1 = new Function(
                node.getNamePath(),
                "ZetaSQL",
                ZetaSQLFunctions.FunctionEnums.Mode.SCALAR,
                signatures
            );
            if (catalog.getFunctionByFullName(fn1.getFullName()) == null) {
              catalog.addFunction(fn1);
            }
            super.visit(node);
          }
        });
    return new ResolvedNodeExtended();
  }

  /////// UTILS  -- a lot of recursive functions trying to find a node, note to self: change
  // the data modelling to use a proper tree representation instead of going so brave with a
  // nested list, and forget about all this, but
  // too late I'm too deep now TAKE ME OUT please///////


  public ResolvedColumnExtended deepCloner(ResolvedColumnExtended column) {

    ResolvedColumnExtended newColumn = new ResolvedColumnExtended(column.name,
        column.tableName, column.indexOriginalTable, column.resolvedIndex, column.type,
        0L, new HashSet<>(column.usedFor),column.joinType);
    newColumn.indexStructList = new LinkedList<>(column.indexStructList);
    newColumn.literalValue = column.literalValue;
    newColumn.originalIndexStructList = new LinkedList<>(column.originalIndexStructList);
    newColumn.makeStructIndex = new LinkedList<>(column.makeStructIndex);
    ArrayList<ResolvedColumnExtended> dependencies = new ArrayList<>();
    for (ResolvedColumnExtended newRefColumn :
        column.columnsReferenced) {
      dependencies.add(deepCloner(newRefColumn));
    }

    newColumn.columnsReferenced = dependencies;
    return newColumn;

  }

  public void connectAllTables(ResolvedColumnExtended currentObject) {
    //get all child-URIs for the category
    List<ResolvedColumnExtended> objectChildren = currentObject.columnsReferenced;
    if (objectChildren.size() == 0) {
      ResolvedNodeExtended sourceTable = allTables.get(currentObject.tableName);
      if (sourceTable != null) {
        if (currentObject.indexOriginalTable != null
            && currentObject.indexOriginalTable.intValue() != -1) {
          ResolvedColumnExtended column =
              sourceTable.columns.get(currentObject.indexOriginalTable.intValue());
          if (!currentObject.originalIndexStructList.isEmpty() && column.type.isStruct()) {
            ResolvedColumnExtended newDependency = deepCloner(column);
            findParent(newDependency, currentObject.originalIndexStructList);
            currentObject.columnsReferenced.add(newDependency);
          } else if (currentObject.tableName.startsWith("$array")
                     && currentObject.originalIndexStructList.size() > 0) {
            // hate arrays
            for (Long index : currentObject.originalIndexStructList) {
              currentObject.columnsReferenced.add(
                  deepCloner(column.columnsReferenced.get(index.intValue())));
            }
          } else {
            currentObject.columnsReferenced.add(deepCloner(column));
          }
        }
      }
    } else {
      for (ResolvedColumnExtended childObj : objectChildren) {

        if (childObj != null) {

          if (childObj.columnsReferenced.size() > 0) {
            connectAllTables(childObj);
          } else {
            ResolvedNodeExtended sourceTable = allTables.get(childObj.tableName);
            if (sourceTable != null) {
              if (childObj.indexOriginalTable != null
                  && childObj.indexOriginalTable.intValue() != -1) {
                ResolvedColumnExtended column =
                    sourceTable.columns.get(childObj.indexOriginalTable.intValue());
                if (!childObj.originalIndexStructList.isEmpty() && column.type.isStruct()) {
                  ResolvedColumnExtended newDependency = deepCloner(column);
                  findParent(newDependency, childObj.originalIndexStructList);
                  childObj.columnsReferenced.add(newDependency);
                } else {
                  childObj.columnsReferenced.add(deepCloner(column));
                }
              }
            }
          }
        }
      }
    }
  }

  public Boolean matchStructSource(LinkedList<Long> currentPath, LinkedList<Long> potentialPath) {
    boolean match = true;
    List<Long> reversed = ImmutableList.copyOf(potentialPath).reverse();
    for (Long potentialPathElement : reversed) {
      if (currentPath.size() > 0) {
        Long expectedPathElement = currentPath.removeLast();
        if (!potentialPathElement.equals(expectedPathElement)) {
          match = false;
        }
      } else {
        match = false;
      }
    }
    return match;
  }

  public Boolean findParent(ResolvedColumnExtended column, LinkedList<Long> indexStructPath) {
    ArrayList<ResolvedColumnExtended> childrenToRemove = new ArrayList<>();
    for (ResolvedColumnExtended children : column.columnsReferenced) {
      LinkedList<Long> discoveredPath = new LinkedList<>(indexStructPath);
      if (matchStructSource(discoveredPath, children.makeStructIndex)) {
        if (children.columnsReferenced.size() > 0 && discoveredPath.size() > 0) {
          boolean shouldBeRemoved = findParent(children, discoveredPath);
          if (shouldBeRemoved) { // all the children have been removed
            childrenToRemove.add(children);
          }
        }
      } else {
        childrenToRemove.add(children);
      }
    }
    if (column.columnsReferenced.size() == childrenToRemove.size()) { // equal
      return true;
    } else if (childrenToRemove.size() > 0) {
      column.columnsReferenced.removeAll(childrenToRemove);
      return false;
    } else {
      return false;
    }

  }

  public void updateChildrenRefs(ResolvedColumnExtended currentObject,
                                 ImmutableList<ResolvedColumn> columnList) {

    //get all child-URIs for the category
    List<ResolvedColumnExtended> objectChildren = currentObject.columnsReferenced;

    if (objectChildren.size() == 0) {

      int index = 0;
      for (ResolvedColumn column : columnList) {
        if (Long.valueOf(column.getId()).longValue() == currentObject.resolvedIndex.longValue()) {
          if (currentObject.indexOriginalTable == null) {
            currentObject.indexOriginalTable = (long) index;
          }
        }
        index++;
      }
    } else {
      for (ResolvedColumnExtended childObj : objectChildren) {

        if (childObj != null) {

          if (childObj.columnsReferenced.size() > 0) {
            updateChildrenRefs(childObj, columnList);
          } else {
            int index = 0;
            for (ResolvedColumn column : columnList) {
              if (Long.valueOf(column.getId()).longValue() == childObj.resolvedIndex.longValue()) {
                if (childObj.indexOriginalTable == null) {
                  childObj.indexOriginalTable = (long) index;
                }
              }
              index++;
            }
          }
        }
      }
    }
  }


  public boolean connectChildrenRefs(ResolvedColumnExtended currentObject,
                                  List<ResolvedColumnExtended> columnList) {
    boolean connected = false;
    //get all child-URIs for the category
    List<ResolvedColumnExtended> objectChildren = currentObject.columnsReferenced;

    if (objectChildren.size() == 0) {
      connected = iterateOverNodes(columnList, currentObject) || connected;

    } else {
      for (ResolvedColumnExtended childObj : objectChildren) {

        if (childObj != null) {

          if (childObj.columnsReferenced.size() > 0) {
            connected = connectChildrenRefs(childObj, columnList) || connected;
          } else {
            connected = iterateOverNodes(columnList, childObj) || connected;
          }
        }
      }
    }
    return connected;
  }

  private boolean iterateOverNodes(final List<ResolvedColumnExtended> columnList,
                                final ResolvedColumnExtended childObj) {
    List<ResolvedColumnExtended> columnsToAdd = new ArrayList<>();
    boolean connected = false;
    for (ResolvedColumnExtended column : columnList) {
      if (column.resolvedIndex.longValue() == childObj.resolvedIndex.longValue()) {
        if (column.type.isStruct() && listOfNames(column.type.asStruct(), column.name).contains(
            childObj.name) && column.columnsReferenced.size() == 0) {
          childObj.indexOriginalTable = column.indexOriginalTable;
          connected = true;
        } else if (column.type.isArray() && listOfNames(column.type.asArray(),
            column.name).contains(
            childObj.name) && column.columnsReferenced.size() == 0) {
          childObj.indexOriginalTable = column.indexOriginalTable;
          connected = true;
        } else {
          columnsToAdd.add(deepCloner(column));
          connected = true;
        }
      }
    }
    //probably improve this

    List<ResolvedColumnExtended> prunedColumnsToAdd = new ArrayList<>();
    if (columnsToAdd.size() > 1) {
      if (childObj.tableName.startsWith("$array")) {
        // hate arrays
        for (Long index : childObj.originalIndexStructList) {
          prunedColumnsToAdd.add(columnsToAdd.get(0).columnsReferenced.get(index.intValue()));
        }
      } else {
        for (ResolvedColumnExtended column : columnsToAdd) {
          if (column.makeStructIndex.equals(childObj.originalIndexStructList)) {
            prunedColumnsToAdd.add(column);
          }
        }
      }
    } else if (childObj.tableName.startsWith("$array") && columnsToAdd.size() > 0) {
      //hate arrays
      if (columnsToAdd.get(0).columnsReferenced.size() == 1) {
        ResolvedColumnExtended newChild = columnsToAdd.get(0).columnsReferenced.get(0);
        for (Long index : childObj.originalIndexStructList) {
          prunedColumnsToAdd.add(newChild.columnsReferenced.get(index.intValue()));
        }
      } else {
        for (Long index : childObj.originalIndexStructList) {
          prunedColumnsToAdd.add(columnsToAdd.get(0).columnsReferenced.get(index.intValue()));
        }
      }
      //change this to be recursive until finding the last node
    } else if (columnsToAdd.size() == 1 && (columnsToAdd.get(0).type.isStruct())) {
      for (ResolvedColumnExtended column : columnsToAdd.get(0).columnsReferenced) {
        if (column.columnsReferenced.size() > 1 && column.type.isStruct()) {
          for (ResolvedColumnExtended columnSubnested : column.columnsReferenced) {
            if (columnSubnested.makeStructIndex.equals(childObj.originalIndexStructList)) {
              prunedColumnsToAdd.add(columnSubnested);
            }
          }
        } else {
          if (column.makeStructIndex.equals(childObj.originalIndexStructList)) {
            prunedColumnsToAdd.add(column);
          }
        }
      }
    } else if (columnsToAdd.size() == 1 && (columnsToAdd.get(
        0).type.isArray())) {
      prunedColumnsToAdd.add(columnsToAdd.get(0).columnsReferenced.get(
          childObj.originalIndexStructList.get(0).intValue()));
    } else {
      prunedColumnsToAdd.addAll(columnsToAdd);
    }
    childObj.columnsReferenced.addAll(prunedColumnsToAdd);
    if(prunedColumnsToAdd.size() > 0) {
      connected = true;
    }
    return connected;
  }

  private List<String> listOfNames(StructType root, String parentStructName) {
    List<String> columns = new ArrayList<>();
    for (StructType.StructField field : root.getFieldList()) {
      if (field.getType().getKind() == ZetaSQLType.TypeKind.TYPE_STRUCT) {
        columns.addAll(
            listOfNames(field.getType().asStruct(), parentStructName + "." + field.getName()));
      } else if (field.getType().getKind() == ZetaSQLType.TypeKind.TYPE_ARRAY) {
        columns.addAll(
            listOfNames(field.getType().asArray(), parentStructName + "." + field.getName()));
      } else {
        columns.add(parentStructName + "." + field.getName());
      }
    }
    return columns;
  }


  private List<String> listOfNames(ArrayType root, String parentStructName) {
    List<String> columns = new ArrayList<>();
    if (root.getElementType().isArray()) {
      columns.addAll(listOfNames(root.getElementType().asArray(), parentStructName));
    } else if (root.getElementType().isStruct()) {
      columns.addAll(listOfNames(root.getElementType().asStruct(), parentStructName));
    } else {
      columns.add(parentStructName);
    }
    return columns;
  }


  private List<ResolvedColumnExtended> expandStruct(StructType root, String tableName, Long id,
                                                    String parentStructName) {
    List<ResolvedColumnExtended> columns = new ArrayList<>();
    Long indexStruct = 0L;
    for (StructType.StructField field : root.getFieldList()) {
      if (field.getType().getKind() == ZetaSQLType.TypeKind.TYPE_STRUCT) {
        List<ResolvedColumnExtended> nestedColumns = expandStruct(field.getType().asStruct(),
            tableName, id, parentStructName + "." + field.getName());
        for (ResolvedColumnExtended column : nestedColumns) {
          column.indexStructList.add(indexStruct);
          column.originalIndexStructList.add(indexStruct);
        }
        columns.addAll(nestedColumns);
      } else if (field.getType().getKind() == ZetaSQLType.TypeKind.TYPE_ARRAY) {
        List<ResolvedColumnExtended> nestedColumns =
            expandStruct(field.getType().asArray(),
                tableName, id, parentStructName + "." + field.getName(), indexStruct);
        columns.addAll(nestedColumns);
      } else {
        ResolvedColumnExtended column =
            new ResolvedColumnExtended(parentStructName + "." + field.getName(),
                tableName, -1L, id, field.getType(), 0L, new HashSet<>(),null);
        column.indexStructList.add(indexStruct);
        column.originalIndexStructList.add(indexStruct);
        columns.add(column);
      }
      indexStruct++;
    }
    return columns;
  }

  private List<ResolvedColumnExtended> expandStruct(ArrayType root, String tableName, Long id,
                                                    String parentStructName, Long indexStructNew) {
    List<ResolvedColumnExtended> columns = new ArrayList<>();
    if (root.getElementType().isArray()) {
      columns.addAll(
          expandStruct(root.getElementType().asArray(), tableName, id, parentStructName,
              indexStructNew));
    } else if (root.getElementType().isStruct()) {
      List<ResolvedColumnExtended> nestedColumns = expandStruct(root.getElementType().asStruct(),
          tableName, id,
          parentStructName);
      for (ResolvedColumnExtended column : nestedColumns) {
        column.indexStructList.add(indexStructNew);
        column.originalIndexStructList.add(indexStructNew);
      }
      columns.addAll(nestedColumns);
    } else {
      ResolvedColumnExtended column = new ResolvedColumnExtended(parentStructName,
          tableName, -1L, id, root.getElementType(), 0L, new HashSet<>(), null);
      column.indexStructList.add(indexStructNew);
      column.originalIndexStructList.add(indexStructNew);
      columns.add(column);
    }
    return columns;
  }

  //TODO:
  private List<ResolvedColumnExtended> expandStructInput(StructType root,
                                                         ResolvedColumn resolvedColumn,
                                                         Long originalIndex,
                                                         String parentStructName) {
    List<ResolvedColumnExtended> columns = new ArrayList<>();
    Long indexStruct = 0L;
    for (StructType.StructField field : root.getFieldList()) {
      if (field.getType().getKind() == ZetaSQLType.TypeKind.TYPE_STRUCT) {
        String fieldName = field.getName().equals("") ?
                           "_field_" + anonymFieldCounter.incrementAndGet() :
                           field.getName();
        List<ResolvedColumnExtended> nestedColumns = expandStructInput(field.getType().asStruct(),
            resolvedColumn, originalIndex, parentStructName + "." + fieldName);
        for (ResolvedColumnExtended column : nestedColumns) {
          column.indexStructList.add(indexStruct);
          column.originalIndexStructList.add(indexStruct);
          column.makeStructIndex.add(indexStruct);
        }
        columns.addAll(nestedColumns);
      } else if (field.getType().getKind() == ZetaSQLType.TypeKind.TYPE_ARRAY) {
        List<ResolvedColumnExtended> nestedColumns =
            expandStructInput(field.getType().asArray(),
                resolvedColumn, originalIndex, parentStructName + "." + field.getName(),
                indexStruct);
        columns.addAll(nestedColumns);
        for (ResolvedColumnExtended column : nestedColumns) {
          column.indexStructList.add(indexStruct);
          column.originalIndexStructList.add(indexStruct);
          column.makeStructIndex.add(indexStruct);
        }
      } else {
        String fieldName = field.getName().equals("") ?
                           "_field_" + anonymFieldCounter.incrementAndGet() :
                           field.getName();
        ResolvedColumnExtended column =
            new ResolvedColumnExtended(parentStructName + "." + fieldName,
                resolvedColumn.getTableName(), originalIndex, resolvedColumn.getId(),
                field.getType(), indexStruct, new HashSet<>(), null);
        column.indexStructList.add(indexStruct);
        column.originalIndexStructList.add(indexStruct);
        column.makeStructIndex.add(indexStruct);
        columns.add(column);
      }
      indexStruct++;
    }
    return columns;
  }

  private List<ResolvedColumnExtended> expandStructInput(ArrayType root,
                                                         ResolvedColumn resolvedColumn,
                                                         Long originalIndex,
                                                         String parentStructName,
                                                         Long carriedIndex) {

    List<ResolvedColumnExtended> columns = new ArrayList<>();

    if (root.getElementType().isArray()) {
      columns.addAll(
          expandStructInput(root.getElementType().asArray(), resolvedColumn, originalIndex,
              parentStructName, carriedIndex));
    } else if (root.getElementType().isStruct()) {
      List<ResolvedColumnExtended> nestedColumns =
          expandStructInput(root.getElementType().asStruct(), resolvedColumn, originalIndex,
              parentStructName);
      columns.addAll(nestedColumns);
    } else {
      ResolvedColumnExtended column = new ResolvedColumnExtended(parentStructName,
          resolvedColumn.getTableName(), originalIndex, resolvedColumn.getId(),
          root.getElementType(), 0L, new HashSet<>(), null);
      column.indexStructList.add(carriedIndex);
      column.originalIndexStructList.add(carriedIndex);
      column.makeStructIndex.add(carriedIndex);
      columns.add(column);
    }
    return columns;
  }

  // this method will remove redundant nodes that are similar and just make the dag grow with
  // not needed nodes.
  public static ResolvedNodeExtended simplifyTable(ResolvedNodeExtended table) {
    for (ResolvedColumnExtended column : table.columns) {
      simplifyNodes(column);
    }
    for (ResolvedColumnExtended column : table.extra_columns) {
      simplifyNodes(column);
    }
    return table;
  }

  public static void simplifyNodes(ResolvedColumnExtended node) {

    if (node.columnsReferenced.size() == 1) {
      ResolvedColumnExtended child = node.columnsReferenced.get(0);
      if (node.name.equals(child.name) && node.tableName.equals(child.tableName)
          && node.resolvedIndex.equals(child.resolvedIndex)) {
        node.columnsReferenced = child.columnsReferenced;
        node.indexOriginalTable = child.indexOriginalTable;
        node.usedFor.addAll(child.usedFor);
        simplifyNodes(node);
      } else if (node.name.startsWith(child.name) && node.tableName.equals(child.tableName)
                 && node.resolvedIndex.equals(child.resolvedIndex)) {
        node.columnsReferenced = child.columnsReferenced;
        node.indexOriginalTable = child.indexOriginalTable;
        node.usedFor.addAll(child.usedFor);
        simplifyNodes(node);
      }
    }
    for (ResolvedColumnExtended child : node.columnsReferenced) {
      simplifyNodes(child);
    }
  }
}
