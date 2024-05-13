package com.borjav.data.model;

import com.google.zetasql.Type;
import com.google.zetasql.resolvedast.ResolvedColumn;

import com.google.zetasql.resolvedast.ResolvedJoinScanEnums;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;


public class ResolvedColumnExtended {

    public enum EXTRACOLUMNS { FILTER, GROUP_BY, ORDER_BY, JOIN_LEFT_TABLE, JOIN_RIGHT_TABLE,
        PARTITION_BY_ANALYTIC_FUNCTION,ORDER_BY_ANALYTIC_FUNCTION}


    public String name;
    public String tableName;
    public Long indexOriginalTable;
    public Long resolvedIndex;
    public List<ResolvedColumnExtended> columnsReferenced = new ArrayList<>();
    public HashSet<EXTRACOLUMNS> usedFor;
    public ResolvedJoinScanEnums.JoinType joinType;
    public Type type;
    public String literalValue;

    // used to keep track of the elements in the list when accessing getStructResolved.
    // it works as a queue
    public LinkedList<Long> indexStructList = new LinkedList<>();
    // This keeps track of what was the original source of the column when it comes from a struct
    // or an array
    public LinkedList<Long> originalIndexStructList = new LinkedList<>();

    // this keeps track of whats the new position when a field has been converted into a struct
    public LinkedList<Long> makeStructIndex = new LinkedList<>();

    public ResolvedColumnExtended(ResolvedColumn resolvedColumn, Long indexOriginalTable) {
        this.name = resolvedColumn.getName();
        this.tableName = resolvedColumn.getTableName();
        this.resolvedIndex = resolvedColumn.getId();
        this.indexOriginalTable = indexOriginalTable;
        this.type = resolvedColumn.getType();
        this.usedFor = new HashSet<>();
        this.joinType = null;
    }


    public ResolvedColumnExtended(String name, String tableName, Long indexOriginalTable,
                                  Long resolvedIndex, String literalValue) {
        this.name = name;
        this.tableName = tableName;
        this.indexOriginalTable = indexOriginalTable;
        this.resolvedIndex = resolvedIndex;
        this.type= null;
        this.literalValue = literalValue;
        this.usedFor = new HashSet<>();
        this.joinType = null;
    }

    public ResolvedColumnExtended(String name, String tableName, Long indexOriginalTable,
                                  Long resolvedIndex, Type type, Long resolvedSubIndex,
                                  HashSet<EXTRACOLUMNS> usedFor, ResolvedJoinScanEnums.JoinType joinType) {
        this.name = name;
        this.tableName = tableName;
        this.indexOriginalTable = indexOriginalTable;
        this.resolvedIndex = resolvedIndex;
        this.type= type;
        this.usedFor = usedFor;
        this.joinType = joinType;
    }
}
