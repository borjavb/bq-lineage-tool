package com.borjav.data.model;

import com.google.zetasql.Type;
import com.google.zetasql.resolvedast.ResolvedColumn;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ResolvedColumnExtended {

    public String name;
    public String tableName;
    public Long indexOriginalTable;
    public Long resolvedIndex;
    public List<ResolvedColumnExtended> columnsReferenced = new ArrayList<>();

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
    }


    public ResolvedColumnExtended(String name, String tableName, Long indexOriginalTable,
                                  Long resolvedIndex, String literalValue) {
        this.name = name;
        this.tableName = tableName;
        this.indexOriginalTable = indexOriginalTable;
        this.resolvedIndex = resolvedIndex;
        this.type= null;
        this.literalValue = literalValue;
    }

    public ResolvedColumnExtended(String name, String tableName, Long indexOriginalTable,
                                  Long resolvedIndex, Type type, Long resolvedSubIndex) {
        this.name = name;
        this.tableName = tableName;
        this.indexOriginalTable = indexOriginalTable;
        this.resolvedIndex = resolvedIndex;
        this.type= type;
    }
}
