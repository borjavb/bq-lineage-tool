package com.borjav.data.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.resolvedast.ResolvedColumn;
import java.util.ArrayList;
import java.util.List;

public class ResolvedNodeExtended {

    public List<ResolvedColumnExtended> columns = new ArrayList<>();
    public List<ResolvedColumnExtended> extra_columns = new ArrayList<>();
    @JsonIgnore
    public ImmutableList<ResolvedColumn> originalColumns;
    public String name;
    public String type;
    public String table_name;
    public List selected_tables = new ArrayList();







}
