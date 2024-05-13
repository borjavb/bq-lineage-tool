package com.borjav.data.model;

import java.util.List;


public class ResolvedJoinExtended {

  public enum JOIN_TYPE {CROSS, LEFT, RIGHT, FULL, INNER}


  public JOIN_TYPE join_type;

  public List<ResolvedColumnExtended> left;
  public List<ResolvedColumnExtended> right;

  public ResolvedJoinExtended(String join_type, List<ResolvedColumnExtended> left,
                              List<ResolvedColumnExtended> right) {
    this.left = left;
    this.right = right;
    this.join_type = JOIN_TYPE.valueOf(join_type.toUpperCase());

  }
}
