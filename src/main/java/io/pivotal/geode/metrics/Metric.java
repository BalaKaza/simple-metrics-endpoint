package io.pivotal.geode.metrics;

import java.io.Serializable;

public class Metric implements Serializable {

  private String name;

  private Number value;

  private String category;

  private String type;

  private static final long serialVersionUID = 1L;

  public Metric(String name, Number value, String category, String type) {
    this.name = name;
    this.value = value;
    this.category = category;
    this.type = type;
  }

  public String getName() {
    return this.name;
  }

  public Number getValue() {
    return this.value;
  }

  public String getCategory() {
    return this.category;
  }

  public String getType() {
    return this.type;
  }

  public String toString() {
    return new StringBuilder()
      .append(getClass().getSimpleName())
      .append("[")
      .append("name=")
      .append(this.name)
      .append("; value=")
      .append(this.value)
      .append("; valueClass=")
      .append(this.value.getClass().getSimpleName())
      .append("; category=")
      .append(this.category)
      .append("; type=")
      .append(this.type)
      .append("]")
      .toString();
  }

  public boolean isMemoryType() {
    return this.category.equals("VMMemoryPoolStats");
  }

  public String getMapName() {
    String mapName = this.name;
    if (this.type != null) {
      mapName = mapName + "." + this.type;
    }
    return mapName;
  }
}
