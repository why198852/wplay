package com.wplay.core.util;

public class TopLevelDomain extends DomainSuffix
{
  private Type type;
  private String countryName = null;

  public TopLevelDomain(String domain, Type type, Status status, float boost) {
    super(domain, status, boost);
    this.type = type;
  }

  public TopLevelDomain(String domain, Status status, float boost, String countryName) {
    super(domain, status, boost);
    this.type = Type.COUNTRY;
    this.countryName = countryName;
  }

  public Type getType() {
    return this.type;
  }

  public String getCountryName()
  {
    return this.countryName;
  }

  public static enum Type
  {
    INFRASTRUCTURE, GENERIC, COUNTRY;
  }
}

/* Location:           /opt/brainbook/log-statistic/LogStatistic.jar
 * Qualified Name:     com.sky.statistic.util.TopLevelDomain
 * JD-Core Version:    0.6.0
 */