package com.wplay.core.util;

public class DomainSuffix
{
  private String domain;
  private Status status;
  private float boost;
  public static final float DEFAULT_BOOST = 1.0F;
  public static final Status DEFAULT_STATUS = Status.IN_USE;

  public DomainSuffix(String domain, Status status, float boost) {
    this.domain = domain;
    this.status = status;
    this.boost = boost;
  }

  public DomainSuffix(String domain) {
    this(domain, DEFAULT_STATUS, 1.0F);
  }

  public String getDomain() {
    return this.domain;
  }

  public Status getStatus() {
    return this.status;
  }

  public float getBoost() {
    return this.boost;
  }

  public String toString()
  {
    return this.domain;
  }

  public static enum Status
  {
    INFRASTRUCTURE, SPONSORED, UNSPONSORED, 
    STARTUP, PROPOSED, DELETED, PSEUDO_DOMAIN, DEPRECATED, IN_USE, NOT_IN_USE, REJECTED;
  }
}