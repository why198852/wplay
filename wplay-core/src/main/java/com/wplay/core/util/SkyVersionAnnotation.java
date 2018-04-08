package com.wplay.core.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A package attribute that captures the version of Hadoop that was compiled.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PACKAGE)
public @interface SkyVersionAnnotation {
 
  /**
   * Get the Hadoop version
   * @return the version string "0.6.3-dev"
   */
  String version();
  
  /**
   * Get the username that compiled Hadoop.
   */
  String user();
  
  /**
   * Get the date when Hadoop was compiled.
   * @return the date in unix 'date' format
   */
  String date();
  
  /**
   * Get the url for the subversion repository.
   */
  String url();
  
  /**
   * Get the subversion revision.
   * @return the revision number as a string (eg. "451451")
   */
  String revision();

  /**
   * Get a checksum of the source files from which
   * Hadoop was compiled.
   * @return a string that uniquely identifies the source
   **/
  String srcChecksum();
}
