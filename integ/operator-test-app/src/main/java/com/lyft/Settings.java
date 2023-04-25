package com.lyft;

public class Settings {
  private static final String CHANGE_JOB_GRAPH = "CHANGE_JOB_GRAPH";
  private static final String SKIP_INDUCED_FAILURE = "SKIP_INDUCED_FAILURE";

  public static boolean changeJobGraph() {
    return System.getenv(CHANGE_JOB_GRAPH) != null && System.getenv(CHANGE_JOB_GRAPH).equals("true");
  }

  public static boolean skipInducedFailure() {
    return System.getenv(SKIP_INDUCED_FAILURE) != null && System.getenv(SKIP_INDUCED_FAILURE).equals("true");
  }
}
