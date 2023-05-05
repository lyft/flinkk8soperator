package com.lyft;

public class Settings {
  private static final String SKIP_INDUCED_FAILURE = "SKIP_INDUCED_FAILURE";

  public static boolean skipInducedFailure() {
    return System.getenv(SKIP_INDUCED_FAILURE) != null && System.getenv(SKIP_INDUCED_FAILURE).equals("true");
  }
}
