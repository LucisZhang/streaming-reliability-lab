package com.p1.reliability.cdc;

public final class IcebergAdmin {
  private IcebergAdmin() {}

  public static void main(String[] args) {
    int resetIndex = -1;
    for (int index = 0; index < args.length; index++) {
      if ("reset-tables".equals(args[index])) {
        resetIndex = index;
        break;
      }
    }
    if (resetIndex < 0) {
      throw new IllegalArgumentException("Usage: IcebergAdmin reset-tables [job args]");
    }
    String[] configArgs = new String[args.length - 1];
    int output = 0;
    for (int index = 0; index < args.length; index++) {
      if (index != resetIndex) {
        configArgs[output] = args[index];
        output++;
      }
    }
    IcebergTables.dropTables(JobConfig.fromArgs(configArgs));
  }
}
