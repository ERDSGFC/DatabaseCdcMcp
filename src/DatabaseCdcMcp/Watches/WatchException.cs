namespace DatabaseCdcMcp.Watches;

public sealed class WatchException(string message, Exception? innerException = null)
    : Exception(message, innerException);
