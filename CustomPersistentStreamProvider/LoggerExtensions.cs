using System.Runtime.CompilerServices;
using Orleans.Runtime;

namespace PipeStreamProvider
{
    // TODO: Take this out
    public static class LoggerExtensions
    {
        public static void AutoError(this Logger logger, string message = "", int code = 0, [CallerMemberName] string callerName = "")
        {
            logger.Error(code, $"{callerName}: {message}");
        }
        public static void AutoWarn(this Logger logger, string message = "", int code = 0, [CallerMemberName] string callerName = "")
        {
            logger.Warn(code, $"{callerName}: {message}");
        }
        public static void AutoInfo(this Logger logger, string message = "", int code = 0, [CallerMemberName] string callerName = "")
        {
            logger.Info(code, $"{callerName}: {message}");
        }
        public static void AutoVerbose(this Logger logger, string message = "", int code = 0, [CallerMemberName] string callerName = "")
        {
            logger.Verbose(code, $"{callerName}: {message}");
        }
    }
}