namespace NestProxy.Server.Core;

/// <summary>
/// Abstraction for a long-lived upstream RTSP relay for a single camera. Implementations encapsulate
/// establishing the upstream session, periodically extending tokens (without teardown), forwarding
/// RTP/RTCP packets to the local server and performing seamless auth refresh (staging + promotion).
/// </summary>
public interface ICameraRelay : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Identifier of the camera handled by this relay (local endpoint path).
    /// </summary>
    string CameraId { get; }

    /// <summary>
    /// Begins connecting to the supplied authenticated upstream RTSP URL and starts forwarding media upon PLAY.
    /// </summary>
    /// <param name="upstreamUrl">Fully qualified RTSP(S) URL including auth query token.</param>
    /// <param name="ct">Cancellation token for the initial start operation.</param>
    Task StartAsync(string upstreamUrl, CancellationToken ct);

    /// <summary>
    /// True once the primary upstream session has been established and media is (or can be) flowing.
    /// </summary>
    bool IsRunning { get; }

    /// <summary>
    /// Raised when the active upstream session disconnects and the relay no longer receives media.
    /// </summary>
    event Action? Disconnected;

    /// <summary>
    /// Initiates an authentication refresh using a new stream token, performing a non-disruptive replay (SETUP/PLAY)
    /// via a staging session. Returns false if a refresh is already in progress or the relay is not in a state that
    /// supports refresh.
    /// </summary>
    /// <param name="newAuthToken">New stream token appended to the upstream base URL auth query.</param>
    /// <returns>True if the refresh sequence started; otherwise false.</returns>
    Task<bool> RefreshAuthAndReplayAsync(string newAuthToken);
}