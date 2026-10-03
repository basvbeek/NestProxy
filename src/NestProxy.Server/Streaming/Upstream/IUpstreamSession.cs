namespace NestProxy.Server.Streaming.Upstream;

/// <summary>
/// Abstraction over an upstream RTSP session to a Google Nest stream endpoint. Encapsulates the
/// DESCRIBE + multi-track SETUP + PLAY sequence, interleaved channel assignment, media forwarding,
/// and basic keep-alive + teardown operations.
/// </summary>
public interface IUpstreamSession : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Unique local instance identifier (used for log correlation; e.g. "1" primary, "2" staging).
    /// </summary>
    string InstanceId { get; }

    /// <summary>
    /// RTSP Session header value once established (null until first successful SETUP).
    /// </summary>
    string? SessionId { get; }

    /// <summary>
    /// Content-Base (or Location) derived base URL resolved from DESCRIBE.
    /// </summary>
    string? BaseUrl { get; }

    /// <summary>
    /// Aggregate (session-level) URL if provided (used for PLAY / session-level SETUP fallback).
    /// </summary>
    string? AggregateUrl { get; }

    /// <summary>
    /// Ordered list of (RTP, RTCP) interleaved channel pairs assigned per track.
    /// </summary>
    IReadOnlyList<(int data, int control)> InterleavedAssignments { get; }
    uint? LastObservedRtpSsrc { get; }
    bool ByeObserved { get; }

    /// <summary>
    /// Raised after DESCRIBE is parsed and control URLs / track URIs are resolved.
    /// </summary>
    event Action<SdpResolutionResult>? DescribeResolved;

    /// <summary>
    /// Raised when the upstream connection is closed or fails without being closed by this session.
    /// </summary>
    event Action? Disconnected;

    /// <summary>
    /// Opens the upstream transport and sends an initial OPTIONS request.
    /// </summary>
    /// <param name="url">Target RTSP(S) URL.</param>
    /// <param name="onRtp">Async handler for incoming RTP packets.</param>
    /// <param name="onRtcp">Async handler for incoming RTCP packets.</param>
    /// <param name="ct">Cancellation token.</param>
    Task ConnectAsync(Uri url, Func<ReadOnlyMemory<byte>, ValueTask> onRtp, Func<ReadOnlyMemory<byte>, ValueTask> onRtcp, CancellationToken ct);

    /// <summary>
    /// Returns the first control (RTCP) channel number, if any.
    /// </summary>
    bool TryGetFirstControlChannel(out int controlChannel);

    /// <summary>
    /// Sends an upstream OPTIONS keep-alive request.
    /// </summary>
    void SendOptionsKeepalive();

    /// <summary>
    /// Sends an upstream GET_PARAMETER keep-alive request.
    /// </summary>
    void SendGetParameterKeepalive();

    /// <summary>
    /// Sends an RTCP keep-alive packet using the entire buffer length.
    /// </summary>
    void SendRtcpKeepalive(byte[] packet);

    /// <summary>
    /// Sends an RTCP keep-alive packet using a specified data length.
    /// </summary>
    void SendRtcpKeepalive(byte[] packet, int length);

    /// <summary>
    /// Attempts to issue a TEARDOWN for the active session.
    /// </summary>
    Task TeardownAsync();

    /// <summary>
    /// Closes the underlying transport without TEARDOWN.
    /// </summary>
    Task CloseAsync();
}