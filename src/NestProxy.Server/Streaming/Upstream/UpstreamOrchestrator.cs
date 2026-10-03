using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

using Microsoft.Extensions.Options;

using NestProxy.Server.Streaming.Downstream.Server;

namespace NestProxy.Server.Streaming.Upstream;

/// <summary>
/// Coordinates one active (primary) upstream RTSP session and (optionally) a staging (secondary) session used during
/// authentication token refresh. It exposes a simple relay surface that forwards upstream RTP / RTCP packets
/// into the in-process <see cref="ILocalRtspServer"/> while handling keep-alive traffic and seamless promotion
/// of a refreshed session (first RTP arrival triggers switchover without downstream interruption).
/// </summary>
public class UpstreamOrchestrator : ICameraRelay
{
    private readonly string _cameraId;
    private readonly ILogger _logger;
    private readonly ILocalRtspServer _localServer;
    private readonly BindingSelector _bindingSelector;
    private readonly IOptions<StreamingOptions> _options;

    private readonly Lock _switchLock = new();

    private readonly IUpstreamSession _primarySession;
    private readonly IUpstreamSession _secondarySession;

    private IUpstreamSession? _activeSession;
    private IUpstreamSession? _stagingSession;

    private CancellationTokenSource? _keepaliveCts;
    private Task? _keepaliveTask;
    private IKeepAliveStrategy? _keepAliveStrategy;
    private bool _disposed;

    /// <summary>
    /// Creates a new upstream orchestrator.
    /// </summary>
    /// <param name="cameraId">Camera identifier (local endpoint path).</param>
    /// <param name="logger">Logger instance.</param>
    /// <param name="localServer">Local RTSP server for forwarding.</param>
    /// <param name="bindingSelector">Binding selector for SSRC management.</param>
    /// <param name="options">Streaming options.</param>
    /// <param name="sessionFactory">Factory for creating upstream session instances.</param>
    public UpstreamOrchestrator(
        string cameraId,
        ILogger logger,
        ILocalRtspServer localServer,
        BindingSelector bindingSelector,
        IOptions<StreamingOptions> options,
        IUpstreamSessionFactory sessionFactory)
    {
        _cameraId = cameraId;
        _logger = logger;
        _localServer = localServer;
        _bindingSelector = bindingSelector;
        _options = options;

        _primarySession = sessionFactory.Create(cameraId, AcceptAllServerCertificate, "primary");
        _primarySession.DescribeResolved += result => OnDescribeResolved(_primarySession, result);
        _primarySession.Disconnected += () => OnSessionDisconnected(_primarySession);

        _secondarySession = sessionFactory.Create(cameraId, AcceptAllServerCertificate, "secondary");
        _secondarySession.DescribeResolved += result => OnDescribeResolved(_secondarySession, result);
        _secondarySession.Disconnected += () => OnSessionDisconnected(_secondarySession);
    }

    /// <summary>
    /// Identifier of the camera this orchestrator manages (local endpoint path).
    /// </summary>
    public string CameraId => _cameraId;

    /// <summary>
    /// Indicates whether the primary upstream session has been started.
    /// </summary>
    public bool IsRunning { get; private set; }

    /// <inheritdoc />
    public event Action? Disconnected;

    /// <summary>
    /// Starts the upstream primary session (if not already started) and begins connecting to the supplied
    /// authenticated upstream RTSP URL.
    /// </summary>
    /// <param name="upstreamUrl">Fully qualified upstream RTSP(S) URL including current auth query token.</param>
    /// <param name="cancellationToken">Cancellation token to abort the initial connection attempt.</param>
    public async Task StartAsync(string upstreamUrl, CancellationToken cancellationToken)
    {
        if (IsRunning)
        {
            return;
        }

        _activeSession = _primarySession;
        _stagingSession = null;

        IUpstreamSession session = _primarySession;
        await session.ConnectAsync(
            new Uri(upstreamUrl),
            data => ForwardRtpAsync(session, data),
            data => ForwardRtcpAsync(session, data),
            cancellationToken);
        StartKeepalive(session);

        _logger.LogInformation("[{CameraId}] [sess-{Sess}] Upstream session created and connecting to {Url}", CameraId, _primarySession.InstanceId, upstreamUrl);
        IsRunning = true;
    }

    private void OnDescribeResolved(IUpstreamSession session, SdpResolutionResult result)
    {
        try
        {
            CameraStream stream = new()
            {
                CameraId = CameraId,
                Sdp = result.SdpLines,
                VideoPayloadType = result.VideoPayloadType ?? 97,
                AudioPayloadType = result.AudioPayloadType ?? 96
            };

            _localServer.RegisterOrUpdateCamera(stream);
            _logger.LogInformation("[{CameraId}] [sess-{Sess}] Registered camera with SDP ({Lines} lines).", CameraId, session.InstanceId, result.SdpLines.Length);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[{CameraId}] [sess-{Sess}] Failed to register camera after DESCRIBE.", CameraId, session.InstanceId);
        }
    }

    private void OnSessionDisconnected(IUpstreamSession session)
    {
        // Sessions being torn down after a switchover also disconnect; only the active one matters.
        if (!IsRunning || session != _activeSession)
        {
            return;
        }

        _logger.LogWarning("[{CameraId}] [sess-{Sess}] Active upstream session disconnected.", CameraId, session.InstanceId);
        Disconnected?.Invoke();
    }

    /// <summary>
    /// Initiates an authentication refresh by creating a staging session that connects with the updated auth
    /// token. When the first RTP packet is received the staging session is promoted to primary seamlessly.
    /// </summary>
    /// <param name="newAuthToken">The new auth token (streamToken) returned by the extend operation.</param>
    /// <returns><c>true</c> if a staging session was started; <c>false</c> if refresh is not possible now.</returns>
    public async Task<bool> RefreshAuthAndReplayAsync(string newAuthToken)
    {
        string? baseUrl = _activeSession?.BaseUrl;
        if (string.IsNullOrEmpty(baseUrl))
        {
            return false;
        }

        string updated = AuthQueryUpdater.UpdateAuth(baseUrl, newAuthToken);

        _stagingSession = _activeSession == _primarySession ? _secondarySession : _primarySession;
        IUpstreamSession stagingSession = _stagingSession;
        await stagingSession.ConnectAsync(
            new Uri(updated),
            data => ForwardRtpAsync(stagingSession, data),
            data => ForwardRtcpAsync(stagingSession, data),
            CancellationToken.None);
        _logger.LogInformation("[{CameraId}] [sess-{Sess}] Session started for refresh", CameraId, stagingSession.InstanceId);

        return true;
    }

    private async ValueTask ForwardRtpAsync(IUpstreamSession session, ReadOnlyMemory<byte> data)
    {
        // Promote staging session on first RTP packet.
        if (_stagingSession != null && session == _stagingSession)
        {
            PromoteStagingSession(session);
            _logger.LogInformation("[{CameraId}] [sess-{Sess}] Promoted staging session to active on first RTP", CameraId, session.InstanceId);
        }

        // Only forward active session.
        if (session != _activeSession)
        {
            return;
        }

        await _localServer.ForwardRtpAsync(CameraId, data);
    }

    private async ValueTask ForwardRtcpAsync(IUpstreamSession session, ReadOnlyMemory<byte> data)
    {
        // Only forward active session.
        if (session != _activeSession)
        {
            return;
        }

        await _localServer.ForwardRtcpAsync(CameraId, data);
    }

    private void PromoteStagingSession(IUpstreamSession newSession)
    {
        lock (_switchLock)
        {
            // Double-checked lock; no need to promote again.
            if (_activeSession == newSession)
            {
                return;
            }

            IUpstreamSession? oldSession = _activeSession;
            _activeSession = newSession;
            _stagingSession = null;

            // Clear all SSRC→binding associations for this camera so downstream clients can re-learn
            // the new SSRCs from the staging session without stale mappings from the old primary.
            _bindingSelector.ClearCamera(CameraId);
            _logger.LogInformation("[{CameraId}] [sess-{Sess}] Cleared SSRC bindings for upstream session switchover.", CameraId, newSession.InstanceId);

            StartKeepalive(newSession);

            // Teardown old session fire-and-forget.
            _ = TeardownOldSession(oldSession);
        }
    }

    private async Task TeardownOldSession(IUpstreamSession? oldSession)
    {
        if (oldSession == null)
        {
            return;
        }

        await Safe.InvokeAsync(oldSession.TeardownAsync);
        await Safe.InvokeAsync(oldSession.CloseAsync);

        try
        {
            // Inject a synthetic BYE for the old session's SSRC if upstream did not send one.
            if (oldSession.LastObservedRtpSsrc != null && !oldSession.ByeObserved)
            {
                uint ssrc = oldSession.LastObservedRtpSsrc.Value;
                byte[] bye = RtcpPacketBuilder.BuildBye(ssrc);
                _logger.LogInformation("[{CameraId}] [sess-{Sess}] Injecting synthetic BYE for old SSRC 0x{Ssrc:X8} after promotion.", CameraId, oldSession.InstanceId, ssrc);
                await _localServer.ForwardRtcpAsync(CameraId, bye);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[{CameraId}] Synthetic BYE injection failed.", CameraId);
        }

        _logger.LogInformation("[{CameraId}] [sess-{Sess}] Old session torn down", CameraId, oldSession.InstanceId);
    }

    private void StartKeepalive(IUpstreamSession session)
    {
        if (!_options.Value.KeepaliveEnabled)
        {
            return;
        }

        Safe.Invoke(() => _keepaliveCts?.Cancel());

        _keepaliveCts = new CancellationTokenSource();
        CancellationToken token = _keepaliveCts.Token;
        _keepAliveStrategy ??= KeepAliveStrategyFactory.Create(_options.Value.KeepaliveMode, LoggerFactory.Create(b => { }));
        _keepaliveTask = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                Safe.Invoke(() => _keepAliveStrategy.TrySend(session));

                if (!await Safe.InvokeAsync(async () => await Task.Delay(_options.Value.KeepaliveIntervalSeconds * 1000, token)))
                {
                    break;
                }
            }
        }, token);
    }

    /// <summary>
    /// Disposes the orchestrator, cancelling keep-alive processing and disposing any active upstream sessions.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore().ConfigureAwait(false);
        Dispose(false);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes the orchestrator.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes the orchestrator resources.
    /// </summary>
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        IsRunning = false;

        if (disposing)
        {
            Safe.Invoke(() => _keepaliveCts?.Cancel());
            Safe.Invoke(() => _keepaliveCts?.Dispose());
            Safe.Invoke(() => _primarySession.Dispose());
            Safe.Invoke(() => _secondarySession.Dispose());
        }

        _disposed = true;
    }

    /// <summary>
    /// Asynchronously disposes the orchestrator resources.
    /// </summary>
    protected virtual async ValueTask DisposeAsyncCore()
    {
        Safe.Invoke(() => _keepaliveCts?.Cancel());
        IsRunning = false;

        if (_keepaliveTask != null)
        {
            await Safe.InvokeAsync(async () => await _keepaliveTask);
        }

        Safe.Invoke(() => _keepaliveCts?.Dispose());

        await Safe.InvokeAsync(async () => await _primarySession.DisposeAsync());
        await Safe.InvokeAsync(async () => await _secondarySession.DisposeAsync());

        _activeSession = null;
    }

    private static bool AcceptAllServerCertificate(object? _, X509Certificate? __, X509Chain? ___, SslPolicyErrors ____) => true;
}