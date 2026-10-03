using System.Net.Security;

using NestProxy.Rtsp.Client;
using NestProxy.Rtsp.Messages;
using NestProxy.Rtsp.Protocol;
using NestProxy.Rtsp.Transport;

namespace NestProxy.Server.Streaming.Upstream;

/// <summary>
/// Represents one upstream RTSP session (aggregate or per-track) to Nest. Handles OPTIONS -> DESCRIBE -> SETUP(s) -> PLAY
/// sequencing, tracks interleaved channel assignment, forwards RTP/RTCP packets, and offers basic keep-alive helpers.
/// Raises <see cref="Disconnected"/> when the connection drops; higher layers own lifetime supervision.
/// </summary>
public sealed class UpstreamSession(
    string cameraId,
    ILogger logger,
    RemoteCertificateValidationCallback certValidator,
    string instanceId,
    IRtspResponseFormatter formatter,
    IUpstreamSessionStateMachine stateMachine,
    IUpstreamSessionStatistics statistics,
    int frameChannelCapacity = 0) : IUpstreamSession
{
    private RtspConnection? _connection;
    private IRtspTransport? _transport;
    private readonly int _frameChannelCapacity = frameChannelCapacity;
    private int _lastDroppedFrames;

    private CancellationTokenSource? _processingCts;
    private Task? _messageLoopTask;
    private Task? _frameLoopTask;
    private Func<ReadOnlyMemory<byte>, ValueTask>? _rtpHandler;
    private Func<ReadOnlyMemory<byte>, ValueTask>? _rtcpHandler;
    private bool _disposed;

    /// <inheritdoc />
    public string InstanceId { get; } = instanceId;

    /// <inheritdoc />
    public string? SessionId => stateMachine.SessionId;

    /// <inheritdoc />
    public string? BaseUrl => stateMachine.BaseUrl;

    /// <inheritdoc />
    public string? AggregateUrl => stateMachine.AggregateUrl;

    /// <inheritdoc />
    public IReadOnlyList<(int data, int control)> InterleavedAssignments => stateMachine.InterleavedAssignments;

    /// <inheritdoc />
    public event Action<SdpResolutionResult>? DescribeResolved
    {
        add => stateMachine.DescribeResolved += value;
        remove => stateMachine.DescribeResolved -= value;
    }

    /// <inheritdoc />
    public event Action? Disconnected;

    /// <inheritdoc />
    public uint? LastObservedRtpSsrc { get; private set; }

    /// <inheritdoc />
    public bool ByeObserved { get; private set; }

    /// <inheritdoc />
    public async Task ConnectAsync(Uri url, Func<ReadOnlyMemory<byte>, ValueTask> onRtp, Func<ReadOnlyMemory<byte>, ValueTask> onRtcp, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(url);
        ArgumentNullException.ThrowIfNull(onRtp);
        ArgumentNullException.ThrowIfNull(onRtcp);

        _rtpHandler = onRtp;
        _rtcpHandler = onRtcp;

        // Reset state and recreate channel for new connection.
        await CloseAsync();
        await ResetAsync();

        _transport = RtspTransportFactory.CreateFromUrl(url, certValidator);
        _ = _transport.GetStream(); // ensures transport is connected

        _connection = new RtspConnection(_transport, _frameChannelCapacity);
        _connection.Disconnected += (_, _) => Disconnected?.Invoke();
        _connection.Start();

        // Give the read loop a moment to start.
        await Task.Delay(10, ct);

        // Start processing loops.
        _processingCts = new CancellationTokenSource();
        CancellationToken processingToken = _processingCts.Token;
        _messageLoopTask = ProcessMessagesAsync(processingToken);
        _frameLoopTask = ProcessFramesAsync(processingToken);

        // Start RTSP session setup sequence by sending OPTIONS.
        RtspRequest optionsRequest = RtspRequest.CreateOptions(url);
        string reqHdrs = formatter.FormatRequestHeaders(optionsRequest, "CSeq", "Accept");
        logger.LogInformation("[{CameraId}] [sess-{Sess}] Sending OPTIONS {Url} headers: {Headers}.", cameraId, InstanceId, url, reqHdrs);
        await _connection.SendMessageAsync(optionsRequest, ct);
    }

    private async Task ResetAsync()
    {
        stateMachine.Reset();
        statistics.Reset();

        LastObservedRtpSsrc = null;
        ByeObserved = false;

        await StopProcessingLoopsAsync();

        _connection = null;
        _transport = null;
    }

    private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
    {
        RtspConnection? connection = _connection;
        if (connection == null)
        {
            return;
        }

        try
        {
            await foreach (RtspMessage message in connection.Messages.ReadAllAsync(cancellationToken))
            {
                await HandleRtspMessageAsync(connection, message).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogDebug("[{CameraId}] [sess-{Sess}] Message processing cancelled.", cameraId, InstanceId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{CameraId}] [sess-{Sess}] Message processing loop failed.", cameraId, InstanceId);
        }
    }

    private async Task HandleRtspMessageAsync(RtspConnection connection, RtspMessage message)
    {
        logger.LogDebug("[{CameraId}] [sess-{Sess}] Received message type: {Type}.", cameraId, InstanceId, message.GetType().Name);

        if (message is not RtspResponse resp)
        {
            logger.LogDebug("[{CameraId}] [sess-{Sess}] Ignoring non-response message.", cameraId, InstanceId);
            return;
        }

        RtspRequest? orig = resp.OriginalRequest;
        if (orig == null)
        {
            logger.LogWarning("[{CameraId}] [sess-{Sess}] Response has no OriginalRequest! CSeq: {CSeq}, ReturnCode: {Code}.",
                cameraId,
                InstanceId,
                resp.Headers.TryGetValue("CSeq", out string? cseq) ? cseq : "none",
                resp.ReturnCode);
            return;
        }

        await stateMachine.HandleResponseAsync(connection, resp, formatter).ConfigureAwait(false);
    }

    private async Task ProcessFramesAsync(CancellationToken cancellationToken)
    {
        RtspConnection? connection = _connection;
        if (connection == null)
        {
            return;
        }

        logger.LogInformation("[{CameraId}] [sess-{Sess}] Packet processing loop started", cameraId, InstanceId);

        try
        {
            await foreach (var frame in connection.InterleavedFrames.ReadAllAsync(cancellationToken))
            {
                try
                {
                    TrackDroppedFrames(connection);
                    await HandleFrameAsync(frame);
                }
                finally
                {
                    frame.Dispose();
                }
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogDebug("[{CameraId}] [sess-{Sess}] Packet processing cancelled", cameraId, InstanceId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{CameraId}] [sess-{Sess}] Packet processing loop failed", cameraId, InstanceId);
        }
        finally
        {
            TrackDroppedFrames(connection);
            logger.LogInformation("[{CameraId}] [sess-{Sess}] Packet processing loop exited", cameraId, InstanceId);
        }
    }

    private void TrackDroppedFrames(RtspConnection connection)
    {
        int currentDropped = connection.DroppedInterleavedFrames;
        if (currentDropped < _lastDroppedFrames)
        {
            int rolloverDelta = currentDropped >= 0 ? currentDropped : 0;
            if (rolloverDelta > 0)
            {
                statistics.RecordPacketsDropped(rolloverDelta);
            }

            _lastDroppedFrames = currentDropped < 0 ? 0 : currentDropped;
            return;
        }

        int delta = currentDropped - _lastDroppedFrames;
        if (delta > 0)
        {
            statistics.RecordPacketsDropped(delta);
            _lastDroppedFrames = currentDropped;
        }
    }

    private async ValueTask HandleFrameAsync(InterleavedFrame frame)
    {
        ReadOnlyMemory<byte> data = frame.Data;
        if (data.Length < 4)
        {
            return;
        }

        ReadOnlySpan<byte> span = data.Span;
        int packetType = span[1] & 0xFF;
        bool isRtcp = (uint)(packetType - 200) <= 11;

        if (isRtcp)
        {
            if (packetType == 203 && span.Length >= 8)
            {
                uint byeSsrc = (uint)((span[4] << 24) | (span[5] << 16) | (span[6] << 8) | span[7]);
                if (LastObservedRtpSsrc != null && byeSsrc == LastObservedRtpSsrc)
                {
                    ByeObserved = true;
                }
            }

            statistics.RecordRtcp();

            Func<ReadOnlyMemory<byte>, ValueTask>? rtcpHandler = _rtcpHandler;
            if (rtcpHandler != null)
            {
                try
                {
                    await rtcpHandler.Invoke(data);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "[{CameraId}] [sess-{Sess}] RTCP handler threw exception", cameraId, InstanceId);
                }
            }
        }
        else if (data.Length >= 12)
        {
            statistics.RecordRtp();
            uint ssrc = (uint)((span[8] << 24) | (span[9] << 16) | (span[10] << 8) | span[11]);
            LastObservedRtpSsrc ??= ssrc;

            Func<ReadOnlyMemory<byte>, ValueTask>? rtpHandler = _rtpHandler;
            if (rtpHandler != null)
            {
                try
                {
                    await rtpHandler.Invoke(data);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "[{CameraId}] [sess-{Sess}] RTP handler threw exception", cameraId, InstanceId);
                }
            }
        }
    }

    /// <inheritdoc />
    public bool TryGetFirstControlChannel(out int controlChannel)
    {
        return stateMachine.TryGetFirstControlChannel(out controlChannel);
    }

    /// <inheritdoc />
    public void SendOptionsKeepalive()
    {
        if (_connection == null || string.IsNullOrEmpty(BaseUrl))
        {
            return;
        }

        RtspRequest opt = RtspRequest.CreateOptions(new Uri(BaseUrl!));
        string reqHdrs = formatter.FormatRequestHeaders(opt, "CSeq", "Accept");
        logger.LogInformation("[{CameraId}] [sess-{Sess}] Sending OPTIONS {Url} headers: {Headers}.", cameraId, InstanceId, opt.RtspUri, reqHdrs);
        _ = _connection.SendMessageAsync(opt);
    }

    /// <inheritdoc />
    public void SendGetParameterKeepalive()
    {
        if (_connection == null || string.IsNullOrEmpty(BaseUrl) || string.IsNullOrEmpty(SessionId))
        {
            return;
        }

        string target = AggregateUrl ?? BaseUrl!;
        RtspRequest kp = RtspRequest.CreateGetParameter(new Uri(target), SessionId);
        string reqHdrs = formatter.FormatRequestHeaders(kp, "CSeq", "Session");
        logger.LogInformation("[{CameraId}] [sess-{Sess}] Sending GET_PARAMETER {Url} headers: {Headers}.", cameraId, InstanceId, target, reqHdrs);
        _ = _connection.SendMessageAsync(kp);
    }

    /// <inheritdoc />
    public void SendRtcpKeepalive(byte[] packet) => SendRtcpKeepalive(packet, packet?.Length ?? 0);

    /// <inheritdoc />
    public void SendRtcpKeepalive(byte[] packet, int length)
    {
        if (_transport == null || !TryGetFirstControlChannel(out int ctrl) || packet == null || length <= 0)
        {
            return;
        }

        try
        {
            Stream? stream = _transport.GetStream();
            if (stream == null)
            {
                return;
            }

            Span<byte> hdr = stackalloc byte[4];
            hdr[0] = 0x24; // '$'
            hdr[1] = (byte)ctrl;
            ushort len = (ushort)length;
            hdr[2] = (byte)(len >> 8);
            hdr[3] = (byte)(len & 0xFF);
            stream.Write(hdr);
            stream.Write(packet, 0, length);
            stream.Flush();
        }
        catch
        {
        }
    }

    /// <inheritdoc />
    public async Task TeardownAsync()
    {
        if (_connection == null || string.IsNullOrEmpty(SessionId))
        {
            return;
        }

        string target = AggregateUrl ?? BaseUrl ?? string.Empty;
        if (string.IsNullOrEmpty(target) || !Uri.TryCreate(target, UriKind.Absolute, out Uri? targetUri))
        {
            return;
        }

        RtspRequest td = RtspRequest.CreateTeardown(targetUri, SessionId);
        string reqHdrs = formatter.FormatRequestHeaders(td, "CSeq", "Session");
        logger.LogInformation("[{CameraId}] [sess-{Sess}] Sending TEARDOWN {Url} headers: {Headers}.", cameraId, InstanceId, target, reqHdrs);
        await _connection.SendMessageAsync(td);
    }

    private async Task StopProcessingLoopsAsync()
    {
        if (_processingCts != null)
        {
            await _processingCts.CancelAsync();
            _processingCts.Dispose();
            _processingCts = null;
        }

        if (_messageLoopTask != null)
        {
            await _messageLoopTask;
            _messageLoopTask = null;
        }

        if (_frameLoopTask != null)
        {
            await _frameLoopTask;
            _frameLoopTask = null;
        }
    }

    /// <inheritdoc />
    public async Task CloseAsync()
    {
        await StopProcessingLoopsAsync();

        Safe.Invoke(() =>
        {
            _connection?.Dispose(); // Disposing connection disposes transport too (usually)
            _transport?.Dispose();
        });
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        // Synchronous clean-up
        _processingCts?.Cancel();
        _processingCts?.Dispose();

        // Fire-and-forget async cleanup if possible, or just sync disposal
        // Since Connection is IDisposable, we call it.
        _connection?.Dispose();
        _transport?.Dispose();

        _disposed = true;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await StopProcessingLoopsAsync();

        if (_connection != null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }

        if (_transport is IAsyncDisposable ad)
        {
            await ad.DisposeAsync().ConfigureAwait(false);
        }
        else
        {
            _transport?.Dispose();
        }

        _disposed = true;
    }
}