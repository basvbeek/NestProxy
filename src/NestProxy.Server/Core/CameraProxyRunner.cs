using Microsoft.Extensions.Options;

using NestProxy.Server.Services;

namespace NestProxy.Server.Core;

/// <summary>
/// Background runner that maintains a long-lived upstream RTSP relay for a single camera. It is responsible for:
/// <list type="bullet">
/// <item><description>Polling the database for the camera's enabled state.</description></item>
/// <item><description>Generating a fresh upstream RTSP stream (URL + tokens) when needed.</description></item>
/// <item><description>Starting an <see cref="ICameraRelay"/> to forward media into the local RTSP server.</description></item>
/// <item><description>Scheduling periodic stream extension before expiry and performing seamless auth refresh.</description></item>
/// </list>
/// </summary>
public class CameraProxyRunner(
    string cameraId,
    IServiceScopeFactory scopeFactory,
    IOptions<StreamingOptions> options,
    ICameraRuntimeState runtimeState,
    ILogger<CameraProxyRunner> logger) : IDisposable, IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();

    private Task? _loopTask;
    private ICameraRelay? _relay;
    private bool _disposed;

    /// <summary>
    /// Camera identifier this runner manages (local endpoint path).
    /// </summary>
    public string CameraId { get; } = cameraId;

    /// <summary>
    /// Starts the background processing loop if it is not already running.
    /// </summary>
    /// <remarks>
    /// Subsequent calls to this method have no effect if the loop is already active. This method is
    /// not thread-safe; concurrent calls may result in multiple loops being started.
    /// </remarks>
    public void Start()
    {
        if (_loopTask != null)
        {
            return;
        }

        _loopTask = Task.Run(RunAsync);
    }

    /// <summary>
    /// Requests graceful shutdown of the runner and awaits completion of the internal loop.
    /// Safe to call multiple times.
    /// </summary>
    public async Task StopAsync()
    {
        _cts.Cancel();
        runtimeState.SetRunning(CameraId, false);

        if (_loopTask != null)
        {
            await Safe.InvokeAsync(async () => await _loopTask, logger, $"camera {CameraId} runner task");
            _loopTask = null;
        }

        if (_relay != null)
        {
            await Safe.InvokeAsync(async () => await _relay.DisposeAsync(), logger, $"camera {CameraId} relay disposal");
            _relay = null;
        }
    }

    private async Task RunAsync()
    {
        logger.LogInformation("[{CameraId}] Camera runner starting.", CameraId);
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                await RunCameraSessionAsync();
            }
            catch (TaskCanceledException)
            {
                // Expected during shutdown.
                break;
            }
            catch (Exception ex)
            {
                // Log but continue - allow runner to recover on next iteration.
                logger.LogError(ex, "[{CameraId}] Unexpected error in camera session - will retry", CameraId);
                await Task.Delay(TimeSpan.FromSeconds(5), _cts.Token);
            }
        }

        logger.LogInformation("[{CameraId}] Camera runner stopped.", CameraId);
    }

    private async Task RunCameraSessionAsync()
    {
        using var scope = scopeFactory.CreateScope();
        var cameraRepository = scope.ServiceProvider.GetRequiredService<ICameraRepository>();
        var camera = await cameraRepository.GetByIdAsync(CameraId, _cts.Token);

        if (camera?.IsEnabled != true)
        {
            // Camera disabled - wait before checking again.
            await Task.Delay(TimeSpan.FromSeconds(5), _cts.Token);
            return;
        }

        // Always acquire a fresh RTSP URL.
        var sdm = scope.ServiceProvider.GetRequiredService<GoogleSdmService>();
        RtspStreamResult? rtspStream = await sdm.GenerateRtspStreamAsync(camera, _cts.Token);

        if (rtspStream == null)
        {
            camera.LastError = "Failed to generate RTSP";
            await cameraRepository.SaveChangesAsync(_cts.Token);

            // Throw to trigger retry with backoff.
            throw new InvalidOperationException($"Failed to generate RTSP stream for camera {CameraId}");
        }

        camera.LastRtspUrl = rtspStream.RtspUrl;
        camera.StreamExtensionToken = rtspStream.StreamExtensionToken;
        camera.ExpiresAt = rtspStream.ExpiresAt;
        camera.LastError = null;
        await cameraRepository.SaveChangesAsync(_cts.Token);

        logger.LogInformation("[{CameraId}] Starting RTSP relay with upstream URL: {Url}", CameraId, camera.LastRtspUrl);

        // Dispose existing relay if present before creating a new one.
        if (_relay != null)
        {
            logger.LogInformation("[{CameraId}] Disposing existing relay before creating new session", CameraId);
            runtimeState.SetRunning(CameraId, false);
            await Safe.InvokeAsync(async () => await _relay.DisposeAsync(), logger, $"camera {CameraId} relay disposal");
            _relay = null;
        }

        var factory = scope.ServiceProvider.GetRequiredService<IUpstreamOrchestratorFactory>();
        _relay = factory.Create(CameraId);

        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _relay.Disconnected += () => disconnected.TrySetResult();

        await _relay.StartAsync(camera.LastRtspUrl!, _cts.Token);
        runtimeState.SetRunning(CameraId, true);

        // Start stream token extension loop.
        await ExtensionLoopAsync(camera.ExpiresAt!.Value, disconnected.Task);
    }

    private async Task ExtensionLoopAsync(DateTime initialExpiry, Task disconnected)
    {
        while (!_cts.IsCancellationRequested)
        {
            var delay = initialExpiry - DateTime.UtcNow - TimeSpan.FromSeconds(options.Value.StreamExtensionOffsetSeconds);
            if (delay < TimeSpan.FromSeconds(10))
            {
                delay = TimeSpan.FromSeconds(10);
            }

            try
            {
                await Task.WhenAny(Task.Delay(delay, _cts.Token), disconnected);
            }
            catch (TaskCanceledException)
            {
                // Expected during shutdown
                return;
            }

            if (_cts.IsCancellationRequested)
            {
                return;
            }

            if (disconnected.IsCompleted)
            {
                // Return to let the outer loop regenerate the session.
                return;
            }

            try
            {
                using var scope = scopeFactory.CreateScope();
                var cameraRepository = scope.ServiceProvider.GetRequiredService<ICameraRepository>();
                var camera = await cameraRepository.GetByIdAsync(CameraId, _cts.Token);

                if (camera?.IsEnabled != true)
                {
                    logger.LogInformation("[{CameraId}] Camera disabled during extension loop - exiting", CameraId);
                    return;
                }

                var sdm = scope.ServiceProvider.GetRequiredService<GoogleSdmService>();
                StreamExtensionResult? res = await sdm.ExtendRtspStreamAsync(camera, _cts.Token);

                if (res == null)
                {
                    logger.LogWarning("[{CameraId}] Stream extend failed - will regenerate session", CameraId);
                    camera.LastError = "Extend failed";
                    await cameraRepository.SaveChangesAsync(_cts.Token);

                    // Exit extension loop to trigger session regeneration in outer loop.
                    throw new InvalidOperationException($"Failed to extend stream for camera {CameraId}");
                }

                camera.StreamExtensionToken = res.StreamExtensionToken;
                camera.ExpiresAt = res.ExpiresAt;
                camera.LastError = null;
                await cameraRepository.SaveChangesAsync(_cts.Token);
                initialExpiry = camera.ExpiresAt ?? DateTime.UtcNow.AddMinutes(5);

                logger.LogDebug("[{CameraId}] Stream extended successfully until {Expiry}", CameraId, initialExpiry);

                // When a new extension token arrives, update upstream auth and replay without re-SETUP teardown.
                string token = res.StreamToken;
                if (!string.IsNullOrWhiteSpace(token) && _relay != null)
                {
                    bool ok = await _relay.RefreshAuthAndReplayAsync(token);
                    if (!ok)
                    {
                        logger.LogDebug("[{CameraId}] Replay after token refresh was not performed (relay not ready or error).", CameraId);
                    }
                }
            }
            catch (TaskCanceledException)
            {
                // Expected during shutdown.
                return;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "[{CameraId}] Exception during stream extension - triggering session regeneration", CameraId);
                throw; // Re-throw to trigger outer loop recovery.
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore().ConfigureAwait(false);
        Dispose(false);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes the runner resources.
    /// </summary>
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        if (disposing)
        {
            _cts.Cancel();
            runtimeState.SetRunning(CameraId, false);
            _relay?.Dispose();
            _cts.Dispose();
        }

        _disposed = true;
    }

    /// <summary>
    /// Asynchronously disposes the runner resources.
    /// </summary>
    protected virtual async ValueTask DisposeAsyncCore()
    {
        _cts.Cancel();

        if (_loopTask != null)
        {
            await Safe.InvokeAsync(async () => await _loopTask, logger, $"camera {CameraId} runner task");
            _loopTask = null;
        }

        if (_relay != null)
        {
            await _relay.DisposeAsync().ConfigureAwait(false);
            _relay = null;
        }

        runtimeState.SetRunning(CameraId, false);
        _cts.Dispose();
    }
}