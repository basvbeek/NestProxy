using System.Net.Security;

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

using NestProxy.Server.Core;
using NestProxy.Server.Models;
using NestProxy.Server.Streaming.Downstream.Server;
using NestProxy.Server.Streaming.Forwarding.Binding;
using NestProxy.Server.Streaming.Upstream;
using NestProxy.Server.Streaming.Upstream.Sdp;

using Xunit;

namespace NestProxy.Tests.Upstream;

public class UpstreamDisconnectTests
{
    private sealed class TestSession(string instanceId) : IUpstreamSession
    {
        private Func<ReadOnlyMemory<byte>, ValueTask>? _onRtp;

        public string InstanceId => instanceId;
        public string? SessionId => "session";
        public string? BaseUrl => "rtsps://example.com/stream?auth=old";
        public string? AggregateUrl => null;
        public IReadOnlyList<(int data, int control)> InterleavedAssignments => Array.Empty<(int, int)>();
        public uint? LastObservedRtpSsrc => null;
        public bool ByeObserved => false;
        public event Action<SdpResolutionResult>? DescribeResolved { add { } remove { } }
        public event Action? Disconnected;
        public void RaiseDisconnected() => Disconnected?.Invoke();
        public ValueTask ReceiveRtpAsync() => _onRtp!.Invoke(new byte[12]);
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public Task ConnectAsync(Uri url, Func<ReadOnlyMemory<byte>, ValueTask> onRtp, Func<ReadOnlyMemory<byte>, ValueTask> onRtcp, CancellationToken ct)
        {
            _onRtp = onRtp;
            return Task.CompletedTask;
        }
        public Task TeardownAsync() => Task.CompletedTask;
        public Task CloseAsync() => Task.CompletedTask;
        public bool TryGetFirstControlChannel(out int controlChannel) { controlChannel = 1; return true; }
        public void SendOptionsKeepalive() { }
        public void SendGetParameterKeepalive() { }
        public void SendRtcpKeepalive(byte[] packet) { }
        public void SendRtcpKeepalive(byte[] packet, int length) { }
    }

    private sealed class TestSessionFactory : IUpstreamSessionFactory
    {
        public TestSession Primary { get; } = new("primary");
        public TestSession Secondary { get; } = new("secondary");

        public IUpstreamSession Create(string cameraId, RemoteCertificateValidationCallback certValidator, string instanceId)
            => instanceId == "primary" ? Primary : Secondary;
    }

    private sealed class NullLocalServer : ILocalRtspServer
    {
        public void RegisterOrUpdateCamera(CameraStream stream) { }
        public void ForwardRtp(string path, ReadOnlyMemory<byte> rtpPacket) { }
        public ValueTask ForwardRtpAsync(string cameraPath, ReadOnlyMemory<byte> rtpPacket, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public void ForwardRtcp(string path, ReadOnlyMemory<byte> rtcpPacket) { }
        public ValueTask ForwardRtcpAsync(string cameraPath, ReadOnlyMemory<byte> rtcpPacket, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public string BuildPublicUrl(string host, int port, string path) => $"rtsp://{host}:{port}/{path}";
        public string? DescribeForTest(string cameraPath) => null;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private static (UpstreamOrchestrator orchestrator, TestSessionFactory sessions) CreateOrchestrator()
    {
        TestSessionFactory sessions = new();
        IOptions<StreamingOptions> options = Options.Create(new StreamingOptions { KeepaliveEnabled = false });
        UpstreamOrchestrator orchestrator = new("cam", NullLogger.Instance, new NullLocalServer(), new BindingSelector(), options, sessions);
        return (orchestrator, sessions);
    }

    [Fact]
    public async Task ActiveSessionDisconnect_RaisesDisconnected()
    {
        var (orchestrator, sessions) = CreateOrchestrator();
        int raised = 0;
        orchestrator.Disconnected += () => raised++;
        await orchestrator.StartAsync("rtsps://example.com/stream?auth=old", TestContext.Current.CancellationToken);

        sessions.Primary.RaiseDisconnected();

        Assert.Equal(1, raised);
    }

    [Fact]
    public void DisconnectBeforeStart_IsIgnored()
    {
        var (orchestrator, sessions) = CreateOrchestrator();
        int raised = 0;
        orchestrator.Disconnected += () => raised++;

        sessions.Primary.RaiseDisconnected();

        Assert.Equal(0, raised);
    }

    [Fact]
    public async Task StagingSessionDisconnect_IsIgnored()
    {
        var (orchestrator, sessions) = CreateOrchestrator();
        int raised = 0;
        orchestrator.Disconnected += () => raised++;
        await orchestrator.StartAsync("rtsps://example.com/stream?auth=old", TestContext.Current.CancellationToken);
        Assert.True(await orchestrator.RefreshAuthAndReplayAsync("new"));

        sessions.Secondary.RaiseDisconnected();

        Assert.Equal(0, raised);
    }

    [Fact]
    public async Task DisconnectAfterPromotion_OnlyRaisedForNewActiveSession()
    {
        var (orchestrator, sessions) = CreateOrchestrator();
        int raised = 0;
        orchestrator.Disconnected += () => raised++;
        await orchestrator.StartAsync("rtsps://example.com/stream?auth=old", TestContext.Current.CancellationToken);
        Assert.True(await orchestrator.RefreshAuthAndReplayAsync("new"));
        await sessions.Secondary.ReceiveRtpAsync(); // first RTP promotes the staging session

        sessions.Primary.RaiseDisconnected();
        Assert.Equal(0, raised);

        sessions.Secondary.RaiseDisconnected();
        Assert.Equal(1, raised);
    }
}
