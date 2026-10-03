using Microsoft.Extensions.Logging.Abstractions;

using NestProxy.Server.Models;
using NestProxy.Server.Streaming.Upstream;
using NestProxy.Server.Streaming.Upstream.KeepAlive;
using NestProxy.Server.Streaming.Upstream.Sdp;

using Xunit;

namespace NestProxy.Tests.Upstream;

public class KeepAliveStrategyTests
{
    private sealed class TestSession : IUpstreamSession
    {
        public string InstanceId => "test";
        public string? SessionId => "session";
        public string? BaseUrl => null;
        public string? AggregateUrl => null;
        public IReadOnlyList<(int data, int control)> InterleavedAssignments => Array.Empty<(int, int)>();
        public uint? LastObservedRtpSsrc => null;
        public bool ByeObserved => false;
        public event Action<SdpResolutionResult>? DescribeResolved { add { } remove { } }
        public event Action? Disconnected { add { } remove { } }
        public bool ControlChannelReady { get; set; } = true;
        public bool ThrowOnGetParameter { get; set; }
        public bool ThrewOnce { get; private set; }
        public int OptionsCount { get; private set; }
        public int GetParameterCount { get; private set; }
        public int RtcpKeepAliveCount { get; private set; }
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public Task ConnectAsync(Uri url, Func<ReadOnlyMemory<byte>, ValueTask> onRtp, Func<ReadOnlyMemory<byte>, ValueTask> onRtcp, CancellationToken ct) => Task.CompletedTask;
        public Task TeardownAsync() => Task.CompletedTask;
        public Task CloseAsync() => Task.CompletedTask;
        public bool TryGetFirstControlChannel(out int controlChannel) { controlChannel = 1; return ControlChannelReady; }
        public void SendOptionsKeepalive() => OptionsCount++;
        public void SendGetParameterKeepalive()
        {
            GetParameterCount++;
            if (ThrowOnGetParameter && !ThrewOnce)
            {
                ThrewOnce = true;
                throw new InvalidOperationException("simulated");
            }
        }
        public void SendRtcpKeepalive(byte[] packet) => RtcpKeepAliveCount++;
        public void SendRtcpKeepalive(byte[] packet, int length) => RtcpKeepAliveCount++;
    }

    [Fact]
    public void OptionsStrategy_SendsOptions()
    {
        IKeepAliveStrategy strat = KeepAliveStrategyFactory.Create(KeepaliveMode.Options, NullLoggerFactory.Instance);
        TestSession session = new();
        bool sent = strat.TrySend(session);
        Assert.True(sent);
        Assert.Equal(1, session.OptionsCount);
        Assert.Equal(0, session.GetParameterCount);
    }

    [Fact]
    public void GetParameterStrategy_FallbacksToOptionsAfterFailure()
    {
        IKeepAliveStrategy strat = KeepAliveStrategyFactory.Create(KeepaliveMode.GetParameter, NullLoggerFactory.Instance);
        TestSession session = new() { ThrowOnGetParameter = true };
        bool first = strat.TrySend(session); // throws inside; fallback sends OPTIONS
        bool second = strat.TrySend(session); // now should send OPTIONS directly
        Assert.True(first);
        Assert.True(second);
        Assert.Equal(1, session.GetParameterCount); // not retried
        Assert.Equal(2, session.OptionsCount); // fallback + second call
    }

    [Fact]
    public void RtcpStrategy_SkipsWithoutControlChannel()
    {
        IKeepAliveStrategy strat = KeepAliveStrategyFactory.Create(KeepaliveMode.Rtcp, NullLoggerFactory.Instance);
        TestSession session = new() { ControlChannelReady = false };
        bool sent = strat.TrySend(session);
        Assert.False(sent);
        Assert.Equal(0, session.RtcpKeepAliveCount);
    }

    [Fact]
    public void RtcpStrategy_SendsWithControlChannel()
    {
        IKeepAliveStrategy strat = KeepAliveStrategyFactory.Create(KeepaliveMode.Rtcp, NullLoggerFactory.Instance);
        TestSession session = new() { ControlChannelReady = true };
        bool sent = strat.TrySend(session);
        if (sent)
        {
            Assert.Equal(1, session.RtcpKeepAliveCount);
        }
        else
        {
            Assert.Equal(0, session.RtcpKeepAliveCount);
        }
    }
}