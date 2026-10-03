using System.Net;
using System.Text;

using NestProxy.Rtsp.Client;
using NestProxy.Rtsp.Messages;
using NestProxy.Rtsp.Protocol;
using NestProxy.Rtsp.Transport;

using Xunit;

namespace NestProxy.Rtsp.Tests.Rtsp.Client;

public sealed class RtspConnectionTests
{
    [Fact]
    public void Constructor_WithNullTransport_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new RtspConnection(null!));
    }

    [Fact]
    public void RemoteAddress_ReturnsTransportRemoteAddress()
    {
        FakeTransport transport = new() { RemoteAddress = "192.168.1.100" };
        RtspConnection connection = new(transport);

        Assert.Equal("192.168.1.100", connection.RemoteAddress);
    }

    [Fact]
    public void Connected_ReturnsTransportConnectedState()
    {
        FakeTransport transport = new() { Connected = true };
        RtspConnection connection = new(transport);

        Assert.True(connection.Connected);
    }

    [Fact]
    public async Task SendMessageAsync_AddsCSeqToRequest()
    {
        FakeTransport transport = new();
        RtspConnection connection = new(transport);

        RtspRequestOptions request = new() { RtspUri = new Uri("rtsp://example.com") };
        await connection.SendMessageAsync(request, TestContext.Current.CancellationToken);

        Assert.NotEmpty(request.Headers["CSeq"]);
        Assert.Equal("1", request.Headers["CSeq"]);
    }

    [Fact]
    public async Task SendMessageAsync_IncrementsCSeqOnMultipleSends()
    {
        FakeTransport transport = new();
        RtspConnection connection = new(transport);

        RtspRequestOptions request1 = new() { RtspUri = new Uri("rtsp://example.com") };
        RtspRequestOptions request2 = new() { RtspUri = new Uri("rtsp://example.com") };

        await connection.SendMessageAsync(request1, TestContext.Current.CancellationToken);
        await connection.SendMessageAsync(request2, TestContext.Current.CancellationToken);

        Assert.Equal("1", request1.Headers["CSeq"]);
        Assert.Equal("2", request2.Headers["CSeq"]);
    }

    [Fact]
    public async Task SendMessageAsync_DoesNotOverwriteExistingCSeq()
    {
        FakeTransport transport = new();
        RtspConnection connection = new(transport);

        RtspRequestOptions request = new() { RtspUri = new Uri("rtsp://example.com") };
        request.Headers["CSeq"] = "999";

        await connection.SendMessageAsync(request, TestContext.Current.CancellationToken);

        Assert.Equal("999", request.Headers["CSeq"]);
    }

    [Fact]
    public async Task SendMessageAsync_SerializesAndWritesToTransport()
    {
        MemoryStream writeStream = new();
        FakeTransport transport = new() { Stream = writeStream };
        RtspConnection connection = new(transport);

        RtspRequestOptions request = new() { RtspUri = new Uri("rtsp://example.com/test") };
        await connection.SendMessageAsync(request, TestContext.Current.CancellationToken);

        Assert.True(writeStream.Length > 0);

        string sent = Encoding.ASCII.GetString(writeStream.ToArray());
        Assert.Contains("OPTIONS rtsp://example.com/test RTSP/1.0", sent);
    }

    [Fact]
    public async Task MessageReceived_ChannelYieldsWhenMessageParsed()
    {
        MemoryStream stream = new();
        byte[] responseData = Encoding.ASCII.GetBytes("RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n");
        await stream.WriteAsync(responseData, TestContext.Current.CancellationToken);
        stream.Position = 0;

        FakeTransport transport = new() { Stream = stream };
        RtspConnection connection = new(transport);

        connection.Start();
        RtspMessage message = await connection.Messages.ReadAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(message);
        Assert.IsType<RtspResponse>(message);

        RtspResponse response = (RtspResponse)message;
        Assert.Equal(200, response.ReturnCode);
    }

    [Fact]
    public async Task DataReceived_ChannelYieldsInterleavedFrame()
    {
        MemoryStream stream = new();

        byte[] frameData = [0x24, 0x00, 0x00, 0x04, 0xDE, 0xAD, 0xBE, 0xEF];
        await stream.WriteAsync(frameData, TestContext.Current.CancellationToken);
        stream.Position = 0;

        FakeTransport transport = new() { Stream = stream };
        RtspConnection connection = new(transport);

        connection.Start();
        InterleavedFrame frame = await connection.InterleavedFrames.ReadAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, frame.Channel);
        Assert.Equal(4, frame.Data.Length);
        frame.Dispose();
    }

    [Fact]
    public void Start_ThrowsIfAlreadyStarted()
    {
        FakeTransport transport = new();
        RtspConnection connection = new(transport);

        connection.Start();

        Assert.Throws<InvalidOperationException>(() => connection.Start());
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        FakeTransport transport = new();
        RtspConnection connection = new(transport);

        connection.Dispose();
        connection.Dispose(); // Should not throw
    }

    [Fact]
    public async Task SendMessageAsync_ThrowsAfterDispose()
    {
        FakeTransport transport = new();
        RtspConnection connection = new(transport);
        connection.Dispose();

        RtspRequestOptions request = new() { RtspUri = new Uri("rtsp://example.com") };

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await connection.SendMessageAsync(request, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ReadFailure_RaisesDisconnectedOnce()
    {
        FakeTransport transport = new() { Stream = new FailingStream(waitForCancellation: false) };
        RtspConnection connection = new(transport);
        int raised = 0;
        connection.Disconnected += (_, _) => Interlocked.Increment(ref raised);

        connection.Start();
        await connection.Messages.Completion;
        await connection.DisposeAsync(); // waits for the read loop to finish

        Assert.Equal(1, raised);
    }

    [Fact]
    public async Task Dispose_DoesNotRaiseDisconnected()
    {
        FailingStream stream = new(waitForCancellation: true);
        FakeTransport transport = new() { Stream = stream };
        RtspConnection connection = new(transport);
        int raised = 0;
        connection.Disconnected += (_, _) => Interlocked.Increment(ref raised);

        connection.Start();
        await stream.ReadStarted.Task.WaitAsync(TestContext.Current.CancellationToken);
        await connection.DisposeAsync(); // read fails with an IOException once the connection is disposed

        Assert.Equal(0, raised);
    }

    /// <summary>
    /// Stream whose reads fail like a dropped socket, either immediately or once the read is cancelled.
    /// </summary>
    private sealed class FailingStream(bool waitForCancellation) : MemoryStream
    {
        public TaskCompletionSource ReadStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            ReadStarted.TrySetResult();
            if (waitForCancellation)
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                }
            }

            throw new IOException("Connection reset");
        }
    }

    private sealed class FakeTransport : IRtspTransport
    {
        public bool Connected { get; set; } = true;
        public string RemoteAddress { get; set; } = "127.0.0.1";
        public IPEndPoint RemoteEndPoint { get; set; } = new(IPAddress.Loopback, 554);
        public IPEndPoint LocalEndPoint { get; set; } = new(IPAddress.Loopback, 12345);
        public Stream? Stream { get; set; }
        public List<byte> BytesWritten { get; } = [];

        public uint NextCommandIndex() => 1;

        public Stream GetStream()
        {
            return Stream ?? new MemoryStream();
        }

        public Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            BytesWritten.AddRange(buffer.Skip(offset).Take(count));
            return Task.CompletedTask;
        }

        public Task WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
        {
            BytesWritten.AddRange(buffer.ToArray());
            return Task.CompletedTask;
        }

        public Task<RtspResponse> SendRequestAsync(RtspRequest request, CancellationToken cancellationToken)
        {
            return Task.FromResult(request.CreateResponse());
        }

        public Task SendResponseAsync(RtspResponse response, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task<object> ReceiveMessageAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult<object>(new RtspResponse());
        }

        public Task<byte> PeekByteAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult((byte)0);
        }

        public Task<InterleavedData> ReceiveInterleavedDataAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(new InterleavedData { Channel = 0, Data = [] });
        }

        public Task SendInterleavedDataAsync(byte channel, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            BytesWritten.AddRange(data.ToArray());
            return Task.CompletedTask;
        }

        public void Close() { }
        public void Reconnect() { }
        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
