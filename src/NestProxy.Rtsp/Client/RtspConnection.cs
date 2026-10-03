using System.Threading.Channels;

using NestProxy.Rtsp.Messages;
using NestProxy.Rtsp.Protocol;
using NestProxy.Rtsp.Transport;

namespace NestProxy.Rtsp.Client;

/// <summary>
/// Represents an RTSP connection that handles message-based and interleaved frame communication.
/// </summary>
public sealed class RtspConnection : IDisposable, IAsyncDisposable
{
    private readonly IRtspTransport _transport;
    private readonly IRtspMessageParser _parser;
    private readonly IRtspMessageSerializer _serializer;
    private readonly RtspRequestTracker _requestTracker;
    private readonly Channel<RtspMessage> _messagesChannel;
    private readonly Channel<InterleavedFrame> _frameChannel;
    private readonly CancellationTokenSource _disposeCts;
    private readonly int _frameChannelCapacity;

    private int _droppedInterleavedFrames;

    private Task? _readTask;
    private uint _nextCSeq;
    private int _disposed;

    /// <summary>
    /// Gets an asynchronous reader that yields RTSP messages as they are parsed.
    /// </summary>
    public ChannelReader<RtspMessage> Messages => _messagesChannel.Reader;

    /// <summary>
    /// Gets an asynchronous reader that yields interleaved RTP/RTCP frames as they arrive.
    /// </summary>
    public ChannelReader<InterleavedFrame> InterleavedFrames => _frameChannel.Reader;

    /// <summary>
    /// Gets the number of interleaved frames dropped due to bounded channel capacity.
    /// </summary>
    public int DroppedInterleavedFrames => _droppedInterleavedFrames;

    /// <summary>
    /// Occurs when the connection is disconnected.
    /// </summary>
    public event EventHandler? Disconnected;

    /// <summary>
    /// Initializes a new instance of the <see cref="RtspConnection"/> class.
    /// </summary>
    /// <param name="transport">The underlying RTSP transport.</param>
    /// <param name="frameChannelCapacity">The capacity of the interleaved frame channel. 0 for unbounded.</param>
    public RtspConnection(IRtspTransport transport, int frameChannelCapacity = 0)
        : this(transport, new RtspMessageParser(), new RtspMessageSerializer(), frameChannelCapacity)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RtspConnection"/> class with custom parser and serializer.
    /// </summary>
    /// <param name="transport">The underlying RTSP transport.</param>
    /// <param name="parser">The message parser.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="frameChannelCapacity">The capacity of the interleaved frame channel. 0 for unbounded.</param>
    public RtspConnection(IRtspTransport transport, IRtspMessageParser parser, IRtspMessageSerializer serializer, int frameChannelCapacity = 0)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _parser = parser ?? throw new ArgumentNullException(nameof(parser));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _requestTracker = new RtspRequestTracker();
        _messagesChannel = Channel.CreateUnbounded<RtspMessage>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true
        });

        _frameChannelCapacity = frameChannelCapacity;
        _frameChannel = frameChannelCapacity > 0
            ? Channel.CreateBounded<InterleavedFrame>(new BoundedChannelOptions(frameChannelCapacity)
            {
                SingleReader = false,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait
            })
            : Channel.CreateUnbounded<InterleavedFrame>(new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = true
            });

        _disposeCts = new CancellationTokenSource();
        _nextCSeq = 1;
    }

    /// <summary>
    /// Gets the remote address of the connection.
    /// </summary>
    public string RemoteAddress => _transport.RemoteAddress;

    /// <summary>
    /// Gets whether the connection is currently connected.
    /// </summary>
    public bool Connected => _transport.Connected;

    /// <summary>
    /// Starts the read loop to process incoming messages and data frames.
    /// </summary>
    public void Start()
    {
        ObjectDisposedException.ThrowIf(_disposed == 1, this);

        if (_readTask != null)
        {
            throw new InvalidOperationException("Connection already started");
        }

        _readTask = Task.Run(async () => await ReadLoopAsync(_disposeCts.Token), _disposeCts.Token);
    }

    /// <summary>
    /// Sends an RTSP message over the connection.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task SendMessageAsync(RtspMessage message, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed == 1, this);

        if (message is RtspRequest request)
        {
            if (!request.Headers.ContainsKey("CSeq"))
            {
                request.Headers["CSeq"] = _nextCSeq.ToString();
                request.CSeq = (int)_nextCSeq;
                _nextCSeq++;
            }
            else
            {
                // Ensure CSeq property matches header
                if (int.TryParse(request.Headers["CSeq"], out int cseq))
                {
                    request.CSeq = cseq;
                }
            }

            // Track the request for response correlation
            _requestTracker.TrackRequest(request);
        }

        Stream stream = _transport.GetStream();

        if (message is RtspRequest req)
        {
            await _serializer.SerializeAsync(req, stream, cancellationToken);
        }
        else if (message is RtspResponse resp)
        {
            await _serializer.SerializeAsync(resp, stream, cancellationToken);
        }
    }

    /// <summary>
    /// Sends an RTSP response message.
    /// </summary>
    /// <param name="response">The response to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task SendResponseAsync(RtspResponse response, CancellationToken cancellationToken = default)
    {
        await SendMessageAsync(response, cancellationToken);
    }

    /// <summary>
    /// Sends interleaved data on the specified channel.
    /// </summary>
    /// <param name="channel">The channel number.</param>
    /// <param name="data">The data to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task SendInterleavedDataAsync(byte channel, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed == 1, this);

        Stream stream = _transport.GetStream();

        // Write interleaved frame header: $ + channel + length (2 bytes big-endian)
        byte[] header = [(byte)'$', channel, (byte)(data.Length >> 8), (byte)(data.Length & 0xFF)];
        await stream.WriteAsync(header, cancellationToken);
        await stream.WriteAsync(data, cancellationToken);
        await stream.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Gets the next CSeq value for request tracking.
    /// </summary>
    /// <returns>The next CSeq value.</returns>
    public uint GetNextCSeq()
    {
        return _nextCSeq++;
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            Stream stream = _transport.GetStream();

            while (!cancellationToken.IsCancellationRequested && _transport.Connected)
            {
                object? parsed = await _parser.ParseAsync(stream, cancellationToken);

                if (parsed == null)
                {
                    // ParseAsync returning null implies stream read returned 0 (EOF)
                    // or protocol error where we can't proceed.
                    break;
                }

                if (parsed is RtspMessage message)
                {
                    // Correlate responses with original requests.
                    if (message is RtspResponse response)
                    {
                        _requestTracker.CorrelateResponse(response);
                    }

                    try
                    {
                        await _messagesChannel.Writer.WriteAsync(message, cancellationToken);
                    }
                    catch (ChannelClosedException)
                    {
                        break;
                    }
                }
                else if (parsed is InterleavedFrame frame)
                {
                    try
                    {
                        if (!_frameChannel.Writer.TryWrite(frame))
                        {
                            // Channel is full - drop the oldest frame if bounded, otherwise drop the new frame.
                            if (_frameChannelCapacity > 0 && _frameChannel.Reader.TryRead(out InterleavedFrame? dropped))
                            {
                                DropFrame(dropped);

                                // Try writing the new frame again.
                                if (!_frameChannel.Writer.TryWrite(frame))
                                {
                                    DropFrame(frame);
                                }
                            }
                            else
                            {
                                DropFrame(frame);
                            }
                        }
                    }
                    catch (ChannelClosedException)
                    {
                        frame.Dispose();
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
        catch (Exception)
        {
            // Connection error - disconnection is signaled below.
        }
        finally
        {
            _messagesChannel.Writer.TryComplete();
            _frameChannel.Writer.TryComplete();

            // Signal disconnection if we exit normally too.
            if (!cancellationToken.IsCancellationRequested)
            {
                Disconnected?.Invoke(this, EventArgs.Empty);
            }
        }
    }

    private void DropFrame(InterleavedFrame frame)
    {
        frame.Dispose();
        Interlocked.Increment(ref _droppedInterleavedFrames);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        // Synchronous cleanup of managed resources.
        _disposeCts.Cancel();
        _transport.Dispose();
        CleanupFinalResources();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        // Asynchronous cleanup of managed resources.
        _disposeCts.Cancel();

        if (_readTask != null)
        {
            try
            {
                await _readTask.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Ignore timeouts or cancellation
            }
        }

        if (_transport is IAsyncDisposable asyncTransport)
        {
            await asyncTransport.DisposeAsync().ConfigureAwait(false);
        }
        else
        {
            _transport.Dispose();
        }

        CleanupFinalResources();
    }

    /// <summary>
    /// Disposes resources that don't need async/sync differentiation.
    /// </summary>
    private void CleanupFinalResources()
    {
        _messagesChannel.Writer.TryComplete();
        _frameChannel.Writer.TryComplete();
        _disposeCts.Dispose();
    }
}
