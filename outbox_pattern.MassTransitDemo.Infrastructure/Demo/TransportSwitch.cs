namespace outbox_pattern.MassTransitDemo.Infrastructure.Demo
{
    /// <summary>
    /// Công tắc phục vụ demo: giả lập "broker sập" hoặc "broker treo" cho transport in-memory (vốn không bao giờ lỗi).
    ///   Down - mọi lần gửi ra transport đều ném lỗi, như RabbitMQ/Service Bus không với tới được.
    ///   Hang - mọi lần gửi ra transport đều CHỜ vô hạn cho tới khi chuyển về Up, như broker không trả lời.
    /// Chỉ điều khiển qua PUT /demo/transport/up|down|hang (endpoint chỉ chạy ở môi trường Development).
    /// </summary>
    public class TransportSwitch
    {
        private volatile bool _down;
        private volatile TaskCompletionSource? _hang;

        public bool IsDown => _down;

        public bool IsHanging => _hang is not null;

        public string Mode => _down ? "down" : _hang is not null ? "hang" : "up";

        public void SetDown(bool down)
        {
            _down = down;

            if (down)
            {
                Release();
            }
        }

        public void Hang()
        {
            _down = false;
            _hang ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        /// <summary>Cho các lần gửi đang bị treo đi tiếp và thôi treo các lần sau.</summary>
        public void Release() => Interlocked.Exchange(ref _hang, null)?.TrySetResult();

        public void SetUp()
        {
            _down = false;
            Release();
        }

        /// <summary>
        /// Chạy trước mỗi lần gửi ra transport: ném lỗi nếu Down, chờ nếu Hang. Chờ có tôn trọng <paramref name="cancellationToken"/>
        /// (như client của broker thật tôn trọng timeout) nên MessageDeliveryTimeout của MassTransit cắt được lần gửi đang treo.
        /// </summary>
        public async Task BeforeSendAsync(CancellationToken cancellationToken)
        {
            if (_down)
            {
                throw new InvalidOperationException("Transport is down (simulated by PUT /demo/transport/down)");
            }

            var hang = _hang;

            if (hang is not null)
            {
                await hang.Task.WaitAsync(cancellationToken);
            }
        }
    }
}
