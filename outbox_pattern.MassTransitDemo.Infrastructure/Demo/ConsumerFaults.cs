using System.Collections.Concurrent;

namespace outbox_pattern.MassTransitDemo.Infrastructure.Demo
{
    /// <summary>
    /// Công tắc phục vụ demo: bắt consumer ném lỗi N lần tiếp theo để quan sát retry và hàng _error.
    /// Chỉ được bật qua PUT /demo/consumer/fail (endpoint chỉ chạy ở môi trường Development).
    /// </summary>
    public class ConsumerFaults
    {
        private readonly ConcurrentQueue<(Guid? MessageId, DateTime AtUtc)> _failures = new();
        private int _remaining;

        public int Remaining => Volatile.Read(ref _remaining);

        /// <summary>Các lần consumer đã bị bắt ném lỗi (message nào, lúc nào) kể từ khi khởi động.</summary>
        public IReadOnlyCollection<(Guid? MessageId, DateTime AtUtc)> Failures => _failures.ToArray();

        public void Arm(int times) => Volatile.Write(ref _remaining, Math.Max(0, times));

        /// <summary>Ném lỗi nếu còn lượt lỗi được bật; mỗi lần gọi tiêu một lượt.</summary>
        public void ThrowIfArmed(Guid? messageId = null)
        {
            while (true)
            {
                var current = Volatile.Read(ref _remaining);

                if (current <= 0)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref _remaining, current - 1, current) == current)
                {
                    _failures.Enqueue((messageId, DateTime.UtcNow));

                    throw new InvalidOperationException("Consumer failure (simulated by PUT /demo/consumer/fail)");
                }
            }
        }
    }
}
