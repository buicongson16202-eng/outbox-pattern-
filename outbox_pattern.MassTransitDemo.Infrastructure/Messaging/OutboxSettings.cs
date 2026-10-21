namespace outbox_pattern.MassTransitDemo.Infrastructure.Messaging
{
    /// <summary>Cấu hình lấy từ appsettings.json, mục "Outbox".</summary>
    public class OutboxSettings
    {
        public const string SectionName = "Outbox";

        /// <summary>
        /// Chu kỳ dịch vụ giao outbox của MassTransit quét bảng khi nghỉ (tương đương PollingInterval của relay tự viết).
        /// Đây chỉ là lưới an toàn: ngay sau khi một request commit xong, MassTransit đánh thức dịch vụ giao outbox CỦA CHÍNH
        /// PROCESS ĐÓ nên message đi gần như tức thì (đã đo: ~60 ms kể cả khi QueryDelay = 5 giây). Nhờ vậy QueryDelay chỉ quyết
        /// định độ trễ với các dòng do process khác ghi hoặc khi relay vừa hồi phục sau sự cố. Muốn nhìn thấy dòng OutboxMessage
        /// còn "chờ gửi" khi demo, hãy dùng PUT /demo/transport/down (hoặc hang) chứ không phải tăng giá trị này.
        /// </summary>
        public TimeSpan QueryDelay { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>Số message tối đa dịch vụ giao outbox gửi trong một lần (mặc định của MassTransit: 100).</summary>
        public int MessageDeliveryLimit { get; set; } = 100;

        /// <summary>
        /// Thời gian tối đa cho MỘT lần gửi ra transport (mặc định của MassTransit: 10 giây). Quan trọng vì dịch vụ giao outbox
        /// đang giữ khoá dòng OutboxState trong DB suốt lúc chờ transport: transport treo vô hạn thì khoá bị giữ vô hạn.
        /// </summary>
        public TimeSpan MessageDeliveryTimeout { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Thời gian MassTransit nhớ một message đã nhận trong InboxState để khử trùng lặp rồi mới dọn.
        /// Mặc định của MassTransit chỉ 30 phút, quá ngắn nếu message có thể bị giao lại muộn; project này đặt 14 ngày
        /// (cùng cỡ InboxRetention của project outbox_pattern). Phải LỚN HƠN khoảng giao lại dài nhất có thể xảy ra.
        /// </summary>
        public TimeSpan DuplicateDetectionWindow { get; set; } = TimeSpan.FromDays(14);

        /// <summary>
        /// Khoảng nghỉ giữa các lần thử lại khi consumer ném lỗi (retry tại chỗ, trước khi message bị đẩy vào hàng _error);
        /// n khoảng nghỉ nghĩa là consumer được gọi tối đa n + 1 lần. Để trống thì không retry.
        /// Cố ý KHÔNG có giá trị mặc định trong code (xem appsettings.json): bộ nạp cấu hình của .NET NỐI các phần tử đọc được
        /// vào mảng đã có sẵn thay vì thay thế, nên một mặc định trong code sẽ cộng dồn với appsettings và làm số lần retry sai.
        /// </summary>
        public TimeSpan[] ConsumerRetryIntervals { get; set; } = [];
    }
}
