# Outbox Pattern bằng MassTransit — giải thích và cách áp dụng trong project này

Tài liệu đi kèm bộ project `outbox_pattern.MassTransitDemo.*` — tổ chức theo **Clean Architecture** thành bốn project Api, Application, Domain, Infrastructure (xem [mục 2.3](#23-cấu-trúc-solution-clean-architecture)). Nó làm **đúng bài toán** của project `outbox_pattern` (POST /orders → event `OrderCreated` → Inventory trừ kho), nhưng thay toàn bộ phần tự viết (bảng outbox, relay, inbox, dọn dẹp) bằng outbox/inbox có sẵn của **MassTransit**.

Lý thuyết nền (dual-write, ACID, 2PC, at-least-once, vì sao cần inbox) **không nhắc lại** ở đây. Xem [OUTBOX_PATTERN.md](../../outbox_pattern/docs/OUTBOX_PATTERN.md) mục 1–5. Tài liệu này tập trung vào: MassTransit làm những bước đó thế nào, khác bản tự viết ở đâu, và các bẫy khi dùng.

Quy ước giống tài liệu kia: chỗ ghi **"đã kiểm chứng"** là kết quả chạy thật trong project này (tổng hợp ở [mục 9](#9-kết-quả-chạy-thử-trong-project-này), mỗi dòng có test tự động tương ứng). Ký hiệu: ✅ *đã kiểm chứng*; ◐ *cơ chế có trong code nhưng chưa thử trong điều kiện thật*; 📖 *lý thuyết/tài liệu, chưa tái hiện*.

## Mục lục

0. [Tóm tắt 30 giây](#0-tóm-tắt-30-giây)
1. [MassTransit là gì, phiên bản và giấy phép](#1-masstransit-là-gì-phiên-bản-và-giấy-phép)
2. [Các thành phần và ba bảng của MassTransit](#2-các-thành-phần-và-ba-bảng-của-masstransit)
3. [So sánh với bản tự viết (project outbox_pattern)](#3-so-sánh-với-bản-tự-viết-project-outbox_pattern)
4. [HOW — hoạt động từng bước và cách cài trong project](#4-how--hoạt-động-từng-bước-và-cách-cài-trong-project)
5. [Bảo đảm giao hàng và inbox của MassTransit](#5-bảo-đảm-giao-hàng-và-inbox-của-masstransit)
6. [Ưu điểm và nhược điểm khi dùng MassTransit](#6-ưu-điểm-và-nhược-điểm-khi-dùng-masstransit)
7. [Scale-out: nhiều instance](#7-scale-out-nhiều-instance)
8. [Thay transport in-memory bằng broker thật](#8-thay-transport-in-memory-bằng-broker-thật)
9. [Kết quả chạy thử trong project này](#9-kết-quả-chạy-thử-trong-project-này)
10. [Chạy thử](#10-chạy-thử)
11. [Giới hạn của bản demo và checklist production](#11-giới-hạn-của-bản-demo-và-checklist-production)
12. [Đọc thêm](#12-đọc-thêm)

---

## 0. Tóm tắt 30 giây

- **Cùng một ý tưởng:** ghi dữ liệu nghiệp vụ *và* message vào **cùng một transaction** của DB; một tiến trình nền gửi message sang broker sau khi commit; consumer dùng **inbox** để xử lý mỗi message đúng một lần.
- **Khác ở chỗ ai viết code:** bạn chỉ (1) thêm ba bảng của MassTransit vào `DbContext`, (2) bật `AddEntityFrameworkOutbox(... UseBusOutbox())`, (3) bật `UseEntityFrameworkOutbox` cho consumer. Use case gọi cổng `IOrderEventPublisher` (cài bằng `IPublishEndpoint.Publish(...)`) rồi `IUnitOfWork.SaveChangesAsync()` như bình thường. Relay, inbox, dọn dẹp, retry, hàng lỗi đều do MassTransit làm.
- **Cái giá:** thêm một thư viện lớn và các quy ước của nó; ít quan sát hơn bản tự viết (không có cột `Attempts`/`LastError`); và **từ v9 MassTransit là sản phẩm thương mại** — project này dùng v8 (Apache-2.0).

```
POST /orders ──► [ INSERT Orders + INSERT OutboxState + INSERT OutboxMessage : 1 transaction ]
                         │ commit xong → MassTransit "đánh thức" dịch vụ giao outbox ngay
                         ▼
          BusOutboxDeliveryService: khoá OutboxState → gửi từng OutboxMessage → XOÁ chúng
                         ▼
                 transport (in-memory / RabbitMQ / Service Bus…)
                         ▼
   Consumer: [ InboxState (MessageId, ConsumerId) + UPDATE StockItems : 1 transaction ]
```

---

## 1. MassTransit là gì, phiên bản và giấy phép

### 1.1 MassTransit là gì

MassTransit là thư viện messaging cho .NET: một lớp trừu tượng phía trên các broker (RabbitMQ, Azure Service Bus, Amazon SQS, Kafka, in-memory…). Bạn viết **message** (class/record), **consumer** (`IConsumer<T>`) và cấu hình; MassTransit lo topology (exchange/queue), serialize, retry, hàng lỗi, và — quan trọng với tài liệu này — **transactional outbox + inbox** lưu bằng EF Core.

### 1.2 Phiên bản dùng trong project

| Package | Phiên bản | Giấy phép |
|---|---|---|
| `MassTransit` | 8.5.11 (có bản build cho `net10.0`) | Apache-2.0 |
| `MassTransit.EntityFrameworkCore` | 8.5.11 (phụ thuộc EF Core 10 trên `net10.0`) | Apache-2.0 |

Thông tin giấy phép và phụ thuộc lấy từ file `.nuspec` của chính hai package trên nuget.org (kiểm tra ngày 2026-10-20).

### 1.3 ⚠️ Giấy phép: v8 mở, v9 thương mại

- Từ **v9**, MassTransit do công ty Massient phát hành dưới **giấy phép thương mại**: trang NuGet của các bản 9.x ghi rằng MassTransit là sản phẩm thương mại phải mua license, và tài liệu v9 có mục cấu hình license key.
- **v8** vẫn giữ giấy phép mã nguồn mở (Apache-2.0) và vẫn dùng được; trang hỗ trợ của Massient nói vẫn hỗ trợ thương mại cho v8 "trong thời gian tới" nhưng khuyến khích chuyển sang v9. Tôi **không tìm thấy ngày kết thúc hỗ trợ chính thức** của v8, cũng không xác nhận được bảng giá/điều kiện miễn phí của v9.
- Hệ quả thực tế: đừng nâng `MassTransit` lên 9.x chỉ vì NuGet báo có bản mới. Đó là quyết định mua phần mềm, cần người có thẩm quyền của dự án quyết. File `.csproj` của project có ghi chú nhắc việc này.

API dùng trong project (`AddEntityFrameworkOutbox`, `UseBusOutbox`, `UseEntityFrameworkOutbox`, `AddInboxStateEntity`…) có ở cả v8 và v9; một số tuỳ chọn mới chỉ có ở v9 (vd. `ConfigureDeliveryRetry` cho bus outbox, 9.2.1+).

---

## 2. Các thành phần và ba bảng của MassTransit

### 2.1 Thành phần

| Thành phần | Vai trò | Trong project |
|---|---|---|
| Ba bảng `OutboxMessage`, `OutboxState`, `InboxState` | Lưu message chờ gửi và sổ "đã nhận" | `modelBuilder.AddInboxStateEntity()` … trong [AppDbContext.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/Persistence/AppDbContext.cs) |
| **Bus outbox** (producer) | `IPublishEndpoint.Publish` ghi message vào `DbContext` thay vì gửi thẳng | `UseBusOutbox()` trong [DependencyInjection.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/DependencyInjection.cs); dùng ở [MassTransitOrderEventPublisher.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/Messaging/MassTransitOrderEventPublisher.cs) |
| **BusOutboxDeliveryService** (relay) | Hosted service đọc outbox, gửi ra transport, xoá dòng đã gửi | MassTransit tự đăng ký khi gọi `AddEntityFrameworkOutbox` |
| **Consumer outbox / inbox** (middleware) | Bọc consumer trong transaction + ghi `InboxState` để khử trùng lặp | `endpoint.UseEntityFrameworkOutbox<AppDbContext>(context)` trong DependencyInjection.cs |
| **InboxCleanupService** | Xoá `InboxState` cũ hơn `DuplicateDetectionWindow` | MassTransit tự đăng ký |
| Retry + hàng `_error` | Consumer lỗi thì thử lại; hết lượt thì message sang hàng `<tên>_error` | `endpoint.UseMessageRetry(...)` trong DependencyInjection.cs |
| Consumer | Adapter nhận message rồi gọi use case trừ kho, **không** chứa code inbox/transaction | [OrderCreatedConsumer.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/Messaging/OrderCreatedConsumer.cs) → [ReserveStock.cs](../../outbox_pattern.MassTransitDemo.Application/Inventory/ReserveStock.cs) |
| Transport | Broker. Demo dùng in-memory | `x.UsingInMemory(...)` |

Hai công tắc chỉ phục vụ demo: [TransportSwitch.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/Demo/TransportSwitch.cs) (giả lập transport sập/treo) và [ConsumerFaults.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/Demo/ConsumerFaults.cs) (bắt consumer ném lỗi). Cả hai được điều khiển qua `/demo/*`.

### 2.2 Ba bảng (cột quan sát được trên SQL Server)

| Bảng | Cột chính | Ý nghĩa |
|---|---|---|
| `OutboxMessage` | `SequenceNumber` (bigint identity, khoá), `OutboxId`, `MessageId`, `MessageType`, `Body`, `Headers`, `DestinationAddress`, `SentTime`, `EnqueueTime`… | Một dòng = một message chờ gửi. `MessageType` vd. `urn:message:orders.order-created.v1`. `Body` là JSON "envelope" của MassTransit |
| `OutboxState` | `OutboxId` (khoá), `LockId`, `RowVersion`, `Created`, `Delivered`, `LastSequenceNumber` | Một dòng = **một lần SaveChanges** có publish. Đây là đơn vị mà relay khoá và giao. `Delivered = NULL` nghĩa là chưa giao xong |
| `InboxState` | `Id` (identity), `MessageId` + `ConsumerId` (duy nhất), `LockId`, `RowVersion`, `Received`, `ReceiveCount`, `Consumed`, `Delivered`, `ExpirationTime`… | Sổ "consumer này đã nhận message này". `Consumed`/`Delivered` có giá trị = đã xử lý xong |

So với bảng `OutboxMessages` tự viết: **không có** `Attempts`, `LastError`, `NextAttemptOnUtc`, và **dòng bị xoá ngay sau khi giao** (✅ đã kiểm chứng) chứ không giữ lại 7 ngày.

### 2.3 Cấu trúc solution: Clean Architecture

```
outbox_pattern.MassTransitDemo.Api            Controller HTTP mỏng + Program.cs (composition root)
        │  tham chiếu
        ├──────────────► outbox_pattern.MassTransitDemo.Application      Use case + các "cổng" (interface)
        │                         │  tham chiếu
        │                         ▼
        │                outbox_pattern.MassTransitDemo.Domain           Entity + quy tắc nghiệp vụ, không phụ thuộc gì
        │                         ▲
        └──────────────► outbox_pattern.MassTransitDemo.Infrastructure  EF Core + MassTransit: cài đặt các cổng
                                  (tham chiếu Application, qua đó tới Domain)
```

**Quy tắc phụ thuộc:** mũi tên chỉ hướng **vào trong**. Domain không biết gì về tầng khác. Application chỉ biết Domain, **không** biết EF Core hay MassTransit. Infrastructure cài đặt các interface mà Application khai báo. Api chỉ nối mọi thứ lại và dịch HTTP ↔ use case. Quy tắc này được kiểm tra tự động bằng [ArchitectureTests.cs](../../outbox_pattern.MassTransitDemo.Tests/Unit/ArchitectureTests.cs) (đọc danh sách assembly mà mỗi tầng thực sự dùng).

| Project | Chứa gì | Phụ thuộc |
|---|---|---|
| **Domain** | [Order.cs](../../outbox_pattern.MassTransitDemo.Domain/Orders/Order.cs) (chỉ tạo được qua `Order.Create`, tự kiểm tra bất biến), [StockItem.cs](../../outbox_pattern.MassTransitDemo.Domain/Inventory/StockItem.cs), `DomainException` | Không gì cả |
| **Application** | Cổng: `IOrderRepository`, `IStockRepository`, `IOrderEventPublisher`, `IUnitOfWork` ([Ports.cs](../../outbox_pattern.MassTransitDemo.Application/Abstractions/Ports.cs)). Use case: [CreateOrderHandler](../../outbox_pattern.MassTransitDemo.Application/Orders/CreateOrder.cs), `GetOrderHandler`, [ReserveStockHandler](../../outbox_pattern.MassTransitDemo.Application/Inventory/ReserveStock.cs) | Domain; `Microsoft.Extensions.DependencyInjection.Abstractions` (chỉ để đăng ký DI) |
| **Infrastructure** | `AppDbContext` (đồng thời là `IUnitOfWork`), repository, hợp đồng message `OrderCreated`, `MassTransitOrderEventPublisher`, `OrderCreatedConsumer`, cấu hình MassTransit ([DependencyInjection.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/DependencyInjection.cs)), công cụ demo | Application; EF Core SQL Server, MassTransit |
| **Api** | [Program.cs](../Program.cs), [OrdersController.cs](../Controllers/OrdersController.cs), [DemoController.cs](../Controllers/DemoController.cs), appsettings | Application, Infrastructure |

**Một request đi qua các tầng thế nào:**

```
POST /orders ─► OrdersController (Api)
                  └─► CreateOrderHandler.HandleAsync (Application)
                        ├─ Order.Create(...)                         (Domain: kiểm tra bất biến)
                        ├─ IOrderRepository.Add          ─► OrderRepository        (Infrastructure, EF)
                        ├─ IOrderEventPublisher.Publish  ─► MassTransitOrderEventPublisher → IPublishEndpoint
                        │                                    → thêm OutboxMessage vào CHÍNH AppDbContext
                        └─ IUnitOfWork.SaveChangesAsync  ─► AppDbContext.SaveChanges: 1 transaction cho cả hai

MassTransit ─► OrderCreatedConsumer (Infrastructure; middleware inbox bọc quanh)
                  └─► ReserveStockHandler.HandleAsync (Application)
                        └─ IStockRepository.TryReserveAsync ─► StockRepository: UPDATE ... WHERE Available >= @q
```

Các quyết định thiết kế khi tách tầng:

1. **Outbox vẫn là MỘT transaction dù đi qua ba cổng.** `IOrderRepository`, `IOrderEventPublisher` và `IUnitOfWork` đều được cài trên **cùng một** `AppDbContext` của scope (`AppDbContext` chính là `IUnitOfWork`). Nếu chúng dùng DbContext khác nhau thì Order và message rơi vào hai transaction và pattern mất tác dụng (lỗi số 1 ở OUTBOX_PATTERN.md mục 4.6). Unit test [UseCaseTests.cs](../../outbox_pattern.MassTransitDemo.Tests/Unit/UseCaseTests.cs) khoá lại thứ tự `add → publish → commit (một lần)`.
2. **Hợp đồng message `OrderCreated` nằm ở Infrastructure, không ở Application.** Đó là định dạng truyền tải (giống schema bảng), và nó mang `[MessageUrn]` của MassTransit. Application chỉ có cổng `PublishOrderCreatedAsync(Order)`. Khi tách thành nhiều service thật, record này thường chuyển sang một package "Contracts" dùng chung. Nhờ `[MessageUrn("orders.order-created.v1")]`, việc chuyển class sang namespace khác lúc tách tầng **không** đổi định danh message (vẫn là `urn:message:orders.order-created.v1`, ✅ test 1 ở mục 9).
3. **Quy tắc "không trừ quá tồn" là một câu UPDATE có điều kiện trong `StockRepository`**, không phải method đọc-sửa-ghi trên entity `StockItem`. Nhiều consumer trừ cùng SKU đồng thời; đọc về rồi ghi lại sẽ mất cập nhật (case C3). Ghi chú ngay trong `StockItem.cs`.
4. **Consumer là adapter mỏng.** `OrderCreatedConsumer` (Infrastructure) dịch message thành `ReserveStockCommand` rồi gọi use case; inbox/transaction do middleware MassTransit lo. Use case không biết mình được gọi từ broker.
5. **Không dùng MediatR.** Handler là class thường, đăng ký DI trực tiếp, controller inject handler. Ít phụ thuộc hơn (MediatR hiện có bản thương mại tính phí theo đội; xem kỹ giấy phép nếu muốn dùng).
6. **`TimeProvider` thay cho `DateTime.UtcNow`** trong use case, để unit test cố định được thời gian.
7. **Ngoại lệ có chủ đích:** `DemoController` gọi thẳng công cụ ở Infrastructure (`DemoTools`, hai công tắc) để đọc bảng của MassTransit và cố ý tạo bản trùng — những việc code nghiệp vụ không được làm. Chỉ chạy ở Development.

---

## 3. So sánh với bản tự viết (project outbox_pattern)

| Khía cạnh | Tự viết (`outbox_pattern`) | MassTransit (`outbox_pattern.MassTransitDemo`) |
|---|---|---|
| Code phải tự viết | Entity outbox/inbox, `OutboxProcessor`, `InventoryConsumer` (inbox + transaction), `MessageCleanupService`, `IMessagePublisher` | Chỉ cấu hình + consumer nghiệp vụ |
| Ghi message | `db.OutboxMessages.Add(OutboxMessage.Create(...))` | `publishEndpoint.Publish(new OrderCreated(...))` — cùng một `SaveChanges` |
| Đơn vị relay claim | Từng message (lô 20 dòng) | Một `OutboxState` = mọi message của **một** transaction, gửi theo `SequenceNumber` |
| Cách claim | Lease: `UPDATE ... SET NextAttemptOnUtc = now + 60s` có điều kiện, **không giữ khoá** khi gọi broker | `SELECT TOP 1 * FROM OutboxState WITH (UPDLOCK, ROWLOCK, READPAST)`, **giữ khoá dòng suốt lúc gửi** (✅ đã kiểm chứng) |
| Khi nào relay chạy | Quét mỗi `PollingInterval` (1 s) | **Được đánh thức ngay sau commit** trong cùng process (✅ ~60 ms kể cả khi `QueryDelay` = 5 s), quét mỗi `QueryDelay` làm lưới an toàn |
| Broker lỗi | `Attempts++`, `LastError`, backoff theo dòng 2 s, 4 s, 8 s… | Log `Outbox Send Fault`, thử lại ở lần quét sau (mỗi `QueryDelay`); **không lưu** số lần thử/lỗi trong DB |
| Broker treo | `PublishTimeout` (10 s) | `MessageDeliveryTimeout` (mặc định 10 s), log `Outbox Send Timeout after …` (✅) |
| Dòng đã gửi | Giữ lại, `ProcessedOnUtc` = thời điểm; dọn sau 7 ngày | Xoá ngay |
| Inbox | Bảng `InboxMessages`, khoá chính `(MessageId, Consumer)`, code tự viết | Bảng `InboxState`, duy nhất `(MessageId, ConsumerId)`, middleware |
| Consumer lỗi | Tự `RequeueAsync` sau 2 s, vô hạn | `UseMessageRetry` (cấu hình được) rồi sang hàng `_error` |
| Dọn inbox | Sau 14 ngày (`InboxRetention`) | Sau `DuplicateDetectionWindow` — **mặc định chỉ 30 phút** (project đặt 14 ngày) |
| Tên message | Hằng `"orders.order-created.v1"` | `[MessageUrn("orders.order-created.v1")]` → `urn:message:orders.order-created.v1` |
| Đổi broker | Viết `IMessagePublisher` + consumer cho broker đó | Đổi `UsingInMemory` thành `UsingRabbitMq`/`UsingAzureServiceBus`… |
| Giấy phép | Code của bạn | v8 Apache-2.0; v9 thương mại |

Hai cách claim là hai lựa chọn hợp lệ đã bàn ở OUTBOX_PATTERN.md mục 8.5. MassTransit chọn "khoá trong transaction" (giống bản tự viết **trước** khi chuyển sang lease): instance chết thì DB nhả khoá ngay, nhưng broker treo thì khoá bị giữ tới khi hết `MessageDeliveryTimeout`.

---

## 4. HOW — hoạt động từng bước và cách cài trong project

### 4.1 Cấu hình (Infrastructure/DependencyInjection.cs)

`Program.cs` của Api chỉ gọi `AddApplication()` và `AddInfrastructure(configuration)`. Phần MassTransit nằm trong `AddInfrastructure`:

```csharp
services.AddMassTransit(x =>
{
    x.AddEntityFrameworkOutbox<AppDbContext>(outbox =>
    {
        outbox.UseSqlServer();                      // cách khoá hàng của SQL Server
        outbox.UseBusOutbox(bus =>                  // IPublishEndpoint → ghi vào OutboxMessage
        {
            bus.MessageDeliveryLimit = 100;         // số message mỗi lần giao (≈ BatchSize)
            bus.MessageDeliveryTimeout = TimeSpan.FromSeconds(10); // ≈ PublishTimeout
        });
        outbox.QueryDelay = TimeSpan.FromSeconds(1);               // mặc định của MassTransit: 10 s
        outbox.DuplicateDetectionWindow = TimeSpan.FromDays(14);   // mặc định của MassTransit: 30 phút
    });

    x.AddConsumer<OrderCreatedConsumer>();

    x.AddConfigureEndpointsCallback((context, name, endpoint) =>
    {
        endpoint.UseMessageRetry(r => r.Intervals(100, 500, 1000)); // TRƯỚC outbox
        endpoint.UseEntityFrameworkOutbox<AppDbContext>(context);   // inbox + transaction quanh consumer
    });

    x.UsingInMemory((context, cfg) => cfg.ConfigureEndpoints(context));
});
```

Giá trị thật được đọc từ mục `"Outbox"` của [appsettings.json](../appsettings.json) qua [OutboxSettings.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/Messaging/OutboxSettings.cs). Các giá trị **mặc định của MassTransit 8.5.11** dưới đây được đọc trực tiếp từ thư viện (✅): `QueryDelay` 10 s, `QueryMessageLimit` 100, `QueryTimeout` 30 s, `DuplicateDetectionWindow` 30 phút, `MessageDeliveryLimit` 100, `MessageDeliveryTimeout` 10 s.

Và trong `DbContext`:

```csharp
modelBuilder.AddInboxStateEntity();
modelBuilder.AddOutboxMessageEntity();
modelBuilder.AddOutboxStateEntity();
```

### 4.2 Bước 1 — Ghi (producer)

Use case [CreateOrderHandler](../../outbox_pattern.MassTransitDemo.Application/Orders/CreateOrder.cs) (Application) chỉ nói chuyện với các cổng:

```csharp
var order = Order.Create(command.Sku, command.Quantity, command.CustomerEmail, clock.GetUtcNow().UtcDateTime);

orders.Add(order);                                       // IOrderRepository
await events.PublishOrderCreatedAsync(order, ct);        // IOrderEventPublisher
await unitOfWork.SaveChangesAsync(ct);                   // IUnitOfWork: một transaction duy nhất
```

Cổng `IOrderEventPublisher` được cài ở Infrastructure bằng bus outbox ([MassTransitOrderEventPublisher.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/Messaging/MassTransitOrderEventPublisher.cs)):

```csharp
publishEndpoint.Publish(new OrderCreated(order.Id, order.Sku, ...), ct); // KHÔNG gửi lên broker: thêm OutboxMessage vào AppDbContext
```

**Đã kiểm chứng** (log SQL thật khi `SaveChanges`): EF gửi **một batch** gồm `INSERT INTO [Orders]`, rồi `INSERT INTO [OutboxState]`, rồi `INSERT INTO [OutboxMessage]`, trong cùng transaction. Khi câu INSERT cuối bị lỗi, Order cũng bị rollback, không còn dòng nào ở cả ba bảng.

Ba điều cần nhớ:
- **Thứ tự `Publish` rồi `SaveChanges`.** Gọi `Publish` sau `SaveChanges` thì message nằm trong `DbContext` mà không được lưu.
- **`IPublishEndpoint` phải lấy từ DI trong scope của request** (ở đây: inject vào `MassTransitOrderEventPublisher`, đăng ký scoped). `IBus.Publish` gửi thẳng lên broker, bỏ qua outbox, tức là quay lại dual-write.
- `Publish` vẫn chạy qua **pipeline publish** của bus để dựng message, kể cả khi đích cuối là bảng outbox. (✅ quan sát khi làm demo: một filter chặn ở pipeline publish làm hỏng luôn `POST /orders`; vì vậy công tắc "transport sập" của demo chỉ chặn ở pipeline **send**.)

### 4.3 Bước 2 — Relay: `BusOutboxDeliveryService`

Không có code relay nào trong project. Câu lệnh SQL mà dịch vụ này chạy (✅ log SQL thật, rút gọn):

```
① SELECT TOP 1 * FROM OutboxState WITH (UPDLOCK, ROWLOCK, READPAST) ORDER BY Created
       → khoá MỘT "lô" (mọi message của một transaction); lô đang bị instance khác khoá thì BỎ QUA
② UPDATE OutboxState SET LockId, LastSequenceNumber, ... WHERE OutboxId = @id AND RowVersion = @rv
③ SELECT TOP(@limit) ... FROM OutboxMessage
       WHERE OutboxId = @id AND SequenceNumber > @lastSequenceNumber ORDER BY SequenceNumber
④ với từng message: gửi ra transport (chờ xác nhận, tối đa MessageDeliveryTimeout)
⑤ DELETE FROM OutboxMessage WHERE SequenceNumber = @n ; DELETE FROM OutboxState WHERE OutboxId = @id AND RowVersion = @rv
```

Các quyết định thiết kế (so với relay tự viết):

- **Claim theo lô transaction, không theo dòng.** Message của cùng một `SaveChanges` đi theo đúng thứ tự `SequenceNumber`. Giữa các transaction khác nhau thì relay lấy theo `Created`, nhưng khi có nhiều instance thì không bảo đảm thứ tự (📖, giống P2 trong OUTBOX_PATTERN.md).
- **Giữ khoá trong lúc gửi.** ✅ Khi transport "treo", `sys.dm_tran_locks` cho thấy khoá U/X trên dòng của `OutboxState`/`OutboxMessage` bị giữ suốt thời gian chờ, và được nhả khi gửi xong. Hệ quả: broker chậm thì connection + khoá bị giữ, nên `MessageDeliveryTimeout` là bắt buộc phải hợp lý (P4).
- **Hết thời gian thì huỷ và thử lại.** ✅ Với `MessageDeliveryTimeout` = 1 s và transport treo vô hạn, MassTransit log `Outbox Send Timeout after 00:00:01: <OutboxId> <SequenceNumber> <MessageId>` khoảng mỗi 1,1 s (timeout + `QueryDelay`), dòng outbox vẫn còn, transport trả lời lại thì được giao.
- **Broker lỗi thì thử lại mãi, không backoff tăng dần.** ✅ Với transport "sập", log `Outbox Send Fault: <OutboxId> <SequenceNumber> <MessageId>` lặp lại khoảng mỗi `QueryDelay` (đo được ~1,0 s khi `QueryDelay` = 1 s; 6 lần trong 6 giây). Dòng vẫn nằm trong DB, không mất. Vì DB không lưu số lần thử, **cảnh báo phải dựa vào log/metric** hoặc tuổi của `OutboxState.Created` còn `Delivered IS NULL`.
- **Được đánh thức ngay sau commit.** ✅ Ngay cả với `QueryDelay` = 5 s, message đi tới consumer khoảng 60 ms sau khi `POST /orders` trả về. Đó là cơ chế thông báo trong cùng process. `QueryDelay` chỉ quyết định độ trễ khi dòng do process **khác** ghi, hoặc khi relay vừa hồi phục. Hệ quả cho việc học: muốn **nhìn thấy** dòng outbox đang chờ, hãy tắt transport (`PUT /demo/transport/down`), đừng tăng `QueryDelay`.
- **Xoá ngay sau khi giao.** ✅ `OutboxMessage` và `OutboxState` trống sau khi message đã đi. Muốn có lịch sử/audit thì phải tự ghi ở chỗ khác.

### 4.4 Bước 3 — Consumer + inbox

[OrderCreatedConsumer.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/Messaging/OrderCreatedConsumer.cs) (Infrastructure) chỉ dịch message thành lệnh và gọi use case [ReserveStockHandler](../../outbox_pattern.MassTransitDemo.Application/Inventory/ReserveStock.cs); use case gọi cổng `IStockRepository`, được cài ở [Repositories.cs](../../outbox_pattern.MassTransitDemo.Infrastructure/Persistence/Repositories.cs) bằng một câu UPDATE có điều kiện:

```csharp
await db.StockItems
    .Where(s => s.Sku == sku && s.Available >= quantity)
    .ExecuteUpdateAsync(s => s.SetProperty(x => x.Available, x => x.Available - quantity), ct) > 0;
```

Consumer, use case và repository dùng chung `AppDbContext` của scope message, nên câu UPDATE chạy trong transaction do middleware mở. Middleware `UseEntityFrameworkOutbox` bao quanh tất cả. SQL thật (✅ log, rút gọn):

```
SELECT * FROM InboxState WITH (UPDLOCK, ROWLOCK) WHERE MessageId = @m AND ConsumerId = @c
   → chưa có: INSERT InboxState (dòng "giữ chỗ")      → đã có và Delivered có giá trị: BỎ QUA (bản trùng)
SELECT * FROM InboxState WITH (UPDLOCK, ROWLOCK) WHERE MessageId = @m AND ConsumerId = @c
UPDATE InboxState SET Received, ReceiveCount, ...      ← đánh dấu đã nhận
UPDATE StockItems SET Available = Available - @q ...   ← consumer (cùng transaction)
UPDATE InboxState SET Consumed = now, Delivered = now  ← đánh dấu đã xử lý
COMMIT
```

- **Inbox + nghiệp vụ trong một transaction**, đúng như bản tự viết: consumer ném lỗi thì rollback cả `UPDATE StockItems` lẫn dấu `Consumed/Delivered` (✅).
- **Ràng buộc duy nhất `(MessageId, ConsumerId)` là chốt chặn cho bản trùng đến đồng thời.** ✅ Khi 5 bản của cùng một message tới gần như cùng lúc, log EF có nhiều dòng `Error Number: 2627` (vi phạm khoá duy nhất) ở mức Error. Đó là MassTransit cố ý chèn trùng rồi xử lý như bản trùng, **không phải lỗi** của ứng dụng. Ở một lần chạy còn thấy một bản trùng gặp `InvalidOperationException: The instance of entity type 'InboxState' cannot be tracked...` và được xử lý lại nhờ `UseMessageRetry`. Vì vậy **retry cũng cần cho đường xử lý bản trùng**, không chỉ cho lỗi nghiệp vụ.
- **`ConsumerId`** là một Guid do MassTransit tính cho consumer/endpoint: cùng một message được hai consumer khác nhau xử lý thì có hai dòng inbox, mỗi bên đúng một lần.
- **Dòng "giữ chỗ" còn lại khi consumer lỗi hẳn.** ✅ Sau khi retry hết lượt, `InboxState` còn một dòng có `Consumed = NULL`. Nếu message được giao lại, nó được xử lý như chưa từng thành công.
- Lỗi nghiệp vụ (hết hàng, SKU lạ) **không ném exception**: consumer chỉ log, transaction vẫn commit, nên không bị retry vô ích (giống bản tự viết).

### 4.5 Retry và hàng `_error`

```csharp
endpoint.UseMessageRetry(r => r.Intervals(100, 500, 1000)); // 3 khoảng nghỉ → tối đa 4 lần gọi consumer
endpoint.UseEntityFrameworkOutbox<AppDbContext>(context);
```

- ✅ Consumer lỗi 2 lần rồi thành công: kho trừ đúng một lần, log có `R-RETRY`.
- ✅ Consumer lỗi mãi: đúng **4** lần gọi (1 + 3), rồi log `R-FAULT` và message sang hàng `OrderCreated_error`; kho không đổi. Với transport in-memory, hàng `_error` **mất khi tắt process**; với broker thật nó nằm trên broker và cần quy trình xử lý (xem lại, sửa, đẩy lại).
- **Đặt retry TRƯỚC outbox** để mỗi lần thử là một transaction mới.
- Bẫy đã gặp khi làm demo (✅): nếu thuộc tính `TimeSpan[] ConsumerRetryIntervals` có sẵn giá trị mặc định trong code, bộ nạp cấu hình của .NET **nối** các phần tử trong appsettings vào sau thay vì thay thế, thành 6 khoảng nghỉ và **7** lần gọi consumer. Đây là hành vi của `Microsoft.Extensions.Configuration`, không phải MassTransit, nhưng rất dễ dính khi cấu hình retry. Project để mặc định rỗng và chỉ đọc từ appsettings.

### 4.6 Dọn dẹp

- **Outbox:** không cần dọn, dòng bị xoá khi giao (✅).
- **Inbox:** `InboxCleanupService` định kỳ chạy (✅ thấy trong log SQL):
  ```sql
  DELETE FROM InboxState WHERE Id IN (SELECT TOP(@p) Id FROM InboxState
                                      WHERE Delivered IS NOT NULL AND Delivered < @removeTimestamp ORDER BY Delivered)
  ```
  với `@removeTimestamp = now - DuplicateDetectionWindow`. Mặc định **30 phút** là quá ngắn nếu message có thể bị giao lại muộn hơn (broker sập lâu, hàng `_error` được đẩy lại sau vài giờ…): bản trùng đến sau khi dòng inbox đã bị xoá sẽ được xử lý như message mới. Project đặt 14 ngày.

### 4.7 Toàn cảnh và lỗi ở từng bước

Đánh số giống OUTBOX_PATTERN.md mục 4.5 để tiện đối chiếu:

```
Client   Orders API          DB                         Relay (BusOutboxDeliveryService)   Transport    Consumer
  │ POST     │ ① BEGIN          │                                │                            │            │
  │─────────►│ INSERT Orders    │                                │                            │            │
  │          │ INSERT OutboxState + OutboxMessage                │                            │            │
  │          │ COMMIT ─────────►│ ── đánh thức ngay ────────────►│                            │            │
  │◄─────────│ 201              │◄─ ② SELECT OutboxState UPDLOCK,READPAST                     │            │
  │                             │                                │── ③ send ─────────────────►│            │
  │                             │                                │◄─ ④ xác nhận ──────────────│── ⑥ ──────►│ ⑦ BEGIN
  │                             │◄─ ⑤ DELETE OutboxMessage/State │                            │            │ InboxState
  │                             │   (nhả khoá)                   │                            │            │ UPDATE stock
  │                             │                                │                            │◄─ ⑧ ack ───│ COMMIT
```

| Bước lỗi | MassTransit làm gì | Hậu quả | Trạng thái |
|---|---|---|---|
| **① trước COMMIT** | Rollback cả Order lẫn hai bảng outbox | Nhất quán, client thử lại | ✅ |
| **② relay không đọc được DB / process chết khi đang giữ khoá** | Transaction của relay rollback, khoá `OutboxState` được nhả; instance khác (hoặc chính nó lúc khởi động lại) nhặt lên | Chỉ **trễ**; message đã kịp gửi trước khi chết sẽ bị gửi lại → **trùng**, inbox chặn | ◐ (có cơ chế; chưa `kill -9` thật) |
| **③ broker từ chối / không với tới** | Log `Outbox Send Fault`, thử lại mỗi `QueryDelay` | Chỉ **trễ** | ✅ |
| **③/④ broker treo** | Huỷ sau `MessageDeliveryTimeout`, log `Outbox Send Timeout`, thử lại | **Trễ**; nếu broker thực ra đã nhận thì **trùng** | ✅ (phần treo) |
| **④ mất xác nhận** | Coi như lỗi, gửi lại cùng `MessageId` | **Trùng**, inbox chặn | ✅ (mô phỏng bằng `/demo/orders-duplicate`) |
| **⑤ DELETE outbox lỗi sau khi đã gửi** | Dòng vẫn còn, lần quét sau gửi lại | **Trùng**, inbox chặn | 📖 |
| **⑥ consumer không chạy** | Message nằm ở broker chờ | **Trễ** (in-memory: **mất** khi tắt process) | 📖 |
| **⑦ consumer lỗi** | Rollback (kể cả dấu inbox), `UseMessageRetry`, hết lượt thì sang `_error` | **Trễ**, hoặc nằm ở `_error` chờ người xử lý | ✅ |
| **⑧ mất ack của consumer** | Broker giao lại; `InboxState.Delivered` đã có nên bỏ qua | **Trùng**, inbox chặn | ✅ (tương đương bản trùng) |

Kết luận giống bản tự viết: sau ①, mọi lỗi chỉ dẫn tới **trễ** hoặc **trùng**. Khác biệt duy nhất về mặt bảo đảm là hàng `_error`: message lỗi mãi ở consumer không tự biến mất nhưng cũng không tự được thử lại. Đó là chỗ cần quy trình vận hành.

### 4.8 Những lỗi hay gặp khi dùng outbox của MassTransit

1. **Gọi `IBus.Publish`** (hoặc publish từ singleton) thay vì `IPublishEndpoint` trong scope → bỏ qua outbox, quay lại dual-write.
2. **Quên `SaveChangesAsync`** sau `Publish`, hoặc gọi `Publish` sau `SaveChanges` → message không bao giờ được lưu.
3. **Quên `UseEntityFrameworkOutbox` cho consumer** → không có inbox, bản trùng trừ kho hai lần.
4. **Đặt `UseMessageRetry` sau outbox** → các lần thử lại dùng chung một transaction đã hỏng.
5. **Để `DuplicateDetectionWindow` mặc định 30 phút** trong khi message có thể đến muộn hơn.
6. **`MessageDeliveryTimeout` quá dài** → broker treo giữ khoá và connection lâu; quá ngắn → huỷ những lần gửi chỉ chậm, sinh bản trùng.
7. **Coi log Error `2627` / `DbUpdateConcurrencyException` là sự cố** → đó là tranh chấp bình thường MassTransit tự xử lý (inbox trùng khoá, `RowVersion` của `OutboxState`). Cần lọc chúng khỏi cảnh báo.
8. **Gắn trigger lên `OutboxMessage`/`OutboxState`** → INSERT của EF/MassTransit dùng mệnh đề `OUTPUT`, SQL Server từ chối bảng có trigger (lỗi 334). Đã gặp khi viết test.
9. **Đổi namespace/tên class message mà không có `[MessageUrn]`** → message đang nằm trong outbox/broker không còn khớp consumer.
10. **Nâng lên v9 mà không biết** → đổi giấy phép (mục 1.3).

---

## 5. Bảo đảm giao hàng và inbox của MassTransit

Giống hệt bản tự viết: outbox cho **at-least-once**; inbox biến nó thành **effectively-once** cho tác dụng nằm trong cùng DB với `InboxState`. Lý do và các nguồn bản trùng: OUTBOX_PATTERN.md mục 5.

Hai điểm riêng của MassTransit:

- **`ReceiveCount` không phải bộ đếm chính xác số lần nhận.** ✅ Quan sát: một lần nhận thành công để lại `ReceiveCount = 3`; ba bản cùng `MessageId` để lại 5; hai lần lỗi rồi một lần thành công để lại 3 (các lần lỗi bị rollback). Chỉ dùng nó như tín hiệu tương đối ("có bản trùng"), đừng tính toán dựa vào nó.
- **Tác dụng ngoài DB không được inbox bảo vệ.** Gửi email, gọi HTTP trong `Consume()` vẫn có thể chạy hai lần. Muốn gửi tiếp message từ consumer thì dùng `context.Publish(...)`: với consumer outbox, các message này được ghi vào `OutboxMessage` cùng transaction (📖, project không dùng).

---

## 6. Ưu điểm và nhược điểm khi dùng MassTransit

### Ưu điểm

1. **Ít code tự viết**, ít chỗ để sai: không còn relay, inbox, cleanup, requeue tự viết.
2. **Đã được nhiều hệ thống dùng**; có sẵn retry, redelivery, hàng `_error`, topology broker, telemetry.
3. **Đổi broker chỉ đổi cấu hình transport** (mục 8).
4. **Độ trễ thấp** nhờ đánh thức ngay sau commit (✅ ~60 ms).
5. **Giữ thứ tự trong một transaction** (`SequenceNumber`).
6. Consumer outbox cho phép consumer publish tiếp mà vẫn nguyên tử (📖).

### Nhược điểm

1. **Giấy phép**: v9 thương mại; ở lại v8 thì phải tự theo dõi vòng đời hỗ trợ.
2. **Ít quan sát hơn**: không có `Attempts`/`LastError`/lịch sử dòng đã gửi; phải dựa vào log và metric.
3. **Giữ khoá DB trong lúc gọi broker** (✅) → nhạy với broker chậm (phụ thuộc `MessageDeliveryTimeout`).
4. **Retry relay không có backoff tăng dần** (✅ đều đặn mỗi `QueryDelay`) → broker sập lâu thì log lỗi đều đặn.
5. **Mặc định cần chỉnh**: `DuplicateDetectionWindow` 30 phút, `QueryDelay` 10 s.
6. **Thêm ba bảng và quy ước** (tên bảng cố định, `MessageUrn`, middleware order) phải học.
7. **Log nhiễu**: EF ghi Error cho các tranh chấp MassTransit tự xử lý.

### Khi nào chọn cái nào

- **MassTransit (hoặc NServiceBus, Wolverine, CAP…)**: production, nhiều service, nhiều loại message, cần retry/hàng lỗi/topology chuẩn, chấp nhận điều kiện giấy phép.
- **Tự viết**: học cơ chế; hệ thống rất nhỏ, ít message; cần kiểm soát hoàn toàn (vd. lease thay vì khoá, cột `Attempts` để cảnh báo); hoặc không thể dùng thư viện vì giấy phép.

---

## 7. Scale-out: nhiều instance

```
                 ┌── Orders #1 (API + BusOutboxDeliveryService) ─┐
 Load balancer ─►├── Orders #2 (API + BusOutboxDeliveryService) ─┤──► [ DB: Orders + OutboxMessage/OutboxState ]
                 └── Orders #3 (API + BusOutboxDeliveryService) ─┘          ▲ UPDLOCK + READPAST trên OutboxState
                                       │ gửi
                                       ▼
                                  [ Broker ] ── competing consumers ──► Inventory #1..#3 ──► [ DB: Stock + InboxState ]
```

- **Relay:** mỗi instance chạy một `BusOutboxDeliveryService`; chúng chia việc qua `SELECT TOP 1 ... WITH (UPDLOCK, ROWLOCK, READPAST)` trên `OutboxState`. Không cần leader election.
  - ✅ **Tiếp quản:** instance A không với tới transport; 10 order đều đi qua A; instance B giao cả 10 và consumer của B xử lý cả 10; A không xử lý cái nào; kho đúng.
  - ✅ **Hai instance khoẻ:** 60 order gửi xen kẽ vào A và B cùng lúc → đúng 60 lần trừ kho, 60 dòng `InboxState`, không trùng tác dụng.
- **Consumer:** với broker thật, nhiều instance cùng đọc một queue (competing consumers); broker chia message. Inbox phải nằm ở DB **dùng chung** (✅ inbox trong DB chặn bản trùng).
- **Lưu ý demo:** mỗi instance có transport in-memory **riêng**, nên message do relay của instance nào gửi thì consumer của chính instance đó nhận. Với broker thật dùng chung thì không còn như vậy.
- **Thứ tự** giữa các transaction/instance không bảo đảm (📖). Cần thì dùng partition/session theo `OrderId` của broker.

---

## 8. Thay transport in-memory bằng broker thật

Phần outbox/inbox **không đổi**. Chỉ thay transport, và bỏ các công tắc demo (`TransportSwitch`, `ConsumerFaults`, `DemoController`).

RabbitMQ (cần thêm package `MassTransit.RabbitMQ` cùng phiên bản 8.5.11):

```csharp
x.UsingRabbitMq((context, cfg) =>
{
    cfg.Host("localhost", "/", h =>
    {
        h.Username("guest");
        h.Password("guest");
    });

    cfg.ConfigureEndpoints(context);
});
```

Azure Service Bus (package `MassTransit.Azure.ServiceBus.Core`): `x.UsingAzureServiceBus((context, cfg) => { cfg.Host(connectionString); cfg.ConfigureEndpoints(context); });`

> Đoạn mã broker thật ở mục này **chưa được chạy** trong project (máy không có RabbitMQ/Service Bus). Kiểm tra lại với tài liệu của đúng phiên bản trước khi dùng.

---

## 9. Kết quả chạy thử trong project này

Chạy bằng test tự động [outbox_pattern.MassTransitDemo.Tests](../../outbox_pattern.MassTransitDemo.Tests): 13 test tích hợp (kịch bản 1–13 dưới đây, mỗi test một instance ứng dụng thật — Program.cs, bus, dịch vụ giao outbox, consumer — trên database LocalDB tạm, xoá khi xong) và 9 unit/architecture test không cần database (kịch bản 14–15). Trước khi tách Clean Architecture, 13 test tích hợp chạy 12 lần liên tiếp đều xanh; sau khi tách, toàn bộ 22 test chạy 5 lần liên tiếp đều xanh, ~7 giây mỗi lần.

| # | Kịch bản | Kết quả đo được | Test |
|---|---|---|---|
| 1 | POST /orders khi transport sập | 201; 1 dòng `Orders`, 1 `OutboxState` (`Delivered` NULL), 1 `OutboxMessage` (`MessageType` = `urn:message:orders.order-created.v1`, `Body` chứa OrderId); kho, inbox chưa đổi | `PostOrder_WritesTheOrderAndOneUndeliveredOutboxMessage` |
| 2 | INSERT OutboxMessage lỗi (CHECK constraint) **sau** khi INSERT Orders đã chạy trong cùng batch | 500; không còn dòng nào ở `Orders`, `OutboxState`, `OutboxMessage` | `PostOrder_WhenTheOutboxInsertFails...` |
| 3 | Request sai (400) | Không ghi gì | `PostOrder_WithAnInvalidBody...` |
| 4 | Luồng cơ bản | Kho 1000 → 998; `OutboxMessage`/`OutboxState` bị xoá; 1 dòng `InboxState` đã `Consumed`/`Delivered` | `EndToEnd_...` |
| 5 | Độ trễ | ~60 ms từ lúc POST trả về tới lúc kho đổi, kể cả khi `QueryDelay` = 5 s | đo riêng khi viết tài liệu |
| 6 | Transport sập rồi hồi phục | `Outbox Send Fault` lặp lại (~mỗi `QueryDelay`); dòng outbox còn; kho chưa đổi; bật lại thì giao, kho trừ đúng 1 lần | `TransportDown_...` |
| 7 | Transport treo | Có khoá U/X trên `OutboxState`/`OutboxMessage` suốt lúc treo; nhả khi giao xong | `WhileTheTransportHangs_...` |
| 8 | Transport treo + `MessageDeliveryTimeout` 1 s | `Outbox Send Timeout after 00:00:01` lặp lại; dòng outbox còn; hồi phục thì giao | `ATransportThatNeverAnswers_...` |
| 9 | Consumer lỗi 2 lần | Lần 3 thành công; kho trừ 1 lần; có `R-RETRY`, không `R-FAULT` | `ConsumerFailsTwice_...` |
| 10 | Consumer lỗi mãi | Đúng 4 lần gọi (1 + 3 retry) cho **một** message; `R-FAULT`; kho không đổi; `InboxState.Consumed` NULL | `ConsumerFailsBeyondTheRetries_...` |
| 11 | 5 bản cùng `MessageId` | Kho trừ 1 lần; 1 dòng `InboxState`; `ReceiveCount` ≥ 5 | `TheSameMessageDeliveredSeveralTimes_...` |
| 12 | Instance A mất transport, B khoẻ | B giao và xử lý cả 10 order đi qua A | `AnotherInstance_TakesOver...` |
| 13 | Hai instance khoẻ, 60 order | Đúng 60 lần trừ kho, 60 dòng inbox | `TwoHealthyInstances_...` |
| 14 | Use case với cổng giả (không DB) | `CreateOrderHandler` gọi đúng thứ tự add → publish → commit một lần; domain từ chối thì không publish, không commit; `ReserveStockHandler` dịch đúng kết quả | [UseCaseTests.cs](../../outbox_pattern.MassTransitDemo.Tests/Unit/UseCaseTests.cs) |
| 15 | Quy tắc phụ thuộc | Domain không dùng project/framework nào; Application không dùng Infrastructure, Api, EF Core, MassTransit; Infrastructure không dùng Api | [ArchitectureTests.cs](../../outbox_pattern.MassTransitDemo.Tests/Unit/ArchitectureTests.cs) |

Các giá trị mặc định ở mục 4.1 được đọc trực tiếp từ MassTransit 8.5.11 bằng reflection lúc viết tài liệu.

---

## 10. Chạy thử

Yêu cầu: .NET 10 SDK và SQL Server LocalDB. Database `OutboxPatternMassTransitDemo` (khác database của project `outbox_pattern`) được **tự tạo** ở lần chạy đầu (`EnsureCreated`), gồm cả ba bảng của MassTransit.

```powershell
cd "E:\Sơn\outbox pattern\outbox pattern git\outbox-pattern-"
dotnet run --project outbox_pattern.MassTransitDemo.Api --launch-profile http        # http://localhost:5200
```

Trong Visual Studio: chọn `outbox_pattern.MassTransitDemo.Api` làm startup project (nằm trong thư mục solution `MassTransitDemo`).

- **Swagger UI:** `http://localhost:5200/swagger`. Project này không chuyển hướng sang HTTPS ở môi trường Development nên dùng được cả http lẫn https (profile `https` có thêm `https://localhost:7228`).
- **Kịch bản có sẵn:** mở [outbox_pattern.MassTransitDemo.Api.http](../outbox_pattern.MassTransitDemo.Api.http) và chạy lần lượt 0 → 6.
- **Quan sát:** `GET /demo/state` trả về số order, số dòng outbox đang chờ, inbox, tồn kho và trạng thái hai công tắc. Log của app có `Outbox Send Fault`, `Outbox Send Timeout`, `R-RETRY`, `R-FAULT`, `Order …: reserved …`.
- **Nhìn bằng SSMS:** server `(localdb)\MSSQLLocalDB`, tích "Trust Server Certificate", database `OutboxPatternMassTransitDemo`, các bảng `OutboxMessage`, `OutboxState`, `InboxState`. Dòng outbox biến mất ngay khi được giao, nên muốn thấy nó thì `PUT /demo/transport/down` trước khi tạo order.

Chạy test:

```powershell
cd "E:\Sơn\outbox pattern\outbox pattern git\outbox-pattern-"
dotnet test outbox_pattern.MassTransitDemo.Tests
```

Test dùng `(localdb)\MSSQLLocalDB` và tự tạo rồi xoá database `MassTransitDemoTests_<guid>`. Đặt biến môi trường `OUTBOX_TESTS_SQL_SERVER` (vd. `(localdb)\TênInstanceKhác`) để chạy trên instance/server khác.

---

## 11. Giới hạn của bản demo và checklist production

### Giới hạn

- **Transport in-memory:** mất message (kể cả hàng `_error`) khi tắt process; mỗi process một transport.
- **Producer và consumer dùng chung một `DbContext`/database** cho gọn. Thực tế Inventory có database riêng với bảng `InboxState` riêng.
- **`EnsureCreated`** thay cho migrations.
- **Công tắc demo** (`TransportSwitch`, `ConsumerFaults`, `/demo/*`) chỉ để học, phải bỏ ở production.
- **Chưa thử:** `kill -9` relay giữa lúc gửi, broker thật, cleanup inbox sau đúng `DuplicateDetectionWindow`, thứ tự message giữa nhiều instance.

### Checklist production

- [ ] Quyết định **giấy phép/phiên bản**: ở lại v8 (Apache-2.0) hay mua v9 (mục 1.3).
- [ ] **EF migrations** cho cả ba bảng của MassTransit; không dùng `EnsureCreated`.
- [ ] **Broker thật** (mục 8); xác nhận broker lưu bền message trước khi xác nhận.
- [ ] `DuplicateDetectionWindow` **lớn hơn** cửa sổ giao lại dài nhất có thể (gồm cả lúc đẩy lại từ `_error`).
- [ ] `MessageDeliveryTimeout` hợp lý; theo dõi khoá/connection khi broker chậm.
- [ ] **Giám sát**: số dòng `OutboxState` có `Delivered IS NULL` và tuổi `Created` lâu nhất; tần suất log `Outbox Send Fault`/`Outbox Send Timeout`; độ dài hàng `_error`.
- [ ] **Quy trình cho hàng `_error`**: ai xem, sửa, đẩy lại thế nào.
- [ ] **Lọc log nhiễu** (`2627`, `DbUpdateConcurrencyException` do MassTransit tự xử lý) khỏi cảnh báo.
- [ ] **Version hoá message** bằng `[MessageUrn("...v1")]`; tương thích ngược khi rolling deploy.
- [ ] Giữ **idempotency key ở API** cho request từ client (outbox không giải quyết việc này).
- [ ] Không gửi tác dụng ngoài DB (email, HTTP) trực tiếp trong consumer mà không có idempotency riêng.

---

## 12. Đọc thêm

- MassTransit — [Transactional Outbox (cấu hình EF Core)](https://masstransit.massient.com/documentation/configuration/middleware/outbox) (masstransit.io hiện chuyển hướng tới tài liệu mới ở masstransit.massient.com).
- MassTransit — [Thông báo về v9](https://masstransit.massient.com/introduction/v9-announcement) và [trang hỗ trợ](https://masstransit.io/support) (giấy phép thương mại, hỗ trợ cho v8).
- NuGet — [MassTransit](https://www.nuget.org/packages/MassTransit) (trang các bản 9.x ghi rõ yêu cầu license; bản 8.x là Apache-2.0).
- [OUTBOX_PATTERN.md](../../outbox_pattern/docs/OUTBOX_PATTERN.md) — lý thuyết nền, bản tự viết, và các case scale-out P1–P13, C1–C7.
