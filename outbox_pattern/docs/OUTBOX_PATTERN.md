# Outbox Pattern — giải thích chi tiết và cách áp dụng trong project này

Tài liệu đi kèm code của project. Những chỗ ghi **"đã kiểm chứng"** là kết quả chạy thật trên project này (tổng hợp ở [mục 10](#10-kết-quả-chạy-thử-trong-project-này)); những chỗ không ghi là kiến thức/lý thuyết chung, chưa được tái hiện trong project.

## Mục lục

0. [Tóm tắt 30 giây](#0-tóm-tắt-30-giây)
1. [Bài toán gốc: dual-write](#1-bài-toán-gốc-dual-write)
2. [WHAT — Outbox Pattern là gì](#2-what--outbox-pattern-là-gì)
3. [WHY — vì sao cần: ACID, 2PC và phát biểu bạn đã gặp](#3-why--vì-sao-cần-acid-2pc-và-phát-biểu-bạn-đã-gặp)
4. [HOW — hoạt động từng bước và cách cài trong project](#4-how--hoạt-động-từng-bước-và-cách-cài-trong-project)
5. [Bảo đảm giao hàng: at-least-once và vì sao cần Inbox](#5-bảo-đảm-giao-hàng-at-least-once-và-vì-sao-cần-inbox)
6. [Ưu điểm và nhược điểm](#6-ưu-điểm-và-nhược-điểm)
7. [Khi nào dùng, khi nào không, và các lựa chọn thay thế](#7-khi-nào-dùng-khi-nào-không-và-các-lựa-chọn-thay-thế)
8. [Các biến thể triển khai relay và cách thay broker thật](#8-các-biến-thể-triển-khai-relay-và-cách-thay-broker-thật)
9. [Scale-out: thêm nhiều instance cho mỗi service](#9-scale-out-thêm-nhiều-instance-cho-mỗi-service)
10. [Kết quả chạy thử trong project này](#10-kết-quả-chạy-thử-trong-project-này)
11. [Chạy thử](#11-chạy-thử)
12. [Giới hạn của bản demo và checklist đưa lên production](#12-giới-hạn-của-bản-demo-và-checklist-đưa-lên-production)
13. [Đọc thêm](#13-đọc-thêm)

---

## 0. Tóm tắt 30 giây

- **Vấn đề:** lưu dữ liệu vào DB *và* gửi event lên broker là ghi vào **hai hệ thống**. Không có transaction chung, nên sẽ có lúc "order đã lưu nhưng event mất" hoặc "event đã gửi nhưng order không tồn tại".
- **Cách giải:** chỉ ghi vào **một** nơi là DB. Trong **cùng một transaction**, ghi dữ liệu nghiệp vụ *và* một dòng "event cần gửi" vào bảng **outbox**. Một tiến trình nền (**relay**) đọc bảng outbox, gửi lên broker, gửi xong mới đánh dấu đã gửi.
- **Cái giá:** event được giao **ít nhất một lần** (có thể trùng) và **trễ một chút** (eventual consistency). Vì vậy bên nhận phải **idempotent**, thường bằng bảng **inbox**.

```
Không có outbox:   [ghi DB] ─────✗────► [gửi broker]      hai hệ thống, hai lần ghi, có thể lệch nhau
Có outbox:         [ghi DB + ghi outbox: 1 transaction] ──(sau đó, bất đồng bộ)──► [relay] ─► [broker]
```

---

## 1. Bài toán gốc: dual-write

### 1.1 Ví dụ cụ thể

Use case `POST /orders`: (a) lưu order vào DB; (b) gửi event `OrderCreated` để Inventory trừ kho. Cách viết "tự nhiên" nhất — đúng là thứ `POST /demo/orders-naive` trong project này làm để đối chứng:

```csharp
db.Orders.Add(order);
await db.SaveChangesAsync();           // (1) ghi DB
await broker.PublishAsync(message);    // (2) ghi broker
```

DB và broker là hai hệ thống độc lập; không có transaction nào bao cả hai. Giữa (1) và (2) luôn có một khoảng hở.

### 1.2 Các cách "ngây thơ" và vì sao đều hỏng

| Cách làm | Hỏng khi nào | Hậu quả |
|---|---|---|
| (1) rồi (2): commit rồi mới publish | Broker lỗi, mạng đứt, hoặc process chết đúng giữa (1) và (2) | **Event mất vĩnh viễn**: order có trong DB nhưng Inventory không bao giờ biết |
| (2) rồi (1): publish rồi mới commit | Commit lỗi (vi phạm ràng buộc, deadlock, mất kết nối) hoặc process chết sau khi đã publish | **Event "ma"**: hệ thống khác xử lý một order chưa từng tồn tại |
| Publish *trong* transaction, trước khi commit | Như dòng trên, và thêm: transaction + lock bị giữ suốt thời gian gọi mạng | Event ma + DB bị kẹt khi broker chậm |
| Bọc `try/catch` rồi retry publish | Process chết khi đang chờ retry (retry nằm trong RAM, chết là mất). Hoặc publish thật ra đã thành công nhưng ack bị mất, retry gửi lần hai | Vẫn mất event, hoặc event bị trùng |

Điểm mấu chốt: **`try/catch` không cứu được crash**. `kill -9`, OOM, node bị evict, mất điện không chạy `catch`/`finally`. Mọi lời giải phải đúng ngay cả khi process chết ở **bất kỳ dòng code nào**.

**Đã kiểm chứng** (kịch bản 4): tắt broker rồi gọi `POST /demo/orders-naive` → HTTP 500, nhưng order **vẫn nằm trong DB**, không còn dấu vết nào của event; bật broker lại cũng không có gì được gửi bù. Cùng điều kiện, `POST /orders` (có outbox) trả 201 và event được giao ngay khi broker hồi phục.

---

## 2. WHAT — Outbox Pattern là gì

### 2.1 Định nghĩa

> Thay vì gửi event trực tiếp lên broker, service **ghi event vào một bảng (outbox) nằm trong chính database của nó, cùng một transaction** với thay đổi dữ liệu nghiệp vụ. Một thành phần riêng (**message relay**) đọc bảng outbox và chuyển các event sang broker.

Nhờ vậy, "ghi dữ liệu" và "ghi nhận ý định phát event" trở thành **một thao tác nguyên tử duy nhất trên một hệ thống duy nhất** (DB). Phần khó (đưa event sang hệ thống khác) được tách ra làm sau, bất đồng bộ, và có thể thử lại bao nhiêu lần cũng được vì "lời hứa" đã nằm an toàn trong DB.

Pattern còn được gọi là **Transactional Outbox**. Phía nhận có pattern đối ngẫu là **Inbox** (hay *Idempotent Receiver*).

### 2.2 Ẩn dụ

Bạn đang ký hợp đồng và phải gửi thư thông báo cho đối tác. Cách dở: vừa ký xong là chạy ra bưu điện; nếu bưu điện đóng cửa, hoặc bạn bị tai nạn trên đường, hợp đồng đã ký mà thư không đi. Cách tốt: ký xong, **bỏ thư vào khay "thư đi" ngay trên bàn** (cùng một hành động hoàn tất công việc). Nhân viên bưu điện (relay) đến lấy theo lịch; nếu hôm nay bưu điện đóng cửa, thư vẫn nằm trong khay. Thỉnh thoảng thư có thể được gửi hai lần (người nhận cần biết bỏ qua thư trùng, đó là Inbox).

### 2.3 Các thành phần

| Thành phần | Vai trò | Trong project |
|---|---|---|
| Bảng **Outbox** | Hàng đợi bền vững các event chờ gửi, nằm trong DB nghiệp vụ | `OutboxMessages` — [OutboxMessage.cs](../Messaging/OutboxMessage.cs) |
| **Producer** | Ghi dữ liệu nghiệp vụ + dòng outbox trong *cùng* transaction | [OrdersController.cs](../Controllers/OrdersController.cs) |
| **Relay** | Đọc outbox, publish lên broker, đánh dấu đã gửi, retry khi lỗi | [OutboxProcessor.cs](../Messaging/OutboxProcessor.cs) |
| **Broker** | Chuyển event tới các bên quan tâm | [InMemoryBroker.cs](../Messaging/InMemoryBroker.cs) (giả lập) |
| **Consumer + Inbox** | Nhận event, khử trùng lặp, áp dụng tác dụng đúng một lần | [InventoryConsumer.cs](../Inventory/InventoryConsumer.cs), [InboxMessage.cs](../Messaging/InboxMessage.cs) |
| **Cleanup** | Xoá dòng outbox/inbox đã cũ | [MessageCleanupService.cs](../Messaging/MessageCleanupService.cs) |

### 2.4 Sơ đồ

```
┌──────────────────────────── Orders service ─────────────────────────────┐
│                                                                         │
│   HTTP POST /orders                                                     │
│        │                                                                │
│        ▼                                                                │
│   ┌──────────────── 1 transaction (ACID cục bộ) ────────────────┐      │
│   │  INSERT Orders          ← dữ liệu nghiệp vụ                  │      │
│   │  INSERT OutboxMessages  ← "tôi BẮT BUỘC phải publish event"  │      │
│   └──────────────────────────────────────────────────────────────┘      │
│                         │  (bảng outbox nằm ngay trong DB)              │
│                         ▼                                               │
│   Relay (BackgroundService): đọc dòng chưa gửi ─► publish ─► đánh dấu   │
└─────────────────────────┼───────────────────────────────────────────────┘
                          ▼
                   ┌─────────────┐
                   │   Broker    │
                   └──────┬──────┘
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Inventory service: Consumer — 1 transaction: INSERT inbox + UPDATE stock │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.5 Cấu trúc bảng outbox trong project

| Cột | Ý nghĩa |
|---|---|
| `Id` (Guid) | **MessageId** xuyên suốt hệ thống. Cố định theo dòng outbox: gửi lại bao nhiêu lần vẫn là cùng `Id`, nhờ đó consumer khử được trùng lặp |
| `Type` | Tên logic của event, vd. `orders.order-created.v1` (không gắn với tên class CLR, có version) |
| `AggregateId` | Khoá thứ tự/partition (vd. `OrderId`). Event cùng khoá cần xử lý đúng thứ tự |
| `Payload` | JSON của event. Relay chuyển nguyên văn, không deserialize nên không phụ thuộc schema |
| `OccurredOnUtc` | Thời điểm event xảy ra |
| `ProcessedOnUtc` | `NULL` = chưa gửi xong. Có giá trị = đã được broker xác nhận |
| `Attempts`, `LastError` | Số lần thử và lỗi gần nhất, phục vụ vận hành/cảnh báo |
| `NextAttemptOnUtc` | Sớm nhất khi nào được thử lại (exponential backoff) |

Hai index (xem [AppDbContext.cs](../Persistence/AppDbContext.cs)): `IX_OutboxMessages_Pending` là **filtered index chỉ chứa dòng chưa xử lý** (`WHERE ProcessedOnUtc IS NULL`) nên luôn nhỏ dù bảng có hàng triệu dòng lịch sử; `IX_OutboxMessages_Processed` phục vụ job dọn dẹp.

---

## 3. WHY — vì sao cần: ACID, 2PC và phát biểu bạn đã gặp

### 3.1 ACID và outbox khai thác nó thế nào

ACID là bốn bảo đảm của một transaction **trên một database**:

| | Ý nghĩa | Outbox dựa vào nó như thế nào |
|---|---|---|
| **A**tomicity | Tất cả hoặc không gì cả | `INSERT Orders` + `INSERT OutboxMessages` hoặc cùng được commit, hoặc cùng bị rollback. Không có order mồ côi, không có event ma |
| **C**onsistency | Transaction đưa DB từ trạng thái hợp lệ sang trạng thái hợp lệ | Bất biến "mỗi order đều có đúng một event OrderCreated đang chờ hoặc đã gửi" đúng tại *mọi* thời điểm commit |
| **I**solation | Các transaction đồng thời không thấy trạng thái dở dang của nhau | Relay (một transaction khác) chỉ thấy hai dòng khi *cả hai* đã commit, không bao giờ thấy outbox mà thiếu order |
| **D**urability | Đã commit là bền vững, kể cả khi mất điện | Ngay khi `SaveChanges` trả về, event đã nằm trên đĩa. Process chết ngay sau đó cũng không mất event |

**Đã kiểm chứng:** log SQL thật của `POST /orders` cho thấy chính xác một transaction, một batch chứa cả hai `INSERT`, rồi commit:

```
Began transaction with isolation level 'ReadCommitted'.
Executed DbCommand:  SET NOCOUNT ON;
  INSERT INTO [Orders] ([Id], [CreatedOnUtc], [CustomerEmail], [Quantity], [Sku]) VALUES (...);
  INSERT INTO [OutboxMessages] ([Id], [AggregateId], [Attempts], ..., [Type]) VALUES (...);
Committing transaction.
Committed transaction.
```

### 3.2 Two-Phase Commit (2PC) là gì và vì sao bị né tránh

Muốn "ghi DB và ghi broker nguyên tử" theo cách trực diện thì cần **giao dịch phân tán**. Giao thức kinh điển là 2PC (chuẩn XA):

1. **Phase 1 — Prepare:** coordinator hỏi mọi participant (DB, broker…) "bạn commit được không?". Mỗi participant ghi đĩa trạng thái *prepared*, **giữ nguyên các lock**, rồi trả lời YES/NO.
2. **Phase 2 — Commit/Abort:** nếu *tất cả* YES, coordinator ghi quyết định rồi bảo mọi bên COMMIT. Chỉ cần một NO thì bảo mọi bên ROLLBACK.

Vì sao ngày nay bị hạn chế:

- **Blocking.** Nếu coordinator chết sau khi các participant đã vote YES, họ rơi vào trạng thái *in-doubt*: không dám tự commit, không dám tự abort, và **giữ lock cho đến khi coordinator hồi phục**.
- **Độ sẵn sàng bị nhân lên.** Giao dịch chỉ thành công khi *mọi* participant cùng online. Ba hệ thống 99,9% thì còn khoảng 99,7%.
- **Chậm và kém scale.** Thêm ít nhất hai vòng round-trip mạng và các lần ghi log đồng bộ; lock bị giữ qua cả các round-trip đó nên throughput giảm và dễ deadlock.
- **Hạ tầng hiện đại không hỗ trợ.** Kafka, RabbitMQ, Amazon SQS/SNS, Azure Service Bus, và hầu hết API HTTP/SaaS không tham gia XA với DB của bạn. Trong .NET, `TransactionScope` leo thang sang MSDTC vốn chỉ có trên Windows, vắng mặt ở phần lớn môi trường container/Linux/cloud.
- **Ràng buộc kiến trúc.** Microservices muốn các service tự chủ (mỗi service một DB, một vòng đời deploy). 2PC trói chặt chúng lại với nhau về độ sẵn sàng.

### 3.3 Phân tích phát biểu bạn đã gặp

> *"The Outbox pattern is (afaik) to address ACID. Especially in scenarios that a compensation operation cannot be used (…). In such a scenario that require an atomic "save", the only option would be a distributed transaction (2PC)… highly expensive… discouraged nowadays. Here is where the Outbox Pattern saves the day. By storing both the local entity and the message/event together in the same database, it ensures atomicity without the need for 2PC."*

Phát biểu này **đúng ở lõi** nhưng có vài chỗ nên nói chính xác hơn:

| Ý trong phát biểu | Đánh giá | Giải thích |
|---|---|---|
| "Lưu entity và event trong cùng DB → atomicity mà không cần 2PC" | ✅ **Đúng, đây là lõi của pattern** | Biến bài toán "atomic trên hai hệ thống" thành "atomic trên một hệ thống" |
| "2PC đắt đỏ, phức tạp, ngày nay không khuyến khích" | ✅ Đúng | Xem 3.2 |
| "Outbox giải quyết ACID" | ⚠️ Nói rộng quá | Outbox dùng **ACID cục bộ của một service** để bảo đảm *atomicity giữa thay đổi trạng thái và việc ghi nhận ý định phát event*. Nó **không** biến cả luồng end-to-end thành ACID. Sau commit, phần còn lại (relay → broker → consumer) là bất đồng bộ nên hệ thống chỉ **eventually consistent**: có một khoảng thời gian service A đã commit mà service B chưa biết gì |
| "Đặc biệt khi không dùng được compensation" | ⚠️ Chưa chính xác | Outbox và compensation (Saga) giải **hai bài toán khác nhau và bổ sung nhau**. Outbox: *đừng làm mất/làm ma event*. Saga: *nếu một bước sau thất bại về nghiệp vụ thì bù trừ thế nào*. Saga còn **cần** outbox để gửi các lệnh/event bù trừ một cách tin cậy. Khi một bước không thể bù trừ (đã gửi email, đã chuyển tiền), outbox không giúp "undo"; nó chỉ bảo đảm lệnh được giao *ít nhất một lần* — bạn vẫn phải thiết kế riêng (đặt bước không đảo ngược được ở cuối, idempotency, bước xác nhận…) |
| "Lựa chọn **duy nhất** là 2PC" | ⚠️ Không phải duy nhất | Còn Saga (kèm outbox), event sourcing, CDC trên log của DB, hay chấp nhận eventual consistency kèm job đối soát. Xem mục 7 |
| (ngầm hiểu) "Outbox cho atomicity là đủ" | ⚠️ Thiếu hai cái giá | (1) Giao hàng **at-least-once**, không phải exactly-once → consumer phải idempotent. (2) Thêm **độ trễ** và **một thành phần phải vận hành** (relay) |
| (ngầm hiểu) "Chỉ cần cùng database" | ⚠️ Cần chính xác hơn | Điều kiện thật là entity và outbox phải **cùng một transaction cục bộ** (cùng DbContext/connection/transaction). Nếu store không có transaction đa bản ghi (một số NoSQL), phải nhúng outbox vào cùng document hoặc dùng change stream của chính store đó |

Cách diễn đạt chính xác hơn của cùng ý:

> **Outbox thay một *giao dịch phân tán* (DB + broker) bằng *một giao dịch cục bộ* (DB) + *giao hàng bất đồng bộ at-least-once* + *consumer idempotent*.** Ta đánh đổi "atomic commit trên nhiều hệ thống" lấy "eventual consistency có bảo đảm".

---

## 4. HOW — hoạt động từng bước và cách cài trong project

### 4.1 Bước 1 — Ghi (producer)

[OrdersController.cs](../Controllers/OrdersController.cs): thêm `Order` *và* `OutboxMessage` vào **cùng DbContext**, rồi gọi **đúng một** `SaveChangesAsync`. EF Core gom mọi thay đổi chờ lưu vào một transaction, nên hai `INSERT` cùng sống hoặc cùng chết.

```csharp
db.Orders.Add(order);
db.OutboxMessages.Add(OutboxMessage.Create(OrderCreatedEvent.TypeName, aggregateId: order.Id.ToString(), orderCreated, now));
await db.SaveChangesAsync(cancellationToken);   // một transaction duy nhất
```

Controller **không bao giờ gọi broker**. Request trả về ngay sau khi commit nên không phụ thuộc độ trễ hay sự cố của broker (**đã kiểm chứng:** broker sập mà `POST /orders` vẫn 201).

### 4.2 Bước 2 — Relay: claim, publish, đánh dấu

[OutboxProcessor.cs](../Messaging/OutboxProcessor.cs) chạy như `BackgroundService`, mỗi nhịp (mặc định 1 giây) làm:

```
BEGIN TRANSACTION
  1. CLAIM  : SELECT TOP (20) ... FROM OutboxMessages WITH (UPDLOCK, READPAST, ROWLOCK)
              WHERE ProcessedOnUtc IS NULL AND NextAttemptOnUtc <= now
              ORDER BY NextAttemptOnUtc, OccurredOnUtc
  2. PUBLISH: với từng message → broker.PublishAsync (có timeout, chờ ack)
  3. MARK   : thành công → ProcessedOnUtc = now
              thất bại   → Attempts++, LastError, NextAttemptOnUtc = now + backoff, rồi DỪNG lô
COMMIT
```

Các quyết định thiết kế đáng chú ý:

- **Khoá hàng khi claim.** `UPDLOCK` giữ các dòng cho riêng relay này đến khi commit; `READPAST` khiến relay khác **bỏ qua** dòng đang bị khoá thay vì chờ; `ROWLOCK` tránh leo thang khoá cả bảng. Đây là cách các instance chia việc mà không cần điều phối. Tương đương PostgreSQL/MySQL 8/Oracle: `FOR UPDATE SKIP LOCKED`.
- **Chỉ đánh dấu "đã gửi" SAU khi broker xác nhận.** Đánh dấu trước thì thành at-most-once và mất event khi crash.
- **Process chết giữa chừng thì sao?** Transaction chưa commit nên SQL Server tự rollback khi connection đứt, các dòng quay lại "chưa xử lý" và instance khác nhặt lên. Hệ quả: message có thể bị gửi lại → at-least-once (xem mục 5).
- **Timeout cho mỗi lần publish** (`PublishTimeout`). Relay đang giữ transaction + row-lock; một lời gọi broker treo vô hạn sẽ giữ lock vô hạn.
- **Dừng lô ngay khi có lỗi.** Lỗi publish thường nghĩa là broker đang gặp sự cố. Dừng sớm để không giữ lock lâu và không "đốt" số lần thử của các message phía sau.
- **Backoff theo từng dòng** (2s, 4s, 8s… tối đa `MaxBackoff`). Một message "độc" luôn lỗi bị đẩy lùi nên **không chặn** các message khác.
- **Không bao giờ tự bỏ cuộc.** Relay thử lại vô hạn thay vì chuyển sang dead-letter sau N lần, vì broker sập 1 giờ không được phép làm mất event. Thay vào đó `Attempts`/`LastError` và `oldestPendingAgeSeconds` (trong `GET /demo/state`) là tín hiệu để cảnh báo. Xem mục 12 về poison message.
- **Tắt ứng dụng êm.** Khi nhận tín hiệu dừng, relay không đụng tới phần còn lại của lô và commit những message *đã* gửi (bằng `CancellationToken.None`) để giảm số bản trùng khi khởi động lại.
- **Relay không deserialize payload**, chỉ chuyển nguyên văn: không bị vỡ khi schema event đổi.

### 4.3 Bước 3 — Consumer + Inbox

[InventoryConsumer.cs](../Inventory/InventoryConsumer.cs) xử lý mỗi message trong **một transaction**:

```
BEGIN TRANSACTION
  1. INSERT InboxMessages (MessageId, Consumer)   ← khoá chính; trùng thì vi phạm → bỏ qua, không làm gì thêm
  2. UPDATE StockItems SET Available = Available - qty WHERE Sku = ... AND Available >= qty
COMMIT
```

Chính vì phép trừ `Available - qty` **không idempotent** nên inbox mới thực sự cần thiết: thiếu nó thì một bản trùng sẽ trừ kho hai lần. Hai bước nằm chung transaction nên "đã ghi nhận đã xử lý" và "tác dụng nghiệp vụ" hoặc cùng có hiệu lực, hoặc cùng không. Khoá chính `(MessageId, Consumer)` là chốt chặn cuối: ngay cả khi hai instance xử lý *cùng một message đồng thời*, DB chỉ cho đúng một bên thắng.

Lỗi nghiệp vụ (hết hàng, SKU lạ) **không** phải lỗi kỹ thuật: vẫn ghi inbox và không retry vô ích. Hệ thống thật sẽ phát event bù trừ (vd. `OrderRejected`). **Đã kiểm chứng:** SKU lạ được xử lý đúng một lần, không retry vô hạn.

### 4.4 Bước 4 — Dọn dẹp

[MessageCleanupService.cs](../Messaging/MessageCleanupService.cs) xoá theo lô 1000 dòng: outbox đã xử lý quá `OutboxRetention` (mặc định 7 ngày), inbox quá `InboxRetention` (mặc định 14 ngày). Lưu ý **inbox phải được giữ lâu hơn khoảng thời gian dài nhất mà một message còn có thể bị giao lại** (broker sập + backoff + redelivery); xoá sớm hơn thì bản trùng đến muộn bị xử lý như message mới.

### 4.5 Toàn cảnh một request và các "điểm chết"

```
Client      Orders API            DB                    Relay            Broker        Consumer
  │ POST       │                   │                      │                │              │
  │───────────►│ BEGIN             │                      │                │              │
  │            │ INSERT Orders     │                      │                │              │
  │            │ INSERT Outbox     │                      │                │              │
  │            │ COMMIT ──────────►│ (đã bền vững) ①      │                │              │
  │◄───────────│ 201 ②             │                      │                │              │
  │            │                   │◄─ BEGIN, SELECT ...  │                │              │
  │            │                   │   UPDLOCK,READPAST ──│                │              │
  │            │                   │                      │── publish ────►│              │
  │            │                   │                      │◄── ack ③ ──────│── deliver ──►│ BEGIN
  │            │                   │◄─ UPDATE processed ──│                │              │ INSERT inbox ⑤
  │            │                   │◄─ COMMIT ④           │                │              │ UPDATE stock
  │            │                   │                      │                │              │ COMMIT
```

| Chết/lỗi tại | Điều gì xảy ra | Hồi phục |
|---|---|---|
| Trước COMMIT ① | Không có order, không có outbox. Nhất quán | Client nhận lỗi/timeout và gọi lại |
| Sau COMMIT ①, trước khi client nhận 201 ② | Order + outbox đều có, nhưng client không biết | Event vẫn sẽ được gửi. Nhưng nếu client gọi lại sẽ tạo order thứ hai — **outbox không giải quyết idempotency của request đầu vào**, cần idempotency key ở API |
| Broker sập | Relay không publish được, ghi `Attempts`/`NextAttemptOnUtc` | Tự gửi bù khi broker hồi phục (đã kiểm chứng) |
| Ack bị mất ③ | Broker đã nhận nhưng relay tưởng lỗi, sẽ gửi lại | Consumer nhận bản trùng → inbox ⑤ chặn (đã kiểm chứng) |
| Relay chết sau publish, trước COMMIT ④ | Transaction rollback, dòng outbox vẫn "chưa xử lý" | Instance khác publish lại → bản trùng → inbox chặn (đã kiểm chứng bằng `kill -9`) |
| Consumer chết giữa transaction | Rollback cả inbox lẫn stock | Broker giao lại message chưa được ack |

### 4.6 Những lỗi hay gặp khi tự cài outbox

1. **Ghi outbox bằng DbContext/connection khác** với dữ liệu nghiệp vụ → không cùng transaction, pattern mất tác dụng.
2. **Gọi `SaveChanges` hai lần** (một cho entity, một cho outbox) → hai transaction → lại có khoảng hở.
3. **Vẫn publish thẳng trong request "cho nhanh"** rồi coi outbox là dự phòng → quay lại dual-write có lỗ hổng. Nếu muốn giảm độ trễ, hãy *đánh thức relay* sau commit, đừng publish song song.
4. **Đánh dấu "đã gửi" trước khi publish** → at-most-once, mất event khi crash.
5. **Không có timeout khi publish** → giữ transaction/lock vô hạn khi broker treo.
6. **Tạo `MessageId` mới mỗi lần gửi lại** → consumer không thể khử trùng lặp. `Id` phải cố định theo dòng outbox.
7. **Retry không backoff** → bão request khi broker vừa hồi phục.
8. **Quên inbox/idempotency ở consumer.**
9. **Không dọn bảng** → tăng vô hạn, làm chậm cả hệ thống.
10. **Relay giữ "con trỏ" id lớn nhất đã xử lý** → bỏ sót vĩnh viễn các transaction commit trễ (xem 9.1, case P9).

### 4.7 Công thức 7 bước để áp dụng vào service khác

1. Tạo bảng outbox trong **cùng DB nghiệp vụ**, kèm filtered index cho dòng chưa xử lý.
2. Mọi use case cần phát event: thêm dòng outbox vào **cùng DbContext, cùng một lần lưu**.
3. Viết relay: claim bằng row-lock → publish (timeout, chờ ack) → đánh dấu → commit; lỗi thì backoff.
4. Gắn `MessageId` cố định vào message.
5. Consumer: ghi inbox (PK) + tác dụng nghiệp vụ trong **một transaction**.
6. Dọn dẹp định kỳ; giám sát backlog, tuổi message cũ nhất, số lần thử.
7. Thử sự cố thật: tắt broker, mất ack, `kill -9` relay, chạy từ hai instance trở lên.

---

## 5. Bảo đảm giao hàng: at-least-once và vì sao cần Inbox

| Mức | Nghĩa | Cách đạt | Rủi ro |
|---|---|---|---|
| At-most-once | Tối đa một lần | Gửi, không retry | **Mất** message |
| **At-least-once** | Ít nhất một lần | Gửi lại đến khi có ack | **Trùng** message |
| Exactly-once (giao hàng) | Đúng một lần | Không đạt được trong trường hợp tổng quát trên mạng không tin cậy (bài toán Two Generals) | — |

Outbox cho **at-least-once**: nó loại bỏ "mất" nhưng chấp nhận "trùng". Bản trùng đến từ ba nguồn không tránh khỏi: ack bị mất (relay không biết message đã tới), relay chết giữa publish và commit, và broker redelivery.

Thứ ta làm được là **effectively-once** = *at-least-once* + *xử lý idempotent*. Các kỹ thuật idempotency:

- **Inbox table** (project này): ghi `MessageId` đã xử lý trong cùng transaction với tác dụng nghiệp vụ. Tổng quát nhất.
- **Idempotent tự nhiên:** `UPSERT`, "set trạng thái = X" thay vì "cộng/trừ". Đơn giản nhất nếu nghiệp vụ cho phép.
- **Kiểm tra version/sequence** theo aggregate: bỏ qua event cũ hơn bản đã áp dụng. Giải quyết cả trùng lặp lẫn đảo thứ tự.
- **Idempotency key ở API** cho request từ client (việc khác, nằm ngoài outbox).

**Đã kiểm chứng** (kịch bản 3, chế độ `LoseAck`): cùng một message được broker nhận **4 lần**, consumer áp dụng **đúng 1 lần** và bỏ qua 3 bản trùng; kho trừ đúng 7 chứ không phải 28.

---

## 6. Ưu điểm và nhược điểm

### Ưu điểm

1. **Atomic giữa trạng thái và event mà không cần 2PC.**
2. **Không mất event** kể cả khi broker, relay hay cả service sập: event nằm bền vững trong DB.
3. **Không có event "ma"**: rollback thì dòng outbox cũng rollback.
4. **Tách độ sẵn sàng:** service vẫn nhận request khi broker sập, và request không chịu độ trễ của broker (đã kiểm chứng).
5. **Không phụ thuộc loại broker.** Đổi RabbitMQ sang Kafka chỉ đổi `IMessagePublisher`.
6. **Dễ quan sát và audit:** bảng outbox là nhật ký các event; replay thủ công chỉ cần đặt lại `ProcessedOnUtc = NULL`.
7. **Scale ngang được** bằng row-lock hoặc partition (mục 9).
8. **Nhất quán nội tại kể cả khi DB mất dữ liệu** (vd. failover với replication bất đồng bộ): order và event mất *cùng nhau* vì chung một transaction.

### Nhược điểm

1. **At-least-once → bản trùng**, bắt buộc consumer idempotent (thêm bảng inbox và logic).
2. **Độ trễ:** với polling, event đến muộn tới một chu kỳ quét (mặc định 1s). Hệ thống eventually consistent; UI không được giả định kho đã cập nhật ngay sau khi tạo order.
3. **Thêm tải lên DB nghiệp vụ:** mỗi event tốn thêm 1 `INSERT` + 1 `UPDATE` (+ 1 `DELETE` khi dọn), cộng truy vấn polling liên tục, cộng dung lượng.
4. **Thêm một thành phần phải vận hành** (relay): backlog, poison message, dead-letter, cảnh báo.
5. **Thứ tự khó giữ** khi có nhiều relay hoặc retry (mục 9, case P2).
6. **Phải có store giao dịch:** nếu store không có transaction đa bản ghi thì phải nhúng outbox vào document hoặc dùng change stream.
7. **Trần throughput bị DB giới hạn** (insert + polling + lock). Ở quy mô rất lớn cần CDC/partition.
8. **Schema event tồn tại lâu** trong bảng và trên broker nên phải quản lý version và tương thích.
9. **Không giải quyết** idempotency của request đầu vào, cũng không giải quyết đọc-sau-ghi ở service khác.
10. **Dữ liệu nhạy cảm nằm thêm một chỗ** (payload) → cần chính sách retention/xoá.

---

## 7. Khi nào dùng, khi nào không, và các lựa chọn thay thế

### Nên dùng khi

- Service ghi vào **DB giao dịch** *và* phải thông báo cho hệ thống khác, mà **mất hoặc ma event có hậu quả nghiệp vụ**: đặt hàng, thanh toán, tồn kho, kế toán, đối soát.
- Kiến trúc microservices/event-driven, Saga (choreography hoặc orchestration).
- Broker không hỗ trợ XA (hầu hết) hoặc bạn không muốn 2PC.
- Đồng bộ **read model** (CQRS), search index, cache, data warehouse từ dữ liệu nguồn.
- Gọi hệ thống ngoài (email, webhook, ERP) một cách tin cậy — "reliable side effects".
- Cần **audit trail** các event đã phát.

### Có thể không cần khi

- Event chỉ là **best-effort** (metrics, log, analytics thô, thông báo UI) và mất vài cái không sao.
- Mọi thứ nằm trong **một process, một DB**: xử lý domain event ngay trong transaction, không cần broker.
- Cần độ trễ cực thấp hoặc throughput cực lớn mà DB thành nút cổ chai → cân nhắc CDC trực tiếp, event sourcing, hoặc giao dịch của Kafka.
- State store **không có transaction** và không nhúng được outbox → dùng change feed của chính store (DynamoDB Streams, Cosmos DB change feed, MongoDB change streams).
- Pipeline **thuần Kafka** (consume → transform → produce): dùng Kafka transactions / exactly-once của Kafka.
- Bên nhận có thể **kéo** dữ liệu qua API/event feed thay vì được đẩy.

### Các lựa chọn thay thế

| Cách | Ý tưởng | Điểm mạnh | Điểm yếu |
|---|---|---|---|
| **2PC / XA** | Coordinator cam kết nguyên tử trên DB + broker | Atomic thật sự | Blocking, chậm, ít hạ tầng hỗ trợ (mục 3.2) |
| **Saga (+ outbox)** | Chuỗi giao dịch cục bộ + giao dịch bù trừ | Phù hợp quy trình nhiều bước, nhiều service | Không tự giải quyết dual-write; vẫn cần outbox để gửi tin cậy |
| **CDC trực tiếp trên bảng nghiệp vụ** | Debezium đọc log của bảng `Orders` rồi phát event | Không cần bảng outbox, độ trễ thấp | Lộ schema nội bộ ra ngoài; khó kiểm soát nội dung/version event |
| **Event sourcing** | Event store *là* nguồn sự thật | Event = trạng thái, không có dual-write | Thay đổi kiến trúc lớn, đường cong học tập cao |
| **Publish-first / "listen to yourself"** | Publish lên broker trước, rồi chính service tự consume để ghi DB | Không cần outbox | Đọc-sau-ghi chậm, nhiều trạng thái trung gian, khó lý giải |
| **Kafka transactions** | Ghi nhiều topic/offset nguyên tử | Exactly-once trong Kafka | Không bao gồm DB bên ngoài |
| **Best-effort + job đối soát** | Gửi thẳng, định kỳ so khớp và sửa lệch | Đơn giản | Chấp nhận sai lệch tạm thời; cần job đối soát |

---

## 8. Các biến thể triển khai relay và cách thay broker thật

### 8.1 Polling publisher (project này)

Relay quét bảng outbox theo chu kỳ.
- ✅ Đơn giản, chạy trên mọi DB hỗ trợ transaction, ít hạ tầng.
- ⚠️ Độ trễ bằng chu kỳ quét; thêm tải polling lên DB; thứ tự phụ thuộc cách claim.

### 8.2 Transaction log tailing / CDC

Một tiến trình đọc **log giao dịch** của DB và chuyển các dòng outbox mới thành message: Debezium với *Outbox Event Router*, SQL Server CDC/Change Tracking, PostgreSQL logical replication.
- ✅ Độ trễ thấp (mili-giây), không polling, thứ tự theo đúng thứ tự commit, không cần cột `ProcessedOnUtc`.
- ⚠️ Hạ tầng nặng hơn (Kafka Connect/Debezium), vận hành phức tạp, phụ thuộc DB cụ thể.

### 8.3 Polling được "đánh thức"

Giữ polling làm lưới an toàn, nhưng bổ sung tín hiệu để giảm độ trễ: PostgreSQL `LISTEN/NOTIFY`, tín hiệu in-process sau khi commit, hoặc thông báo từ DB.

### 8.4 Dùng thư viện thay vì tự viết

Project này tự cài để bạn thấy từng cơ chế. Ở production nên cân nhắc thư viện đã được kiểm chứng có sẵn outbox/inbox, như MassTransit (EF Core outbox), NServiceBus (Outbox), Wolverine (durable inbox/outbox) hoặc CAP (DotNetCore.CAP). **Hãy kiểm tra license và phiên bản hiện hành** của từng thư viện trước khi chọn.

### 8.5 Cách claim: khoá trong transaction hay lease

| | Khoá trong transaction (project này) | Lease (cột `LockedBy`/`LockedUntil`) |
|---|---|---|
| Cách làm | `SELECT ... UPDLOCK, READPAST` → publish → `UPDATE` → `COMMIT` | `UPDATE ... SET LockedBy, LockedUntil ... OUTPUT inserted.*` (transaction ngắn) → publish **ngoài** transaction → `UPDATE ... WHERE Id AND LockedBy = @me` |
| Instance chết | Connection đứt → DB tự nhả khoá ngay | Phải chờ lease hết hạn mới có người tiếp quản |
| Giữ connection/khoá khi gọi broker | Có (cần timeout + lô nhỏ) | Không |
| Trùng khi lease hết hạn trong lúc publish chậm | Không | Có thể |
| Phụ thuộc | Cần `SKIP LOCKED`/`READPAST` | Chạy được trên DB không có `SKIP LOCKED` |

Câu truy vấn claim tương đương trên PostgreSQL:

```sql
SELECT * FROM outbox_messages
WHERE processed_on_utc IS NULL AND next_attempt_on_utc <= now()
ORDER BY next_attempt_on_utc, occurred_on_utc
LIMIT 20
FOR UPDATE SKIP LOCKED;
```

Lưu ý với SQL Server: `READPAST` chỉ dùng được ở mức cô lập `READ COMMITTED` hoặc `REPEATABLE READ` (không phải `SERIALIZABLE`/`SNAPSHOT`).

### 8.6 Thay broker giả lập bằng broker thật

`Program.cs` đăng ký `IMessagePublisher` là `InMemoryBroker`. Để dùng RabbitMQ/Kafka/Azure Service Bus:

1. Viết một `IMessagePublisher` mới. Hợp đồng: **chỉ trả về thành công khi broker đã xác nhận** (publisher confirms của RabbitMQ, `acks=all` + idempotent producer của Kafka, ack của Service Bus). Mọi lỗi/timeout phải ném exception để relay biết mà thử lại. Truyền `BrokerMessage.Id` làm message id, `BrokerMessage.Type` làm type, `BrokerMessage.Key` làm routing/partition key để giữ thứ tự theo aggregate.
2. Thay `InventoryConsumer` bằng consumer của broker đó, **chỉ ack message sau khi transaction (inbox + tác dụng nghiệp vụ) đã commit**. Bên trong, logic `HandleAsync` giữ nguyên.
3. `OutboxProcessor`, bảng outbox, bảng inbox, dọn dẹp **không đổi**.

> Mã cụ thể cho từng broker cần được thử với broker thật; project này chưa chạy với broker ngoài nên không đính kèm để tránh đưa mã chưa kiểm chứng.

---

## 9. Scale-out: thêm nhiều instance cho mỗi service

### 9.1 Mô hình

```
                 ┌── Orders #1 (API + relay) ─┐
 Load balancer ─►├── Orders #2 (API + relay) ─┤──► [ DB: Orders + OutboxMessages ]  ◄─ dùng chung
                 └── Orders #3 (API + relay) ─┘            ▲
                                  │                        └ row-lock (UPDLOCK/READPAST) để chia việc
                                  ▼ publish
                            [   Broker   ]
                                  │ giao cho các consumer cạnh tranh (competing consumers)
                 ┌── Inventory #1 ─┐
                 ├── Inventory #2 ─┼──► [ DB: Stock + InboxMessages ]  ◄─ dùng chung
                 └── Inventory #3 ─┘
```

Khi nhân bản, **mỗi instance Orders đều chạy relay** và **mỗi instance Inventory đều chạy consumer**. Các case dưới đây là hệ quả. Ký hiệu: ✅ *đã kiểm chứng* bằng cách chạy thật trong project; ◐ *cơ chế có trong code nhưng chưa thử trong điều kiện thật*; 📖 *lý thuyết, chưa tái hiện*.

### 9.2 Phía producer / relay

**P1 — Nhiều relay đọc cùng một dòng → publish trùng.** ✅
Không có phối hợp thì N relay cùng `SELECT` thấy cùng các dòng "chưa xử lý" và cùng publish.
*Cách xử lý:* row-lock `UPDLOCK, READPAST` / `SKIP LOCKED` (project này); hoặc lease; hoặc leader election (chỉ một relay hoạt động, qua `sp_getapplock`/`pg_advisory_lock`/lease của Kubernetes); hoặc tách relay thành deployment riêng với 1 replica; hoặc CDC. Luôn giữ inbox làm lớp bảo vệ cuối.
*Số liệu:* 3 instance, 300 order → **300 lần publish cho 300 message, 0 trùng**. Bỏ hint khoá khỏi cùng đoạn code → **699 lần publish, 220 message bị gửi trùng**; tồn kho vẫn đúng chỉ nhờ inbox. Nghĩa là khoá hàng chống *lãng phí và rối thứ tự*, inbox bảo vệ *tính đúng đắn*; hai lớp bổ sung cho nhau.

**P2 — Đảo thứ tự event của cùng một aggregate.** 📖
Relay A đang giữ message #1 của order X (publish chậm hoặc lỗi và bị backoff), relay B claim message #2 của X và gửi trước ⇒ broker nhận #2 trước #1. Backoff theo từng dòng cũng gây đúng hiện tượng này kể cả chỉ có một relay.
```
Relay A: claim #1(X) ........ publish lỗi / chậm → backoff ........
Relay B:        claim #2(X) → publish OK
Broker nhận: #2, rồi #1    ✗
```
*Project này **không** bảo đảm thứ tự giữa các message/instance.* Cách xử lý, theo mức chặt tăng dần:
1. Thiết kế consumer chịu được đảo thứ tự: version/sequence theo aggregate, bỏ qua event cũ, ưu tiên event mang *trạng thái* thay vì *delta*.
2. Claim loại trừ aggregate đang có message trước nó chưa xong (`NOT EXISTS` bản ghi cũ hơn cùng `AggregateId`): đúng thứ tự, đổi lại throughput thấp hơn.
3. Chia outbox theo `hash(AggregateId) % N`, mỗi partition chỉ một relay hoạt động.
4. Một relay duy nhất + khoá partition của broker (`BrokerMessage.Key`).

**P3 — Tải polling nhân theo số instance.** 📖
N instance × 1 truy vấn/giây thì không đáng kể, nhưng 100 instance × quét mỗi 100ms thì có. *Xử lý:* jitter, polling thích ứng (ngủ lâu hơn khi rỗng), filtered index (project có), `LISTEN/NOTIFY`, chỉ vài instance chạy relay, hoặc CDC.

**P4 — Giữ transaction/khoá trong lúc gọi mạng.** ◐
Broker chậm ⇒ mỗi relay giữ một connection + row-lock lâu, connection pool của N instance cạn dần. *Xử lý:* `PublishTimeout`, lô nhỏ (`BatchSize`), dừng lô khi có lỗi (project có cả ba cơ chế, nhưng broker giả lập không bao giờ chậm nên chưa thử trong điều kiện thật), hoặc dùng lease để transaction ngắn.

**P5 — Instance chết hoặc bị thay thế (rolling deploy, autoscale-in, spot).** ✅
- *Chết đột ngột:* kill -9 một instance khi nó đang giữ lô chưa commit ⇒ DB rollback, khoá được nhả, instance còn lại **tự tiếp quản**; kết quả cuối `200/200`, kho `800` (đúng). Những message mà instance chết đã kịp publish được gửi lại và bị inbox chặn (10 bản trùng trong lần thử).
- *Tắt êm:* nhận tín hiệu dừng giữa lô ⇒ relay commit những message đã gửi, nhả phần còn lại; khi khởi động lại không phát sinh bản trùng.
- Với lease: thời gian tiếp quản = TTL của lease.

**P6 — Poison message.** 📖 (cơ chế backoff đã chạy, dead-letter thì chưa có)
Một message luôn lỗi (payload sai, bị broker từ chối vĩnh viễn). Với backoff theo dòng + dừng lô, nó không chặn cả hàng đợi nhưng tốn một lần thử mỗi chu kỳ backoff. *Cần thêm ở production:* giới hạn số lần cho **lỗi không thể thử lại** rồi chuyển dead-letter + cảnh báo. Lưu ý phân biệt *lỗi tạm thời* (broker sập, tuyệt đối không dead-letter) với *lỗi vĩnh viễn*.

**P7 — Bảng outbox phình to.** ✅
Dọn dẹp theo lô (project có; nhiều instance cùng chạy vẫn an toàn vì xoá là idempotent). Ở quy mô lớn: partition bảng theo ngày rồi drop partition, hoặc archive.

**P8 — Lệch đồng hồ giữa các instance.** 📖
`NextAttemptOnUtc` (và lease nếu dùng) so với giờ của instance khác nhau. Project dùng giờ ứng dụng cho đơn giản và chấp nhận sai lệch nhỏ; chặt hơn thì dùng giờ DB (`SYSUTCDATETIME()` / `now()`) cho mọi phép so sánh.

**P9 — Relay giữ "con trỏ" theo identity/sequence sẽ bỏ sót event.** 📖
Giao dịch T1 lấy id=10 nhưng commit chậm; T2 lấy id=11 commit trước. Relay thấy 11, nhớ "đã xử lý đến 11", và **không bao giờ thấy 10**. Project tránh bằng cờ trạng thái (`ProcessedOnUtc IS NULL`) thay vì con trỏ. CDC không dính vì đọc theo thứ tự commit trong log.

**P10 — Rolling deploy: hai phiên bản code cùng chạy.** 📖
Dòng outbox do v1 ghi có thể do relay v2 gửi (và ngược lại). Relay không deserialize payload nên không vỡ. Nhưng schema event phải tương thích (thêm trường tùy chọn, tăng version trong `Type` khi có thay đổi phá vỡ), và consumer phải bỏ qua type/trường lạ.

**P11 — Failover và replica.** 📖
Relay phải đọc/ghi trên **primary**; đọc outbox từ replica có độ trễ sẽ sai. Nếu failover làm mất vài transaction chưa kịp replicate, order và event mất *cùng nhau*: vẫn nhất quán (ưu điểm của chung transaction).

**P12 — Index và điểm nóng.** 📖
Khoá clustered là Guid ngẫu nhiên gây phân mảnh khi insert dày; cân nhắc khoá tăng dần theo thời gian (identity/`NEWSEQUENTIALID`, hoặc UUIDv7 trên PostgreSQL). Filtered index cho dòng chưa xử lý là bắt buộc.

**P13 — DB theo tenant.** 📖
Mỗi tenant một DB ⇒ relay phải duyệt nhiều DB; cần giới hạn số connection và công bằng giữa các tenant.

### 9.3 Phía consumer

**C1 — Competing consumers.** 📖
Mỗi message chỉ tới một instance ⇒ scale tốt, nhưng mất thứ tự giữa các message. Muốn giữ thứ tự theo key thì dùng partition/session/consumer-group theo `AggregateId`.

**C2 — Cùng một message tới hai instance khác nhau.** ✅
Do relay trùng, ack timeout, hay rebalance (Kafka). Việc khử trùng lặp **phải** nằm ở kho dùng chung và **trong cùng transaction** với tác dụng nghiệp vụ; cache trong RAM của từng instance **không đủ**. Khoá chính `(MessageId, Consumer)` chặn cả race đồng thời. Trong lần thử `kill -9`, các bản trùng được xử lý ở instance *khác* với instance đã áp dụng lần đầu và vẫn bị chặn đúng.

**C3 — Nhiều message cùng đụng một bản ghi, xử lý đồng thời ở nhiều instance.** ✅
Dùng cập nhật nguyên tử (`Available = Available - @q WHERE Available >= @q`, như project) hoặc optimistic concurrency (`rowversion`) + retry. Tránh kiểu "đọc về, tính trong code, ghi lại". Đã kiểm chứng: ba consumer cùng trừ một SKU qua 1000 order, kho về đúng 0, không âm, không thiếu.

**C4 — Đảo thứ tự.** 📖 Như P2: version/sequence, bỏ qua event cũ.

**C5 — Retention của inbox.** 📖
Inbox phải giữ lâu hơn cửa sổ giao lại tối đa (mục 4.4); nếu không, bản trùng đến muộn bị xử lý lại.

**C6 — Poison message và bão retry ở consumer.** 📖
Dùng DLQ, giới hạn số lần giao, backoff. Project mô phỏng "requeue sau 2 giây" vô hạn: chỉ phù hợp demo.

**C7 — Backpressure và autoscale.** 📖
Scale consumer theo độ dài hàng đợi/độ trễ; sau khi broker hồi phục sẽ có "đợt dồn" message ⇒ cần giới hạn tốc độ.

### 9.4 Phía broker và vận hành

- **Giữ thứ tự theo key:** dùng `AggregateId` làm routing/partition key.
- **Outage dài:** backlog tích trong bảng outbox; giám sát *tuổi của message cũ nhất* (`oldestPendingAgeSeconds`), *số dòng pending*, *Attempts lớn nhất*, *tỷ lệ lỗi publish*; đặt cảnh báo.
- **Dữ liệu nhạy cảm:** payload nằm ở DB và broker ⇒ áp dụng retention/mã hoá.

### 9.5 Bảng tổng hợp

| Case | Hiện tượng | Xử lý chính | Trong project |
|---|---|---|---|
| P1 | Nhiều relay publish trùng | Row-lock `SKIP LOCKED`/lease/leader + inbox | ✅ Có (UPDLOCK/READPAST) |
| P2 | Đảo thứ tự | Version, claim loại trừ, partition relay | ❌ Không bảo đảm thứ tự |
| P3 | Tải polling | Jitter, notify, ít relay, CDC | ◐ Chỉ có filtered index |
| P4 | Giữ khoá khi gọi mạng | Timeout, lô nhỏ, lease | ◐ Có timeout + dừng lô (chưa thử broker chậm thật) |
| P5 | Instance chết/deploy | Khoá tự nhả; tắt êm | ✅ Cả hai |
| P6 | Poison message | Backoff, dead-letter, cảnh báo | ◐ Backoff, chưa có dead-letter |
| P7 | Bảng phình | Dọn dẹp, partition | ✅ Cleanup |
| P8 | Lệch đồng hồ | Dùng giờ DB | ❌ Dùng giờ ứng dụng |
| P9 | Con trỏ identity | Cờ trạng thái / CDC | ✅ Dùng cờ trạng thái |
| C2 | Trùng xuyên instance | Inbox dùng chung, trong cùng transaction | ✅ Có |
| C3 | Nhiều message đụng cùng bản ghi | Cập nhật nguyên tử / rowversion | ✅ Cập nhật nguyên tử |

---

## 10. Kết quả chạy thử trong project này

Đo trên một máy phát triển (Windows 11, SQL Server LocalDB, .NET 10), mỗi kịch bản chạy bằng chính code trong repo. Số liệu cụ thể (đặc biệt phân bố việc giữa các instance và số bản trùng khi bỏ khoá) thay đổi theo từng lần chạy.

| # | Kịch bản | Kết quả đo được |
|---|---|---|
| 1 | Luồng cơ bản | `POST /orders` → outbox → relay → consumer; kho `SKU-001`: 1000 → 998 |
| 2 | Broker sập | 3 order vẫn 201; 3 dòng outbox pending (`attempts=2`, kho chưa đổi). Bật broker: **2,3 giây sau** giao hết, kho đúng (991) |
| 3 | Mất ack → bản trùng | Broker nhận cùng một message **4 lần**; consumer áp dụng **1 lần**, bỏ **3** bản trùng; kho trừ đúng 7 |
| 4a | Dual-write ngây thơ khi broker sập | HTTP 500, order **đã lưu**, outbox trống, event mất vĩnh viễn |
| 4b | Outbox khi broker sập | HTTP 201, event được giao bù khi broker hồi phục |
| 5 | 3 instance, cùng DB | 300 order → 300/300 publish, 0 trùng. 1000 order → 1000/1000, 0 trùng. Kho khớp tuyệt đối |
| 5b | **Đối chứng âm:** bỏ `UPDLOCK, READPAST, ROWLOCK` | 699 lần publish cho 300 message; **220 message bị gửi trùng**; kho vẫn đúng nhờ inbox |
| 6 | `kill -9` một instance giữa lô | Instance còn lại tiếp quản; 200/200 xử lý, kho 800; 10 message được publish lại, 10 bản trùng bị inbox chặn |
| 7 | Tắt êm giữa lô | Commit đúng 12 message đã gửi, nhả 88 còn lại; không trùng khi khởi động lại; không có exception |
| 8 | Dọn dẹp | Outbox 5 → 0, inbox 5 → 0 sau khi hết retention |
| 9 | Log SQL | Một transaction, một batch chứa cả `INSERT Orders` lẫn `INSERT OutboxMessages` |
| 10 | Biên | Request lỗi (400/404) không để lại dòng nào; SKU lạ xử lý đúng một lần; `/demo/*` trả 404 ngoài Development |

---

## 11. Chạy thử

Yêu cầu: .NET 10 SDK và SQL Server LocalDB (đi kèm Visual Studio). Database `OutboxPatternDemo` được **tự tạo** ở lần chạy đầu (`EnsureCreated`).

```powershell
cd "E:\Sơn\outbox pattern\outbox_pattern\outbox_pattern"
dotnet run --launch-profile http        # http://localhost:5100
```

Mở [outbox_pattern.http](../outbox_pattern.http) (Visual Studio hoặc VS Code với REST Client) và chạy lần lượt các kịch bản 1 → 4. Hoặc dùng **Swagger UI** tại `http://localhost:5100/swagger` (chỉ bật ở môi trường Development; tài liệu OpenAPI thô ở `/openapi/v1.json`). `GET /demo/state` cho thấy toàn bộ trạng thái: broker, outbox, inbox, tồn kho, các message đang kẹt. Log của app cho thấy luồng: `Broker accepted message …`, `Outbox relay: claimed N, published M`, `Order …: reserved …`, `Duplicate message … skipped`.

### Chạy nhiều instance

```powershell
dotnet build
# Terminal 1 — đợi dòng "Application started" (instance đầu tiên tạo database) rồi mới mở các terminal còn lại
$env:ASPNETCORE_ENVIRONMENT = "Development"; dotnet run --no-build --no-launch-profile --urls http://localhost:5100
# Terminal 2
$env:ASPNETCORE_ENVIRONMENT = "Development"; dotnet run --no-build --no-launch-profile --urls http://localhost:5101
# Terminal 3
$env:ASPNETCORE_ENVIRONMENT = "Development"; dotnet run --no-build --no-launch-profile --urls http://localhost:5102
```

Để thấy các relay tranh nhau thế nào: tắt broker trên cả ba (`PUT /demo/broker/down` tới từng cổng), tạo vài trăm order qua các cổng khác nhau, rồi bật lại broker trên cả ba cùng lúc (`PUT /demo/broker/up`). Mỗi instance sẽ in `Outbox relay: claimed N, published N`; mỗi `MessageId` chỉ xuất hiện **một lần** trong tổng log của cả ba. Thử `kill` một instance giữa chừng để xem instance khác tiếp quản.

> Mỗi process có broker giả lập riêng, nên message do instance nào publish thì consumer của chính instance đó nhận. Với broker thật dùng chung, điều này không còn đúng, đó cũng là lý do inbox phải nằm trong DB dùng chung.

### Chạy test tự động

Project [outbox_pattern.Tests](../../outbox_pattern.Tests) (xUnit) biến các kịch bản ở mục 10 thành test lặp lại được: POST /orders nguyên tử (kể cả khi `SaveChanges` lỗi giữa chừng), relay → consumer đúng một lần, broker sập rồi hồi phục, mất ack, nhiều relay đồng thời, hai consumer cùng một `MessageId`. Test dùng chính các class production (`OutboxProcessor`, `InventoryConsumer`, `InMemoryBroker`, `Program.cs`) trên LocalDB `(localdb)\MSSQLLocalDB` với một database tạm tên `OutboxPatternTests_<guid>`, tự tạo khi bắt đầu và `DROP` khi kết thúc; database `OutboxPatternDemo` không bị đụng tới.

```powershell
cd "E:\Sơn\outbox pattern\outbox_pattern"
dotnet test          # ~10 giây
```

---

## 12. Giới hạn của bản demo và checklist đưa lên production

### Giới hạn của bản demo

- **Broker giả lập trong bộ nhớ:** mất sạch khi process tắt; mỗi process một broker. Cần thay bằng broker thật (mục 8.6).
- **Không bảo đảm thứ tự** giữa các message/instance (P2).
- **Không có dead-letter** cho poison message (P6); consumer requeue vô hạn.
- **`EnsureCreated`** thay cho migrations, và có race nếu hai instance khởi động cùng lúc trên DB chưa tồn tại.
- **SQL claim viết riêng cho SQL Server** (`UPDLOCK, READPAST`); PostgreSQL cần `FOR UPDATE SKIP LOCKED`.
- Nghiệp vụ nằm thẳng trong controller; chưa có xác thực, idempotency key ở API.
- Test tự động (mục 11) mới phủ các kịch bản 1, 2, 3, 4b, 5, 9 và phần request lỗi của 10 trong mục 10; chưa có test cho 4a (dual-write ngây thơ), 5b (đối chứng bỏ khoá), 6 (`kill -9`), 7 (tắt êm), 8 (dọn dẹp) và các endpoint `/demo/*`.
- Giờ ứng dụng (không phải giờ DB) được dùng cho `NextAttemptOnUtc`.

### Checklist production

- [ ] Dùng **EF migrations**; không dùng `EnsureCreated`.
- [ ] Broker thật với **publisher confirms**; consumer chỉ ack sau khi commit.
- [ ] **Poison message:** giới hạn số lần cho lỗi vĩnh viễn → dead-letter + cảnh báo; không bao giờ dead-letter vì lỗi tạm thời.
- [ ] **Giám sát & cảnh báo:** số dòng pending, tuổi dòng cũ nhất, `Attempts` lớn nhất, tỷ lệ lỗi publish, tốc độ publish/consume.
- [ ] **Retention:** outbox giữ đủ để debug; inbox giữ **lâu hơn** cửa sổ redelivery tối đa.
- [ ] **Thứ tự:** quyết định rõ cần thứ tự theo aggregate không; nếu cần, chọn một trong các cách ở P2.
- [ ] **Index:** filtered index cho dòng chưa xử lý; khoá clustered phù hợp; kế hoạch partition/archive khi bảng lớn.
- [ ] **Idempotency key** cho API nhận request từ client.
- [ ] **Version hoá schema event** (hậu tố trong `Type`), tương thích ngược khi rolling deploy.
- [ ] **Timeout** cho publish; kích thước lô vừa phải; kiểm tra connection pool khi nhân bản.
- [ ] **Dữ liệu nhạy cảm** trong payload: chính sách retention/mã hoá/xoá.
- [ ] **Thử sự cố thật:** tắt broker, mất ack, `kill -9` relay/consumer, chạy ≥ 2 instance.
- [ ] Cân nhắc **thư viện** (MassTransit, NServiceBus, Wolverine, CAP…) hoặc **CDC** thay vì tự viết relay.

---

## 13. Đọc thêm

- Chris Richardson — [Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html), [Polling publisher](https://microservices.io/patterns/data/polling-publisher.html), [Transaction log tailing](https://microservices.io/patterns/data/transaction-log-tailing.html); sách *Microservices Patterns* (Manning, 2018).
- Gunnar Morling — [Reliable Microservices Data Exchange With the Outbox Pattern](https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/) và tài liệu [Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html) của Debezium.
- Gregor Hohpe — [Your Coffee Shop Doesn't Use Two-Phase Commit](https://www.enterpriseintegrationpatterns.com/ramblings/18_starbucks.html).
- Pat Helland — *Life beyond Distributed Transactions: an Apostate's Opinion* (CIDR 2007): nền tảng lý thuyết cho việc thay giao dịch phân tán bằng "thực thể + thông điệp + idempotence".
- Microsoft Learn — [Table hints (T-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table) (`UPDLOCK`, `READPAST`, `ROWLOCK`); PostgreSQL — [SELECT … FOR UPDATE SKIP LOCKED](https://www.postgresql.org/docs/current/sql-select.html).
