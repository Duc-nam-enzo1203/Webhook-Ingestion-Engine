# ADR-0001: Bounded Durability Window via Redis Streams vs. Direct PostgreSQL Writes

## Status
Accepted

## Context
Cổng tiếp nhận webhook phải chịu được các đợt tăng vọt lưu lượng (Flash Sale spike) từ các sàn TMĐT và phản hồi trong vòng dưới 50ms (p99 < 15ms) để tránh bị sàn đánh dấu timeout. Nếu ghi trực tiếp vào PostgreSQL ở tầng Gateway, chi phí Disk I/O và tranh chấp khóa (Lock contention) trên B-Tree Index sẽ làm cạn kiệt Connection Pool khi lưu lượng vượt quá 1,000 RPS.

## Decision
Sử dụng **Redis Streams** với cấu hình `appendfsync everysec` làm bộ đệm tiếp nhận sơ cấp (Primary Ingestion Buffer), kết hợp cơ chế **Active Backpressure (HTTP 503 + Retry-After)** và **Safe `MINID` Trimming**.

## Trade-offs & Mitigations
* **Đánh đổi:** Không tuyên bố "Zero Data Loss" vô căn cứ. Với `appendfsync everysec`, nếu máy chủ vật lý chứa Redis bị mất điện đột ngột, tối đa 1 giây dữ liệu đã trả về `202 Accepted` có thể bị mất.
* **Biện pháp bù đắp rủi ro:**
  1. Tuyệt đối không dùng `XADD ... MAXLEN` cắt tự động khi hàng đợi đầy. Nếu Redis đạt 85% RAM hoặc backlog vượt ngưỡng, Gateway trả về `HTTP 503` để sàn TMĐT kích hoạt cơ chế retry của họ.
  2. Định kỳ chạy tiến trình **Reconciliation Poller** đối soát với API đơn hàng của sàn để kéo bù các đơn bị thiếu trong cửa sổ sự cố.

