# ADR-0004: Multi-Tenant Isolation via Shuffle Sharding & ZSET Lease Semaphores

## Status
Accepted

## Context
Rate Limit tại cổng chỉ chặn được lưu lượng vượt hạn mức, nhưng không chặn được việc một merchant lớn có gói cước VIP đẩy hàng chục ngàn sự kiện vào hàng đợi khi Downstream ERP của chính họ đang bị nghẽn (phản hồi 3-5 giây/request). Nếu dùng chung 1 hàng đợi FIFO hoặc chia shard tĩnh đơn giản (`hash(tenant) % N`), các merchant nhỏ cùng shard sẽ bị tắc nghẽn (Head-of-Line Blocking).

## Decision
1. **Shuffle Sharding kết hợp Power of Two Choices:** Mỗi tenant được gán 2 shards cố định trong 16 shards; Gateway luôn chọn shard có độ dài hàng đợi (`XLEN`) thấp hơn tại thời điểm gửi. Giảm độ lệch tải tối đa từ $O(\frac{\log N}{\log \log N})$ xuống $O(\log \log N)$.
2. **ZSET Lease Semaphore:** Giới hạn số lượng worker tối đa mà 1 tenant được phép chiếm dụng đồng thời, tự động thu hồi slot bằng điểm số TTL nếu worker bị crash.
3. **Per-Tenant Circuit Breaker:** Khi ERP của một tenant bị sập (lỗi >50%), ngắt mạch và cô lập sự kiện của tenant đó, không làm nghẽn worker pool chung.

