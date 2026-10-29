# ADR-0002: Two-Phase Lease Locking & Partitioned Idempotency Schema

## Status
Accepted

## Context
Trong hệ thống phân tán, không tồn tại "Exactly-Once Delivery" qua giao thức mạng HTTP. Nếu chỉ dùng `SET key NX EX 60` trên Redis làm khóa chống trùng, khi worker bị crash/OOM giữa chừng, các bản retry hợp lệ trong vòng 60 giây tiếp theo sẽ bị từ chối nhầm. Ngược lại, nếu gộp bảng kiểm tra trùng lặp vào chung bảng `webhook_events` (được partition theo `RANGE (created_at)`), PostgreSQL bắt buộc khóa chính phải kèm cột `created_at`, làm mất khả năng chặn trùng lặp toàn cục qua các ngày khác nhau.

## Decision
1. **Thiết kế khóa hai pha trên Redis:** Trạng thái `LOCKED:{worker_id}` (có Lease TTL ngắn 30s, tự giải phóng khi worker chết) tách biệt hoàn toàn với trạng thái `DONE` (TTL 24h).
2. **Tách riêng bảng `idempotency_keys`:** Bảng được partition theo `HASH (tenant_id)` để duy trì `PRIMARY KEY (tenant_id, platform, event_id)` chặt chẽ ở cấp độ Database, giữ kích thước nhỏ gọn để luôn nằm trong RAM (Postgres Buffer Pool).
3. **End-to-End Idempotency:** Truyền header `X-Idempotency-Key` xuống hệ thống ERP phía sau.

