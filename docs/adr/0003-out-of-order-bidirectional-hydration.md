# ADR-0003: Out-of-Order Event Resolution via State Weights & Bidirectional JSONB Merge

## Status
Accepted

## Context
Đồng hồ giữa các máy chủ của sàn TMĐT có thể bị lệch (Clock Skew), hoặc mạng định tuyến phân tán làm sự kiện cập nhật (`ORDER_CANCELLED` - chỉ chứa ID và lý do hủy) đến trước sự kiện gốc (`ORDER_CREATED` - chứa danh sách hàng hóa `line_items` và thông tin người mua).

## Decision
Không dùng timestamp đơn thuần để bỏ qua sự kiện cũ. Áp dụng máy trạng thái hữu hạn có trọng số (**Weighted Finite State Machine**) kết hợp phép gộp JSONB hai chiều (**Bidirectional JSONB Hydration**) ngay trong câu lệnh `INSERT ... ON CONFLICT DO UPDATE` nguyên tử của PostgreSQL.

Sự kiện có trọng số cao hơn luôn nằm ở **bên phải** toán tử `||` của PostgreSQL để giữ quyền ưu tiên trạng thái; sự kiện cũ hơn đến muộn nằm ở **bên trái** để chỉ bồi đắp các trường dữ liệu còn khuyết mà không làm lùi trạng thái đơn hàng.

