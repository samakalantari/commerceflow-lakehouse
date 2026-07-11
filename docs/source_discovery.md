# Source Discovery & Data Contract

## Phase 1 — Source Discovery & Data Contract

The goal of this phase is to inspect the available Kafka sources before implementing the Bronze Layer. This document summarizes Kafka topics, topic-level statistics, field-level profiling, registered schemas, and initial data contract rules.

---

# 1. Inspect Kafka Topics

## 1.1 Kafka Topics Overview

| Topic Name | Category | Partitions | Replication Factor | Number of Messages | Size | Status |
|---|---|---:|---:|---:|---:|---|
| `__consumer_offsets` | internal | 50 | 1 | 2 | 658 Bytes | excluded |
| `_schemas` | internal | 1 | 1 | 9 | 7 KB | excluded |
| `behavioral.events` | behavioral | 3 | 1 | 3,452,624+ | 217 MB | included |
| `transactional.categories` | transactional | 3 | 1 | 117 | 12 KB | included |
| `transactional.order_items` | transactional | 3 | 1 | 138,337+ | 6 MB | included |
| `transactional.orders` | transactional | 3 | 1 | 85,199+ | 4 MB | included |
| `transactional.product_price_history` | transactional | 3 | 1 | 19,173+ | 2 MB | included |
| `transactional.products` | transactional | 3 | 1 | 5,000 | 648 KB | included |
| `transactional.returns_refunds` | transactional | 3 | 1 | 0 | 0 Bytes | included / empty |
| `transactional.users` | transactional | 3 | 1 | 36,335+ | 5 MB | included |

## 1.2 Topic Classification

| Topic Name | Source Type | Business Entity / Event |
|---|---|---|
| `behavioral.events` | behavioral | User clickstream and application events |
| `transactional.categories` | transactional | Product categories |
| `transactional.order_items` | transactional | Order line items |
| `transactional.orders` | transactional | Order headers |
| `transactional.product_price_history` | transactional | Product price changes |
| `transactional.products` | transactional | Product master data |
| `transactional.returns_refunds` | transactional | Returns and refunds |
| `transactional.users` | transactional | User master data |

## 1.3 Kafka UI Statistics

| Topic Name | Offset Min | Offset Max | Segment Size | Segment Count | Under Replicated Partitions | Bytes In / Sec | Bytes Out / Sec |
|---|---:|---:|---:|---:|---:|---:|---:|
| `behavioral.events` | 0 | 3,466,? | 227,827,261 | 3 | 0 | null | null |
| `transactional.categories` | 0 | 117 | 12,366 | 3 | 0 | null | null |
| `transactional.order_items` | 0 | 138,880 | 6,396,000 | 3 | 0 | null | null |
| `transactional.orders` | 0 | 85,483 | 4,703,576 | 3 | 0 | null | null |
| `transactional.product_price_history` | 0 | 19,205 | 2,231,181 | 3 | 0 | null | null |
| `transactional.products` | 0 | 5,000 | 663,327 | 3 | 0 | null | null |
| `transactional.returns_refunds` | 0 | 0 | 0 | 3 | 0 | null | null |
| `transactional.users` | 0 | 36,363 | 5,020,979 | 3 | 0 | null | null |

## 1.4 Field-Level Profiling — `behavioral.events`

### Observed Event Types

| event_type | Notes |
|---|---|
| `page_view` | Page or URL visit event. |
| `product_search` | Product search event. |
| `add_to_cart` | Product added to cart. |
| `wishlist_add` | Product added to wishlist. |

### Device Values

| device |
|---|
| `mobile` |
| `desktop` |
| `tablet` |

---

# 2. Source Fields and Registered Schemas

## 2.1 `behavioral.events`

### Fields

| Field | Avro Type | Required? | Nullable? | Seen in Samples? | Notes |
|---|---|---|---|---|---|
| `timestamp` | `string` | yes | no | yes | Event timestamp in string format. |
| `user_id` | `string` | yes | no | yes | User identifier. Kafka message key also appears to be user-based. |
| `event_type` | `string` | yes | no | yes | Example values: `page_view`, `product_search`, `add_to_cart`, `wishlist_add`. |
| `device` | `string` | yes | no | yes | Example values: `mobile`, `desktop`, `tablet`. |
| `session_id` | `string` | yes | no | yes | User session identifier. |
| `product_id` | `[null, string]` | no | yes | yes | Present for product-related events such as `add_to_cart` and `wishlist_add`. |
| `quantity` | `[null, int]` | no | yes | yes | Present for cart events. |
| `cart_total_items` | `[null, int]` | no | yes | yes | Present for cart events. |
| `cart_items` | `[null, array<cart_item>]` | no | yes | no | Expected for cart snapshot events. |
| `cart_items.product_id` | `string` | yes | no | no | Product identifier inside cart item. |
| `cart_items.price` | `double` | yes | no | no | Product price inside cart item. |
| `cart_items.quantity` | `int` | yes | no | no | Product quantity inside cart item. |
| `cart_value` | `[null, double]` | no | yes | no | Expected for cart value events. |
| `shipping_method` | `[null, string]` | no | yes | no | Expected for checkout or shipping-related events. |
| `order_id` | `[null, string]` | no | yes | no | Expected for order completion events. |
| `fulfillment_speed` | `[null, string]` | no | yes | no | Expected for fulfillment-related events. |
| `url_path` | `[null, string]` | no | yes | yes | Present for page view events. |
| `duration_sec` | `[null, int]` | no | yes | yes | Present for page view events. |
| `http_status` | `[null, int]` | no | yes | yes | Present for page view events. |
| `payment_type` | `[null, string]` | no | yes | no | Expected for payment-related events. |
| `success` | `[null, boolean]` | no | yes | no | Expected for payment or action result. |
| `error_code` | `[null, string]` | no | yes | no | Expected for failed payment or error events. |
| `query` | `[null, string]` | no | yes | yes | Present for product search events. |
| `results_count` | `[null, int]` | no | yes | yes | Present for product search events. |
| `clicked_position` | `[null, int]` | no | yes | yes | Present for product search events. |
| `rating` | `[null, int]` | no | yes | no | Expected for review events. |
| `text_length` | `[null, int]` | no | yes | no | Expected for review events. |
| `wishlist_name` | `[null, string]` | no | yes | yes | Present for wishlist events. |

## 2.2 `transactional.categories`

### Fields

| Field | Avro Type | Required? | Nullable? | Seen in Samples? | Notes |
|---|---|---|---|---|---|
| `category_id` | `string` | yes | no | yes | Category identifier. |
| `name` | `string` | yes | no | yes | Category name. |
| `parent_category_id` | `[null, string]` | no | yes | yes | Parent category identifier for hierarchical categories. |

## 2.3 `transactional.products`

### Fields

| Field | Avro Type | Required? | Nullable? | Seen in Samples? | Notes |
|---|---|---|---|---|---|
| `product_id` | `string` | yes | no | yes | Product identifier. |
| `name` | `string` | yes | no | yes | Product name. |
| `price` | `decimal(10,2)` | yes | no | yes | Product price. Encoded as Avro decimal bytes. |
| `category` | `[null, string]` | no | yes | yes | Product category. |
| `inventory` | `[null, int]` | no | yes | yes | Available product inventory. |
| `popularity_score` | `[null, decimal(4,2)]` | no | yes | yes | Product popularity score. Encoded as Avro decimal bytes. |

## 2.4 `transactional.users`

### Fields

| Field | Avro Type | Required? | Nullable? | Seen in Samples? | Notes |
|---|---|---|---|---|---|
| `user_id` | `string` | yes | no | yes | User identifier. |
| `username` | `string` | yes | no | yes | Username. |
| `email` | `string` | yes | no | yes | User email address. |
| `signup_date` | `date` | yes | no | yes | Avro logical date. |
| `device` | `[null, string]` | no | yes | yes | User default device. |
| `loyalty_tier` | `[null, string]` | no | yes | yes | User loyalty segment. |
| `location` | `[null, string]` | no | yes | yes | User location. |

## 2.5 `transactional.orders`

### Fields

| Field | Avro Type | Required? | Nullable? | Seen in Samples? | Notes |
|---|---|---|---|---|---|
| `order_id` | `string` | yes | no | yes | Order identifier. |
| `user_id` | `string` | yes | no | yes | User identifier. |
| `timestamp` | `timestamp-micros` | yes | no | yes | Order timestamp. Validate decoding during ingestion. |
| `total` | `decimal(10,2)` | yes | no | yes | Order amount. Encoded as Avro decimal bytes. |
| `status` | `string` | yes | no | yes | Order status. |
| `payment_method` | `[null, string]` | no | yes | yes | Payment method. |

## 2.6 `transactional.order_items`

### Fields

| Field | Avro Type | Required? | Nullable? | Seen in Samples? | Notes |
|---|---|---|---|---|---|
| `order_item_id` | `string` | yes | no | yes | Order item identifier. |
| `order_id` | `string` | yes | no | yes | Related order identifier. |
| `product_id` | `string` | yes | no | yes | Related product identifier. |
| `quantity` | `int` | yes | no | yes | Item quantity. |
| `unit_price` | `decimal(10,2)` | yes | no | yes | Unit price. Encoded as Avro decimal bytes. |
| `item_total_amount` | `decimal(10,2)` | yes | no | yes | Final item amount. Encoded as Avro decimal bytes. |

## 2.7 `transactional.product_price_history`

### Fields

| Field | Avro Type | Required? | Nullable? | Seen in Samples? | Notes |
|---|---|---|---|---|---|
| `price_history_id` | `string` | yes | no | yes | Price history record identifier. |
| `product_id` | `string` | yes | no | yes | Related product identifier. |
| `price` | `decimal(10,2)` | yes | no | yes | Product price. Encoded as Avro decimal bytes. |
| `valid_from` | `timestamp-micros` | yes | no | yes | Price validity start time. |
| `valid_to` | `[null, timestamp-micros]` | no | yes | yes | Price validity end time. Null means still valid. |
| `is_current` | `boolean` | yes | no | yes | Current price flag. |

## 2.8 `transactional.returns_refunds`

### Fields

| Field | Avro Type | Required? | Nullable? | Seen in Samples? | Notes |
|---|---|---|---|---|---|
| `return_refund_id` | not available | unknown | unknown | no | Topic is currently empty; registered schema was not available in the provided schema list. |
| `order_id` | not available | unknown | unknown | no | Expected relationship to an order. |
| `order_item_id` | not available | unknown | unknown | no | Expected relationship to an order item. |
| `return_timestamp` | not available | unknown | unknown | no | Expected return timestamp. |
| `refund_amount` | not available | unknown | unknown | no | Expected refund amount. |
| `return_reason` | not available | unknown | unknown | no | Expected return reason. |

---

# 3. Sales Funnel Event Roles

This section defines how behavioral `event_type` values map to the sales funnel. The funnel is used later for conversion analysis, drop-off analysis, and business dashboards.

| Funnel Step | event_type | Role in Funnel | Required Fields | Seen in Samples? | Notes |
|---:|---|---|---|---|---|
| 1 | `page_view` | User visits a page or product/category URL. This is the entry point of the funnel. | `timestamp`, `user_id`, `session_id`, `event_type`, `device`, `url_path` | yes | Observed with fields such as `url_path`, `duration_sec`, and `http_status`. |
| 2 | `product_search` | User searches for a product. This shows product discovery intent. | `timestamp`, `user_id`, `session_id`, `event_type`, `device`, `query` | yes | The project requirement mentions search; the observed event name is `product_search`. |
| 3 | `add_to_cart` | User adds a product to cart. This is a strong purchase-intent signal. | `timestamp`, `user_id`, `session_id`, `event_type`, `device`, `product_id`, `quantity` | yes | Observed with `product_id`, `quantity`, and `cart_total_items`. |
| 4 | `cart_view` | User views the current cart state. This can be used to analyze cart value and cart contents before checkout. | `timestamp`, `user_id`, `session_id`, `event_type`, `device`, `cart_items`, `cart_value` | no | Not observed in current samples, but supported by registered schema fields such as `cart_items` and `cart_value`. |
| 5 | `checkout_start` | User starts the checkout process. This marks transition from cart intent to checkout intent. | `timestamp`, `user_id`, `session_id`, `event_type`, `device` | no | Not observed in current samples. Related optional schema fields include `shipping_method` and `fulfillment_speed`. |
| 6 | `payment_attempt` | User attempts payment. This is used to monitor payment conversion and payment failures. | `timestamp`, `user_id`, `session_id`, `event_type`, `device`, `payment_type`, `success` | no | Not observed in current samples. Related optional schema fields include `payment_type`, `success`, and `error_code`. |
| 7 | `order_complete` | User completes the order. This is the final conversion event in the funnel. | `timestamp`, `user_id`, `session_id`, `event_type`, `device`, `order_id` | no | Not observed in current behavioral samples. It should be reconciled with `transactional.orders` using `order_id` and/or `user_id`. |

## 3.1 Funnel Interpretation

| Metric | Definition |
|---|---|
| Visit to Search Rate | Sessions with `product_search` / sessions with `page_view` |
| Search to Cart Rate | Sessions with `add_to_cart` / sessions with `product_search` |
| Cart to Checkout Rate | Sessions with `checkout_start` / sessions with `add_to_cart` |
| Checkout to Payment Rate | Sessions with `payment_attempt` / sessions with `checkout_start` |
| Payment to Order Rate | Sessions with `order_complete` / sessions with `payment_attempt` |
| Overall Conversion Rate | Sessions with `order_complete` / sessions with `page_view` |

---

# 4. Initial Data Contract Rules

This section defines the initial validation rules that must be applied when consuming source data into the Bronze and Silver layers.

## 4.1 Mandatory Field Rules

Mandatory fields are the fields marked as `Required? = yes` in the source field tables above. These fields must be present and non-null in every valid record.

| Source | Mandatory Fields | Action on Violation |
|---|---|---|
| `behavioral.events` | `timestamp`, `user_id`, `event_type`, `device`, `session_id` | Quarantine record. |
| `transactional.categories` | `category_id`, `name` | Quarantine record. |
| `transactional.products` | `product_id`, `name`, `price` | Quarantine record. |
| `transactional.users` | `user_id`, `username`, `email`, `signup_date` | Quarantine record. |
| `transactional.orders` | `order_id`, `user_id`, `timestamp`, `total`, `status` | Quarantine record. |
| `transactional.order_items` | `order_item_id`, `order_id`, `product_id`, `quantity`, `unit_price`, `item_total_amount` | Quarantine record. |
| `transactional.product_price_history` | `price_history_id`, `product_id`, `price`, `valid_from`, `is_current` | Quarantine record. |
| `transactional.returns_refunds` | not available yet | Revisit when schema or messages become available. |

## 4.2 Unique Key Rules

Each source must have a stable business key. If multiple messages exist for the same key, the ingestion process should either keep the latest version or preserve the change history depending on the target table design.

| Source | Unique Key | Rule |
|---|---|---|
| `behavioral.events` | no explicit `event_id`; use fallback composite key | Use a composite key such as `session_id`, `user_id`, `timestamp`, `event_type`, `product_id`, and Kafka metadata (`topic`, `partition`, `offset`) for traceability. |
| `transactional.categories` | `category_id` | One latest category record per `category_id`. |
| `transactional.products` | `product_id` | One latest product record per `product_id`. |
| `transactional.users` | `user_id` | One latest user record per `user_id`. |
| `transactional.orders` | `order_id` | One latest order record per `order_id`; status changes should keep the latest message by Kafka offset or event timestamp. |
| `transactional.order_items` | `order_item_id` | One latest order item record per `order_item_id`. |
| `transactional.product_price_history` | `price_history_id` | Each price history record must be unique. In addition, only one record per `product_id` should have `is_current = true`. |
| `transactional.returns_refunds` | not available yet | Revisit when schema or messages become available. |

## 4.3 Timestamp Rules

| Source | Timestamp Field | Rule | Action on Violation |
|---|---|---|---|
| `behavioral.events` | `timestamp` | Must be parseable as a valid timestamp string. | Quarantine record. |
| `transactional.orders` | `timestamp` | Must decode correctly from Avro `timestamp-micros`. | Quarantine record. |
| `transactional.product_price_history` | `valid_from`, `valid_to` | `valid_from` is required. If `valid_to` exists, it must be greater than or equal to `valid_from`. | Quarantine record. |
| `transactional.users` | `signup_date` | Must decode correctly from Avro logical `date`. | Quarantine record. |

Additional rule: timestamps that decode to suspicious defaults, such as `1970-01-01T00:00:00Z`, must be flagged for data quality review unless this value is explicitly expected by the source system.

## 4.4 Allowed Value Rules

| Field | Allowed Values | Action on Violation |
|---|---|---|
| `behavioral.events.event_type` | `page_view`, `product_search`, `add_to_cart`, `cart_view`, `checkout_start`, `payment_attempt`, `order_complete`, `wishlist_add`, `review_submit` | Quarantine or mark as `unknown_event_type`. |
| `behavioral.events.device` | `mobile`, `desktop`, `tablet` | Keep record and flag for review if a new value appears. |
| `transactional.orders.status` | `created`, `shipped`, `delivered`, `cancelled`, `refunded` | Quarantine record. |
| `transactional.orders.payment_method` | To be profiled from source data | Keep record and add new value to profiling report. |
| `transactional.users.loyalty_tier` | To be profiled from source data | Keep record and add new value to profiling report. |

## 4.5 Non-Negative Amount and Quantity Rules

| Source | Field | Rule | Action on Violation |
|---|---|---|---|
| `transactional.products` | `price` | Must be greater than or equal to zero. | Quarantine record. |
| `transactional.orders` | `total` | Must be greater than or equal to zero. | Quarantine record. |
| `transactional.order_items` | `quantity` | Must be greater than zero. | Quarantine record. |
| `transactional.order_items` | `unit_price` | Must be greater than or equal to zero. | Quarantine record. |
| `transactional.order_items` | `item_total_amount` | Must be greater than or equal to zero. | Quarantine record. |
| `transactional.product_price_history` | `price` | Must be greater than or equal to zero. | Quarantine record. |
| `transactional.returns_refunds` | `refund_amount` | Must be greater than or equal to zero when the topic becomes available. | Quarantine record. |

## 4.6 Event-Specific Behavioral Rules

| event_type | Required Context Fields | Rule |
|---|---|---|
| `page_view` | `url_path` | Page view events should include `url_path`; `duration_sec` and `http_status` are recommended when available. |
| `product_search` | `query` | Search events should include `query`; `results_count` and `clicked_position` are recommended when available. |
| `add_to_cart` | `product_id`, `quantity` | Cart add events must include product and quantity information. |
| `cart_view` | `cart_items`, `cart_value` | Cart view events should include cart snapshot information. |
| `checkout_start` | `session_id`, `user_id` | Checkout start must be traceable to a user session. |
| `payment_attempt` | `payment_type`, `success` | Payment attempt should include payment type and success/failure flag. |
| `order_complete` | `order_id` | Completed order events must include `order_id` for reconciliation with `transactional.orders`. |
| `wishlist_add` | `product_id` | Wishlist events must include product information. |

## 4.7 Invalid Record Handling

| Condition | Severity | Target Handling |
|---|---|---|
| Missing mandatory field | error | Quarantine. |
| Invalid timestamp | error | Quarantine. |
| Negative amount | error | Quarantine. |
| Invalid order status | error | Quarantine. |
| Unknown behavioral event type | error | Quarantine or mark as `unknown_event_type`. |
| New but valid-looking enum value | warning | Keep record and flag for profiling review. |
| Missing optional context field | warning | Keep record and flag for data quality monitoring. |
