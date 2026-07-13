# Source Discovery & Data Contract

## 1. Scope and Evidence Status

This document records the results of Kafka source discovery and defines the initial data contract for the Bronze and Silver layers. It covers the topic inventory, registered schemas, field-level observations, Kafka keys, timestamp semantics, topic settings, and the validation rules derived from them.

Discovery evidence was collected on **13 July 2026** from:

- Kafka UI inspection of topics, messages, and topic settings;
- Schema Registry inspection of subjects, versions, and compatibility;
- a bounded server-side connectivity test.

> A bounded Spark Structured Streaming smoke test confirmed connectivity to the remote Kafka cluster and successful consumption of Kafka metadata and binary values. This test did not validate Avro decoding, duplicate-key behavior, tombstones, CDC semantics, or business timestamp correctness.

### 1.1 Evidence Categories

Every important statement in this document belongs to one of the following categories:

| Category | Meaning |
|---|---|
| **Observed** | Directly seen in inspected Kafka messages or Kafka UI. |
| **Schema-confirmed** | Present in the registered Avro schema, but not necessarily observed in inspected messages. |
| **Expected** | Derived from the project brief or intended business workflow; not observed. |
| **Provisional** | A proposed downstream rule that depends on unverified source behavior. |
| **Unconfirmed** | Insufficient evidence exists. |

Message-level inspection was sample-based. Statements scoped to "the inspected sample" describe the messages displayed during discovery, not the full topic contents. Full-topic profiling has not been performed.

## 2. Kafka Source Inventory

### 2.1 Business Topics

| Topic | Source Type | Business Entity / Event |
|---|---|---|
| `behavioral.events` | behavioral | User clickstream and application events |
| `transactional.categories` | transactional | Product categories |
| `transactional.products` | transactional | Product master data |
| `transactional.users` | transactional | User master data |
| `transactional.orders` | transactional | Order headers |
| `transactional.order_items` | transactional | Order line items |
| `transactional.product_price_history` | transactional | Product price changes |
| `transactional.returns_refunds` | transactional | Returns and refunds (empty at capture time) |

### 2.2 Internal Topics

The following Kafka-internal topics exist on the cluster. They are excluded from the source-contract scope and must not be ingested as business data.

| Topic | Partitions | Replication Factor | Role |
|---|---:|---:|---|
| `__consumer_offsets` | 50 | 1 | Kafka consumer-offset storage |
| `_schemas` | 1 | 1 | Schema Registry backing storage |

### 2.3 Point-in-Time Kafka Snapshot

Business-topic snapshot captured on **13 July 2026**:

| Topic | Partitions | Replication Factor | Under-Replicated Partitions | Messages | Size |
|---|---:|---:|---:|---:|---:|
| `behavioral.events` | 3 | 1 | 0 | 2,407,714 | 122 MB |
| `transactional.categories` | 3 | 1 | 0 | 117 | 12 KB |
| `transactional.order_items` | 3 | 1 | 0 | 96,739 | 3 MB |
| `transactional.orders` | 3 | 1 | 0 | 59,781 | 3 MB |
| `transactional.product_price_history` | 3 | 1 | 0 | 7,616 | 864 KB |
| `transactional.products` | 3 | 1 | 0 | 5,301 | 687 KB |
| `transactional.returns_refunds` | 3 | 1 | 0 | 0 | 0 Bytes |
| `transactional.users` | 3 | 1 | 0 | 49,449 | 7 MB |

> Message counts, offsets, and topic sizes are point-in-time observations. They may increase or decrease while producers run and Kafka retention removes old segments.

### 2.4 Topic Retention and Cleanup

Observed topic settings:

| Topic group | `cleanup.policy` | `retention.ms` | Retention | `message.timestamp.type` |
|---|---|---:|---:|---|
| `behavioral.events` | `delete` | 172800000 | 2 days | `CreateTime` |
| All `transactional.*` topics | `delete` | 604800000 | 7 days | `CreateTime` |

The transactional group covers `transactional.categories`, `transactional.products`, `transactional.users`, `transactional.orders`, `transactional.order_items`, `transactional.product_price_history`, and `transactional.returns_refunds`.

Architectural consequences:

- Kafka is temporary source storage, not permanent history.
- All topics are delete-retained, not compacted. Kafka does not keep one latest record per business key.
- The currently retained history may not contain the complete business lifecycle of any entity.
- Bronze ingestion must preserve records before Kafka retention removes them; records removed by retention cannot be recovered from Kafka.
- The retention windows are not safe downtime allowances. Records may already be partway through their retention age when ingestion stops, so the operational recovery objective must be significantly shorter than the retention window, with monitoring and alerting before ingestion lag approaches it.
- The topic settings alone do not indicate whether repeated business keys represent updates, retries, or duplicates. `cleanup.policy = delete` does not prove append-only behavior.

### 2.5 Schema Registry Summary

Global compatibility level: **`BACKWARD`**.

| Subject | Schema ID | Type | Version | Compatibility |
|---|---:|---|---:|---|
| `behavioral.events-value` | 5 | AVRO | 1 | BACKWARD |
| `transactional.categories-value` | 1 | AVRO | 1 | BACKWARD |
| `transactional.products-value` | 2 | AVRO | 1 | BACKWARD |
| `transactional.users-value` | 3 | AVRO | 1 | BACKWARD |
| `transactional.orders-value` | 4 | AVRO | 1 | BACKWARD |
| `transactional.order_items-value` | 6 | AVRO | 1 | BACKWARD |
| `transactional.product_price_history-value` | 7 | AVRO | 1 | BACKWARD |

> `transactional.returns_refunds-value` is **not registered**. The topic is empty; its value format, key contract, field types, nullability, timestamps, and mutation behavior remain unverified (see §3.8).

## 3. Topic-Level Source Contracts

Field tables reflect the registered Avro value schemas. The **Observed in Sample?** column records whether the field carried a value in the messages inspected during discovery. A value of "no" means schema-confirmed but not observed; it does not mean the field is absent from the schema.

### 3.1 `behavioral.events`

#### Fields

| Field | Avro Type | Required? | Nullable? | Observed in Sample? | Notes |
|---|---|---|---|---|---|
| `timestamp` | `string` | yes | no | yes | Event timestamp string. Observed format `yyyy-MM-dd'T'HH:mm:ss`; contains no timezone or UTC offset. |
| `user_id` | `string` | yes | no | yes | User identifier. The Kafka message key matched `user_id` in the inspected sample. |
| `event_type` | `string` | yes | no | yes | Observed values: `page_view`, `product_search`, `add_to_cart`, `cart_view`, `wishlist_add`. |
| `device` | `string` | yes | no | yes | Observed values: `mobile`, `desktop`, `tablet`. |
| `session_id` | `string` | yes | no | yes | User session identifier. |
| `product_id` | `[null, string]` | no | yes | yes | Present for product-related events such as `add_to_cart` and `wishlist_add`. |
| `quantity` | `[null, int]` | no | yes | yes | Present for cart events. |
| `cart_total_items` | `[null, int]` | no | yes | yes | Present for cart events. |
| `cart_items` | `[null, array<cart_item>]` | no | yes | yes | Observed in `cart_view` events. |
| `cart_items.product_id` | `string` | yes | no | yes | Product identifier inside a cart item. Observed in `cart_view` events. |
| `cart_items.price` | `double` | yes | no | yes | Product price inside a cart item. Observed in `cart_view` events. |
| `cart_items.quantity` | `int` | yes | no | yes | Product quantity inside a cart item. Observed in `cart_view` events. |
| `cart_value` | `[null, double]` | no | yes | yes | Observed in `cart_view` events. Some observed records carried `0.0` despite non-empty `cart_items`; see the data-quality observation below. |
| `shipping_method` | `[null, string]` | no | yes | no | Schema-confirmed; expected for checkout or shipping events; not observed in the inspected sample. |
| `order_id` | `[null, string]` | no | yes | no | Schema-confirmed; expected for order-completion events; not observed in the inspected sample. |
| `fulfillment_speed` | `[null, string]` | no | yes | no | Schema-confirmed; expected for fulfillment events; not observed in the inspected sample. |
| `url_path` | `[null, string]` | no | yes | yes | Present for page-view events. |
| `duration_sec` | `[null, int]` | no | yes | yes | Present for page-view events. |
| `http_status` | `[null, int]` | no | yes | yes | Present for page-view events. |
| `payment_type` | `[null, string]` | no | yes | no | Schema-confirmed; expected for payment events; not observed in the inspected sample. |
| `success` | `[null, boolean]` | no | yes | no | Schema-confirmed; expected for payment or action results; not observed in the inspected sample. |
| `error_code` | `[null, string]` | no | yes | no | Schema-confirmed; expected for failed-payment or error events; not observed in the inspected sample. |
| `query` | `[null, string]` | no | yes | yes | Present for product-search events. |
| `results_count` | `[null, int]` | no | yes | yes | Present for product-search events. |
| `clicked_position` | `[null, int]` | no | yes | yes | Present for product-search events. |
| `rating` | `[null, int]` | no | yes | no | Schema-confirmed; expected for review events; not observed in the inspected sample. |
| `text_length` | `[null, int]` | no | yes | no | Schema-confirmed; expected for review events; not observed in the inspected sample. |
| `wishlist_name` | `[null, string]` | no | yes | yes | Present for wishlist events. |

#### Source Metadata

| Property | Observation |
|---|---|
| Kafka key | In the inspected sample, the Kafka key matched `user_id`. The key is not a unique event identifier. |
| Value encoding | Confluent-framed Avro using registered subject `behavioral.events-value` (schema ID 5). |
| Business timestamp | Payload `timestamp` string; observed format `yyyy-MM-dd'T'HH:mm:ss` with no timezone or UTC offset. Timezone semantics must be confirmed by the source owner. |
| Kafka timestamp | Available; the topic uses `CreateTime`. Preserved separately in Bronze. |
| Record identity | No source `event_id` was observed. The technical Bronze identity is `topic + partition + offset`. |
| Observed source behavior | Observed event types and field coverage are recorded in the field table. The `cart_value = 0.0` inconsistency is described under Observations. |
| Known uncertainty | Duplicate-event, retry, and replay behavior remain unconfirmed. Payload timestamp timezone remains unconfirmed. |

#### Observations

- Expected from the project requirements or supported by the schema, but not observed: `checkout_start`, `payment_attempt`, `order_complete`, `review_submit`.
- Data-quality observation: some `cart_view` records contained non-empty `cart_items` with positive item prices while `cart_value` was `0.0`. These records must not be automatically quarantined. The inconsistency is a source-quality observation that requires profiling and source-owner clarification.
- Unknown but structurally valid event types must be preserved in Bronze and flagged in Silver. A new event type is not discarded merely because it is absent from the current known list.

### 3.2 `transactional.categories`

#### Fields

| Field | Avro Type | Required? | Nullable? | Observed in Sample? | Notes |
|---|---|---|---|---|---|
| `category_id` | `string` | yes | no | yes | Category identifier. |
| `name` | `string` | yes | no | yes | Category name. |
| `parent_category_id` | `[null, string]` | no | yes | yes | Null for root categories; contains another category identifier for child categories. |

#### Source Metadata

| Property | Observation |
|---|---|
| Kafka key | In the inspected sample, the Kafka key matched `category_id` (example: key `C2`, payload `category_id = C2`). |
| Value encoding | Confluent-framed Avro using registered subject `transactional.categories-value` (schema ID 1). |
| Business timestamp | None present in the payload. |
| Kafka timestamp | Available; the topic uses `CreateTime`. Preserved in Bronze. |
| Observed source behavior | 117 messages were inspected, matching the point-in-time message count of the topic at capture. Root categories carried `parent_category_id = null`; child categories referenced another category identifier. |
| Known uncertainty | Mutation semantics remain unconfirmed; the keep-latest rule is provisional (§5.5). |

#### Observations

- Hierarchy validation (`parent_category_id` referencing an existing `category_id`) belongs to Silver; missing parents are flagged, not quarantined (§5.8).

### 3.3 `transactional.products`

#### Fields

| Field | Avro Type | Required? | Nullable? | Observed in Sample? | Notes |
|---|---|---|---|---|---|
| `product_id` | `string` | yes | no | yes | Product identifier. |
| `name` | `string` | yes | no | yes | Product name. |
| `price` | `decimal(10,2)` | yes | no | yes | Product price. Encoded as Avro decimal bytes. |
| `category` | `[null, string]` | no | yes | no | Schema-confirmed; not observed in the inspected sample. Whether it carries a category ID or a category name remains unconfirmed. |
| `inventory` | `[null, int]` | no | yes | no | Schema-confirmed; not observed in the inspected sample. When present, it must be non-negative. |
| `popularity_score` | `[null, decimal(4,2)]` | no | yes | no | Schema-confirmed; not observed in the inspected sample. Encoded as Avro decimal bytes. |

#### Source Metadata

| Property | Observation |
|---|---|
| Kafka key | In the inspected sample, the Kafka key matched `product_id` (example: key `P1001`, payload `product_id = P1001`). |
| Value encoding | Confluent-framed Avro using registered subject `transactional.products-value` (schema ID 2). |
| Business timestamp | None present in the payload. |
| Kafka timestamp | Available; the topic uses `CreateTime`. Preserved in Bronze. |
| Observed source behavior | Approximately 1,500 messages were loaded during inspection. `product_id`, `name`, and `price` carried values in every displayed record. |
| Known uncertainty | `category` semantics (ID versus name) remain unconfirmed; no join to `transactional.categories` is defined until this is resolved. Mutation semantics remain unconfirmed; the keep-latest rule is provisional (§5.5). |

#### Observations

- Values displayed by Kafka UI's Produce Message form are generated examples. They are not sample evidence and were not used in this document.

### 3.4 `transactional.users`

#### Fields

| Field | Avro Type | Required? | Nullable? | Observed in Sample? | Notes |
|---|---|---|---|---|---|
| `user_id` | `string` | yes | no | yes | User identifier. |
| `username` | `string` | yes | no | yes | Username. |
| `email` | `string` | yes | no | yes | User email address. Personal data; see §5.9 handling note. |
| `signup_date` | `date` | yes | no | yes | Avro logical date. Observed format `yyyy-MM-dd`. A logical date, not an event timestamp; timezone does not apply. |
| `device` | `[null, string]` | no | yes | no | Schema-confirmed; not observed in the inspected sample. |
| `loyalty_tier` | `[null, string]` | no | yes | yes | Observed values: `Bronze`, `Silver`, `Gold`, `Platinum`. |
| `location` | `[null, string]` | no | yes | yes | Single mixed-format string; observed forms conceptually equivalent to `city, province` and `city, country`. Not verified as separate city and country fields. |

#### Source Metadata

| Property | Observation |
|---|---|
| Kafka key | In the inspected sample, the Kafka key matched `user_id` (example: key `U1001`, payload `user_id = U1001`). |
| Value encoding | Confluent-framed Avro using registered subject `transactional.users-value` (schema ID 3). |
| Business timestamp | None. `signup_date` is a signup attribute, not a record-update timestamp. |
| Kafka timestamp | Available; the topic uses `CreateTime`. Preserved in Bronze. |
| Observed source behavior | `user_id`, `username`, `email`, `signup_date`, `loyalty_tier`, and `location` carried values in the inspected sample. |
| Known uncertainty | Mutation semantics remain unconfirmed; the keep-latest rule is provisional (§5.5). `device` values are unknown. |

#### Observations

- `email` is personal data. Mask or restrict it in Silver and Gold outputs wherever the full value is unnecessary.

### 3.5 `transactional.orders`

#### Fields

| Field | Avro Type | Required? | Nullable? | Observed in Sample? | Notes |
|---|---|---|---|---|---|
| `order_id` | `string` | yes | no | yes | Order identifier. |
| `user_id` | `string` | yes | no | yes | User identifier. |
| `timestamp` | `timestamp-micros` | yes | no | yes | Every inspected payload value decoded to `1970-01-01T00:00:00Z` (epoch zero); see Source Metadata. |
| `total` | `decimal(10,2)` | yes | no | yes | Order amount. Encoded as Avro decimal bytes. |
| `status` | `string` | yes | no | yes | Observed value: `created` only. |
| `payment_method` | `[null, string]` | no | yes | yes | Observed values listed in §5.6. |

#### Source Metadata

| Property | Observation |
|---|---|
| Kafka key | In the inspected sample, the Kafka key matched `order_id` (example: key `O419575`, payload `order_id = O419575`). |
| Value encoding | Confluent-framed Avro using registered subject `transactional.orders-value` (schema ID 4). |
| Business timestamp | Payload `timestamp` (`timestamp-micros`). All inspected values were `1970-01-01T00:00:00Z`. This is a serious source-data-quality issue; the field is currently unusable as real order time. |
| Kafka timestamp | Available; the topic uses `CreateTime`. Preserved separately in Bronze. May be used temporarily for Bronze partitioning or fallback event-time handling only with recorded provenance. |
| Observed source behavior | Observed `status`: `created` only. No CDC envelope was observed. |
| Known uncertainty | Mutation semantics, including retries, remain unconfirmed; the keep-latest rule is provisional (§5.5). |

#### Observations

- Handling rules for the epoch-zero timestamp are defined in §5.4 (preserve and flag) and §5.5 (no latest-record selection on this field).
- Statuses other than `created` (`shipped`, `delivered`, `cancelled`, `refunded`) are expected from the project requirements but were not observed.

### 3.6 `transactional.order_items`

#### Fields

| Field | Avro Type | Required? | Nullable? | Observed in Sample? | Notes |
|---|---|---|---|---|---|
| `order_item_id` | `string` | yes | no | yes | Order item identifier. |
| `order_id` | `string` | yes | no | yes | Related order identifier. |
| `product_id` | `string` | yes | no | yes | Related product identifier. |
| `quantity` | `int` | yes | no | yes | Item quantity. Mostly `1` in the inspected sample. |
| `unit_price` | `decimal(10,2)` | yes | no | yes | Unit price. Encoded as Avro decimal bytes. |
| `item_total_amount` | `decimal(10,2)` | yes | no | yes | Final item amount. Encoded as Avro decimal bytes. See Source Metadata for the observed amount relationship. |

#### Source Metadata

| Property | Observation |
|---|---|
| Kafka key | In the inspected sample, the Kafka key matched `order_item_id` (example: `OI680136`). |
| Value encoding | Confluent-framed Avro using registered subject `transactional.order_items-value` (schema ID 6). |
| Business timestamp | None present in the payload. |
| Kafka timestamp | Available; the topic uses `CreateTime`. Preserved in Bronze. |
| Observed source behavior | `item_total_amount` was sometimes equal to `unit_price` and often lower than `quantity × unit_price`. Observed example: `quantity = 1`, `unit_price = 263.45`, `item_total_amount = 255.55`, difference `7.90`. |
| Known uncertainty | The meaning of the amount difference is unconfirmed; it may represent a discount or another adjustment, and a positive difference must not be assumed to be a discount. Mutation semantics remain unconfirmed; the keep-latest rule is provisional (§5.5). |

#### Observations

- Amount-relationship validation and the optional `derived_discount` field are defined in §5.7.
- Unresolved references to `transactional.orders` and `transactional.products` are flagged, not quarantined (§5.8).

### 3.7 `transactional.product_price_history`

#### Fields

| Field | Avro Type | Required? | Nullable? | Observed in Sample? | Notes |
|---|---|---|---|---|---|
| `price_history_id` | `string` | yes | no | yes | Price history record identifier. |
| `product_id` | `string` | yes | no | yes | Related product identifier. |
| `price` | `decimal(10,2)` | yes | no | yes | Product price. Encoded as Avro decimal bytes. |
| `valid_from` | `timestamp-micros` | yes | no | yes | Business effective timestamp for the price. This is the topic's business timestamp. |
| `valid_to` | `[null, timestamp-micros]` | no | yes | null only | Optional validity end boundary; not the main event timestamp. No non-null value was observed in the inspected sample. |
| `is_current` | `boolean` | yes | no | yes | Current-price flag. In the inspected sample, at least one product had more than one `is_current = true` record; see Observations. |

#### Source Metadata

| Property | Observation |
|---|---|
| Kafka key | In the inspected sample, the Kafka key matched `price_history_id`. |
| Value encoding | Confluent-framed Avro using registered subject `transactional.product_price_history-value` (schema ID 7). |
| Business timestamp | `valid_from`. Kafka record timestamp is preserved separately. |
| Kafka timestamp | Available; the topic uses `CreateTime`. Preserved in Bronze. |
| Observed source behavior | The inspected sample contained many `is_current = true` records. At least one product had more than one record with `is_current = true`. No CDC envelope or delete event was observed. |
| Known uncertainty | Mutation semantics remain unconfirmed (§5.5). |

#### Observations

- "Only one current record per product" is a Silver data-quality expectation that is already violated in the observed source data. It is not a guaranteed source invariant and must not be documented as one. Violations are flagged, not silently repaired.

### 3.8 `transactional.returns_refunds`

#### Verified Topic Facts

| Property | Observation |
|---|---|
| Topic exists | Yes. |
| Partitions | 3 |
| Replication factor | 1 |
| Message count | 0 |
| Size | 0 Bytes |
| `cleanup.policy` | `delete` |
| Retention | 7 days (`retention.ms = 604800000`) |
| `message.timestamp.type` | `CreateTime` |
| Registered value schema | None. `transactional.returns_refunds-value` is not registered. |
| Representative messages | None exist. |

#### Expected Fields

The project requirements expect `return_refund_id`, `order_id`, `order_item_id`, `return_timestamp`, `refund_amount`, and `return_reason`. All of these are **Expected from project requirements — not source-verified**: none can be assigned Avro types, nullability, keys, timestamp semantics, or validation rules.

#### Source Metadata

| Property | Observation |
|---|---|
| Kafka key | Unconfirmed. |
| Value encoding | Unconfirmed. Kafka UI displays `Value Serde = String` in its default producer form; this is a form default and does not establish the real source format. |
| Business timestamp | Unconfirmed. |
| Kafka timestamp | Available; the topic uses `CreateTime`. |
| Observed source behavior | None. The topic contained zero messages at capture time; no source behavior could be observed. |
| Known uncertainty | The entire message contract is unverified. The registered schema (or producer contract) and representative messages must be requested from the source owner. |

## 4. Behavioral Event and Funnel Discovery

Behavioral `event_type` values map to the sales funnel used later for conversion analysis, drop-off analysis, and business dashboards. All behavioral events carry the mandatory fields `timestamp`, `user_id`, `session_id`, `event_type`, and `device`.

| Step | `event_type` | Funnel role | Event-specific context fields | Evidence status |
|---:|---|---|---|---|
| 1 | `page_view` | Entry point: user visits a page or product/category URL. | `url_path`; `duration_sec` and `http_status` when available | Observed in the inspected sample. |
| 2 | `product_search` | Product discovery intent. | `query`; `results_count` and `clicked_position` when available | Observed in the inspected sample. |
| 3 | `add_to_cart` | Strong purchase-intent signal. | `product_id`, `quantity`, `cart_total_items` | Observed in the inspected sample. |
| 4 | `cart_view` | Cart state before checkout; source for cart-value and cart-content analysis. | `cart_items`, `cart_value` | Observed in the inspected sample. Some records carried `cart_value = 0.0` with non-empty `cart_items` (see §3.1). |
| 5 | `checkout_start` | Transition from cart intent to checkout intent. | Schema-confirmed related fields: `shipping_method`, `fulfillment_speed` | Expected; not observed. |
| 6 | `payment_attempt` | Payment conversion and failure monitoring. | Schema-confirmed related fields: `payment_type`, `success`, `error_code` | Expected; not observed. |
| 7 | `order_complete` | Final conversion event. | Schema-confirmed related field: `order_id` | Expected; not observed. Reconcile with `transactional.orders` on `order_id` once observed. |

Non-funnel event types:

| `event_type` | Evidence status | Context fields |
|---|---|---|
| `wishlist_add` | Observed in the inspected sample. | `product_id`, `wishlist_name` |
| `review_submit` | Expected; not observed. | Schema-confirmed related fields: `rating`, `text_length` |

### 4.1 Funnel Interpretation

| Metric | Definition |
|---|---|
| Visit to Search Rate | Sessions with `product_search` / sessions with `page_view` |
| Search to Cart Rate | Sessions with `add_to_cart` / sessions with `product_search` |
| Cart to Checkout Rate | Sessions with `checkout_start` / sessions with `add_to_cart` |
| Checkout to Payment Rate | Sessions with `payment_attempt` / sessions with `checkout_start` |
| Payment to Order Rate | Sessions with `order_complete` / sessions with `payment_attempt` |
| Overall Conversion Rate | Sessions with `order_complete` / sessions with `page_view` |

Metrics that depend on `checkout_start`, `payment_attempt`, or `order_complete` cannot be computed from observed data yet, because those event types have not been observed.

## 5. Initial Data Contract

Bronze preserves the source faithfully — it is not a business-rule quarantine layer. Silver validates business semantics. Rules that depend on unverified source behavior are marked provisional and are not treated as implemented facts.

### 5.1 Bronze Responsibilities

Bronze must:

- preserve the Kafka key, topic, partition, offset, and Kafka timestamp;
- preserve `raw_value` as binary, and record the ingestion time;
- preserve the schema identifier where available;
- decode Avro into decoded columns without destroying the raw bytes;
- create timestamp-derived fields with explicit provenance (§5.4);
- preserve decode failures together with their error information;
- retain records even when downstream semantic validation fails.

Recommended conceptual Bronze metadata:

```text
kafka_key
kafka_topic
kafka_partition
kafka_offset
kafka_timestamp
ingested_at
raw_value
schema_id
decode_status
decode_error
event_timestamp
event_date
event_time_source
timestamp_status
```

### 5.2 Silver Responsibilities

Silver must:

- validate required business fields;
- validate logical types and value ranges;
- normalize timestamps and flag epoch-zero values;
- normalize or profile enum values;
- apply event-specific validation for behavioral events;
- detect duplicates where identity semantics are known (currently limited to the technical identity, since business mutation semantics are unconfirmed);
- check relationships and handle late-arriving references;
- calculate clearly labeled derived fields;
- flag unknown event types;
- quarantine technically or semantically unusable records;
- preserve uncertain but potentially valid records for review.

### 5.3 Mandatory Field Rules

Mandatory fields are schema-required, non-nullable fields. Bronze retains the raw record in all cases; quarantine applies to the Silver representation.

| Source | Mandatory fields | Silver action on violation |
|---|---|---|
| `behavioral.events` | `timestamp`, `user_id`, `event_type`, `device`, `session_id` | Quarantine. |
| `transactional.categories` | `category_id`, `name` | Quarantine. |
| `transactional.products` | `product_id`, `name`, `price` | Quarantine. |
| `transactional.users` | `user_id`, `username`, `email`, `signup_date` | Quarantine. |
| `transactional.orders` | `order_id`, `user_id`, `timestamp`, `total`, `status` | Quarantine. |
| `transactional.order_items` | `order_item_id`, `order_id`, `product_id`, `quantity`, `unit_price`, `item_total_amount` | Quarantine. |
| `transactional.product_price_history` | `price_history_id`, `product_id`, `price`, `valid_from`, `is_current` | Quarantine. |
| `transactional.returns_refunds` | Unconfirmed. | Revisit when the schema and messages become available. |

### 5.4 Timestamp Rules

Three timestamps are distinguished and never conflated:

| Timestamp | Meaning |
|---|---|
| Business timestamp | Time represented by the business payload. |
| Kafka timestamp | Kafka record `CreateTime`. |
| `ingested_at` | Time the Bronze consumer processed the record. |

Fallback order for `event_timestamp`, always with explicit provenance:

```text
valid business timestamp
→ otherwise Kafka timestamp
→ otherwise ingested_at
```

Provenance is recorded in `event_time_source` (values: `payload_timestamp`, `valid_from`, `kafka_timestamp`, `ingested_at`) and `timestamp_status`. The Kafka timestamp is never presented as the business time.

Topic-level treatment:

| Topic | Timestamp treatment |
|---|---|
| `behavioral.events` | Use the payload `timestamp` when parseable; timezone remains unconfirmed. |
| `transactional.orders` | Flag epoch-zero payload values; see the validation rules below. |
| `transactional.product_price_history` | Use `valid_from`. |
| `transactional.categories` | No business timestamp; use the Kafka timestamp as fallback with provenance. |
| `transactional.products` | No business timestamp; use the Kafka timestamp as fallback with provenance. |
| `transactional.order_items` | No business timestamp; use the Kafka timestamp as fallback with provenance. |
| `transactional.users` | Keep `signup_date` as a date attribute; use the Kafka timestamp for record arrival and partitioning when required. |
| `transactional.returns_refunds` | Unconfirmed. |

Validation rules:

| Source | Field | Rule | Handling |
|---|---|---|---|
| `behavioral.events` | `timestamp` | Parseable as `yyyy-MM-dd'T'HH:mm:ss`. | If unparseable, fall back to the Kafka timestamp with `event_time_source = kafka_timestamp` and a `timestamp_status` flag. |
| `transactional.orders` | `timestamp` | Decodes from `timestamp-micros`; epoch-zero values are invalid business time. | Preserve the original value, flag epoch-zero in Silver, never silently overwrite; use the Kafka timestamp only as a documented fallback. |
| `transactional.product_price_history` | `valid_from`, `valid_to` | `valid_from` is required. When `valid_to` is present, `valid_to >= valid_from`. | Violations are flagged for data-quality review. |
| `transactional.users` | `signup_date` | Decodes from Avro logical `date`. | Decode failures surface in Bronze `decode_status`; Silver quarantines undecodable records. |

### 5.5 Key and Identity Rules

Two distinct concepts apply:

1. **Kafka message key** — the partitioning key set by the producer.
2. **Record identity** — how a record is uniquely identified downstream.

The authoritative technical identity in Bronze is:

```text
kafka_topic + kafka_partition + kafka_offset
```

Observed Kafka keys (in the inspected samples):

| Topic | Observed Kafka key |
|---|---|
| `behavioral.events` | `user_id` |
| `transactional.categories` | `category_id` |
| `transactional.products` | `product_id` |
| `transactional.users` | `user_id` |
| `transactional.orders` | `order_id` |
| `transactional.order_items` | `order_item_id` |
| `transactional.product_price_history` | `price_history_id` |
| `transactional.returns_refunds` | Unconfirmed |

Rules:

- Entity keys are Kafka message keys, not globally unique event identifiers. In `behavioral.events`, the key (`user_id`) groups many events; deduplication requires the technical identity.
- All "keep one latest record per business key" rules are **provisional**, because repeated-key, update, delete, and tombstone behavior remain unconfirmed for every topic.
- For `transactional.orders`, the payload timestamp must not be used for latest-record selection while the epoch-zero issue is unresolved.

### 5.6 Allowed and Observed Values

Enums are treated as open. A new, structurally valid value is preserved, flagged, profiled, and confirmed with the source owner. It is never quarantined solely because it was absent from the inspected sample.

| Field | Observed values | Expected, not observed | Handling of new values |
|---|---|---|--|
| `behavioral.events.event_type` | `page_view`, `product_search`, `add_to_cart`, `cart_view`, `wishlist_add` | `checkout_start`, `payment_attempt`, `order_complete`, `review_submit` | Preserve in Bronze; flag as `unknown_event_type` in Silver; profile; confirm with source owner. |
| `behavioral.events.device` | `mobile`, `desktop`, `tablet` | — | Default handling. |
| `transactional.orders.status` | `created` | `shipped`, `delivered`, `cancelled`, `refunded` | Default handling. |
| `transactional.orders.payment_method` | `credit_card`, `debit_card`, `cash_on_delivery`, `paypal`, `bank_transfer`, `shaparak`, `google_pay`, `apple_pay` | — | Preserve, flag, profile, and confirm with the source owner. |
| `transactional.users.loyalty_tier` | `Bronze`, `Silver`, `Gold`, `Platinum` | — | Default handling. |
| `transactional.users.device` | None observed | Unconfirmed value set | Profile when values are first observed. |

### 5.7 Amount and Quantity Rules

| Source | Field | Rule | Silver action on violation |
|---|---|---|---|
| `transactional.products` | `price` | `>= 0` | Quarantine. |
| `transactional.products` | `inventory` | `>= 0` when present | Flag for review. |
| `transactional.orders` | `total` | `>= 0` | Quarantine. |
| `transactional.order_items` | `quantity` | `> 0` | Quarantine. |
| `transactional.order_items` | `unit_price` | `>= 0` | Quarantine. |
| `transactional.order_items` | `item_total_amount` | `>= 0` | Quarantine. |
| `transactional.product_price_history` | `price` | `>= 0` | Quarantine. |
| `transactional.returns_refunds` | `refund_amount` | Deferred until the source contract exists. | — |

`order_items` amount relationship (provisional until the source meaning is confirmed):

- Do **not** enforce `item_total_amount = quantity × unit_price`; the shortfall observed in the sample (§3.6) has an unconfirmed meaning.
- Flag records only when `item_total_amount > quantity × unit_price`.
- Optional derived Silver field: `derived_discount = quantity × unit_price − item_total_amount`, clearly labeled as derived, not source-provided.

No range rule is defined for `popularity_score`; the field has not been observed and its range is unconfirmed.

### 5.8 Relationship Checks

Relationship validation belongs to Silver.

| Referencing field | References | Handling |
|---|---|---|
| `categories.parent_category_id` | `categories.category_id` | Flag missing parents for review. |
| `orders.user_id` | `users.user_id` | Flag unresolved references. |
| `order_items.order_id` | `orders.order_id` | Flag unresolved references. |
| `order_items.product_id` | `products.product_id` | Flag unresolved references. |
| `product_price_history.product_id` | `products.product_id` | Flag unresolved references. |

Because related records may arrive late, unresolved references are initially flagged, not automatically quarantined, and may be reconciled later.

No product-category relationship is defined until `products.category` semantics (ID versus name) are confirmed. Expected `returns_refunds` relationships (`order_id`, `order_item_id`) remain undefined until its contract exists.

### 5.9 Invalid and Uncertain Record Handling

| Condition | Layer | Handling |
|---|---|--|
| Avro decode failure | Bronze | Preserve raw bytes with `decode_status` and `decode_error`. Silver quarantines technically unusable records. |
| Missing mandatory field | Silver | Quarantine. |
| Unparseable business timestamp | Silver | Apply the timestamp fallback with provenance and flag `timestamp_status`; do not drop the record. |
| Epoch-zero business timestamp (`transactional.orders`) | Silver | Preserve and flag per §5.4. |
| Negative amount / non-positive quantity | Silver | Quarantine. |
| Unknown but structurally valid `event_type` | Silver | Preserve and flag as `unknown_event_type`; profile. |
| New valid-looking enum value | Silver | Preserve, flag, profile, and confirm with the source owner. |
| Unresolved relationship reference | Silver | Flag per §5.8. |
| `cart_view` with `cart_value = 0.0` and non-empty `cart_items` | Silver | Preserve and flag; requires profiling and source clarification. |
| More than one `is_current = true` record per product | Silver | Flag as a data-quality violation; do not silently repair. |
| Missing optional context field | Silver | Keep the record and flag for data-quality monitoring. |
| `users.email` | Silver / Gold | Personal data: mask or restrict wherever the full value is unnecessary. |

## 6. Discovery Limitations and Open Questions

### 6.1 Source-to-Project Requirement Gaps

The project brief expects fields that the observed Kafka contracts do not supply. These gaps affect revenue, discount, and shipping reporting, geographic analysis, return-rate dashboards, funnel reconciliation, and dimensional model design. They must be resolved with the source owner before the affected models are designed.

| Source | Project-expected fields | Actual source status | Downstream impact |
|---|---|---|---|
| `behavioral.events` | `event_id`, `ip_address`, `utm_source` | Not present in the registered schema | Event identity and attribution reporting are limited. |
| `transactional.products` | `created_at`, `updated_at`, `category_id`, `is_active` | Not present; the source provides an ambiguous `category` field | Category joins and product-state history require clarification. |
| `transactional.users` | `country`, `city` | Not present as separate fields; only the mixed `location` string exists | Reliable geographic dimensions cannot yet be built. |
| `transactional.orders` | `order_date`, `discount_amount`, `final_amount`, `shipping_amount` | Not present | Net-revenue, discount, and shipping analytics cannot be calculated directly. |
| `transactional.order_items` | `discount_amount` | Not present | Any discount value would have to be derived provisionally (§5.7). |
| `transactional.returns_refunds` | Complete return contract | Topic empty; schema absent | Return-rate and refund analytics cannot yet be implemented. |

### 6.2 Representative Message Evidence

Exact representative payloads were inspected in Kafka UI but were not archived in this document. The requirement to retain 10–20 exact real messages per non-empty topic — including key, partition, offset, Kafka timestamp, and decoded value — remains outstanding. No representative message exists for `transactional.returns_refunds` because the topic was empty. When collected, the samples belong in a separate evidence file or appendix so this document stays readable.

### 6.3 Unconfirmed Source Behaviors

The following remain unconfirmed. Kafka UI inspection and the bounded smoke test do not prove them.

```text
full-topic repeated-key frequency
tombstone/null-value frequency
append-only versus update behavior
delete-event behavior
CDC semantics
complete business lifecycle history
source timezone for the behavioral payload timestamp
products.category semantics
returns_refunds schema and message contract
Kafka record header usage
```

Kafka record headers were not inspected during discovery and are not currently selected by the shared Bronze Kafka consumer. Whether the producers use headers for schema, tracing, correlation, or other source metadata remains unconfirmed.

### 6.4 Open Questions

Open questions for the source owner:

| # | Open question | Required evidence |
|---:|---|---|
| 1 | Does `products.category` contain a category ID or a category name? | Source-owner confirmation or producer documentation. The product-category join stays undefined until then. |
| 2 | What timezone does `behavioral.events.timestamp` represent? | Source-owner confirmation of producer timezone handling. |
| 3 | Why do all observed `transactional.orders.timestamp` values decode to epoch zero? | Producer fix or explanation of the intended business time. |
| 4 | What is the `transactional.returns_refunds` message contract? | Registered value schema and representative messages. |
| 5 | Do repeated business keys represent updates, retries, or duplicates? Are deletes or tombstones produced? | Producer contract or CDC documentation per transactional topic; full-topic repeated-key profiling. |
| 6 | Why do some `cart_view` events carry `cart_value = 0.0` with non-empty `cart_items`? | Producer clarification of how `cart_value` is computed; profiling of affected records. |
| 7 | Is "one `is_current = true` record per product" an intended invariant? | Source-owner confirmation; observed data already violates it. |
| 8 | What values does `users.device` carry? | Sample records or field documentation. |
| 9 | Does `item_total_amount` include discounts or other adjustments? | Confirmation of the pricing meaning; the equality check stays disabled until then. |
