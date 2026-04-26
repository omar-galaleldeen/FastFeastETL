# FastFeastETL Data Transformations & Loading Guide

## Transformation Pipeline Overview

All files go through the same validation pipeline, but transformations differ based on file type (batch dimension vs stream fact):

```
Raw File → FileReader → Schema Validation → Records Validation → Orphan Validation (stream only) → PII Masking → DWH Loader
```

---

## 1. Batch Files → Dimension Tables

### 1.1 cities.csv → `dim_region` (via city cache)

**Source Columns:**
| Column | Dtype | Nullable |
|--------|-------|----------|
| city_id | int | NOT NULL |
| city_name | str | NOT NULL |
| country | str | NOT NULL |
| timezone | str | NOT NULL |

**Transformation:**
Cities are NOT inserted directly into a table. Instead, they populate an in-memory cache (`_city_cache`) used by `load_regions()` to denormalize `city_name` into `dim_region`:

```python
# In dim_loader.load_cities():
_city_cache = {
    str(row["city_id"]).split(".")[0]: row["city_name"]
    for _, row in df.iterrows()
}
```

**Loading Strategy:** No direct INSERT — cache only.

---

### 1.2 regions.csv → `dim_region`

**Source Columns:**
| Column | Dtype | Nullable |
|--------|-------|----------|
| region_id | int | NOT NULL |
| region_name | str | NOT NULL |
| city_id | int | NOT NULL |
| delivery_base_fee | float | NOT NULL |

**Transformations:**

1. **City Name Resolution:** `city_id` from source is looked up in `_city_cache` to get `city_name`:
   ```python
   city_id_list = _safe(df, "city_id")
   city_names = [city_map.get(str(v).split(".")[0]) for v in city_id_list]
   ```
   If cache is empty (restart scenario), falls back to reading `data/master/cities.csv` directly.

2. **Float ID Normalization:** `city_id` values like `1.0` are normalized to `"1"` for cache lookup.

**Target Table: `dim_region`**
| Target Column | Source Column | Transformation |
|---------------|---------------|----------------|
| region_id | region_id | Direct mapping |
| region_name | region_name | Direct mapping |
| city_name | city_id → city_name | Resolved from city cache |
| delivery_base_fee | delivery_base_fee | Direct mapping |

**Loading:** `INSERT INTO dim_region ... ON CONFLICT (region_id) DO NOTHING`

---

### 1.3 segments.csv → `dim_segment`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| segment_id | int | NOT NULL | - |
| segment_name | str | NOT NULL | Regular, VIP |
| discount_pct | int | nullable | - |
| priority_support | bool | nullable | - |

**Transformations:**

1. **Boolean Parsing:** `priority_support` string values (`"True"`, `"true"`, `"1"`, `"False"`, `"false"`, `"0"`) are converted to Python bools in `schema_validator.py`.

2. **Categorical Validation:** `segment_name` must be `"Regular"` or `"VIP"` — rejected otherwise.

**Target Table: `dim_segment`**
| Target Column | Source Column | Transformation |
|---------------|---------------|----------------|
| segment_id | segment_id | Direct mapping |
| segment_name | segment_name | Categorical validated |
| discount_pct | discount_pct | Direct mapping |
| priority_support | priority_support | String → bool conversion |

**Loading:** `INSERT INTO dim_segment ... ON CONFLICT (segment_id) DO NOTHING`

---

### 1.4 categories.csv → `dim_category`

**Source Columns:**
| Column | Dtype | Nullable |
|--------|-------|----------|
| category_id | int | NOT NULL |
| category_name | str | NOT NULL |

**Transformations:** None (direct mapping).

**Target Table: `dim_category`**
| Target Column | Source Column |
|---------------|---------------|
| category_id | category_id |
| category_name | category_name |

**Loading:** `INSERT INTO dim_category ... ON CONFLICT (category_id) DO NOTHING`

---

### 1.5 teams.csv → `dim_team`

**Source Columns:**
| Column | Dtype | Nullable |
|--------|-------|----------|
| team_id | int | NOT NULL |
| team_name | str | nullable |

**Transformations:** None (direct mapping).

**Target Table: `dim_team`**
| Target Column | Source Column |
|---------------|---------------|
| team_id | team_id |
| team_name | team_name |

**Loading:** `INSERT INTO dim_team ... ON CONFLICT (team_id) DO NOTHING`

---

### 1.6 reason_categories.csv → `dim_reason_category`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| reason_category_id | int | NOT NULL | - |
| category_name | str | NOT NULL | Delivery, Food, Payment |

**Transformations:**
- **Categorical Validation:** `category_name` must be one of: `"Delivery"`, `"Food"`, `"Payment"`.

**Target Table: `dim_reason_category`**
| Target Column | Source Column |
|---------------|---------------|
| reason_category_id | reason_category_id |
| category_name | category_name |

**Loading:** `INSERT INTO dim_reason_category ... ON CONFLICT (reason_category_id) DO NOTHING`

---

### 1.7 reasons.csv → `dim_reason`

**Source Columns:**
| Column | Dtype | Nullable |
|--------|-------|----------|
| reason_id | int | NOT NULL |
| reason_name | str | nullable |
| reason_category_id | int | nullable |
| severity_level | int | nullable |
| typical_refund_pct | float | nullable |

**Transformations:**

1. **Category Name Denormalization:** `reason_category_id` is looked up against `dim_reason_category` to populate `reason_category` (the category name):
   ```python
   reason_cat_map = _fetch_reason_category_map()
   reason_cat_names = [
       reason_cat_map.get(str(v).split(".")[0])
       for v in reason_cat_id_list
   ]
   ```

**Target Table: `dim_reason`**
| Target Column | Source Column | Transformation |
|---------------|---------------|----------------|
| reason_id | reason_id | Direct mapping |
| reason_name | reason_name | Direct mapping |
| reason_category_id | reason_category_id | Direct mapping |
| reason_category | reason_category_id → name | Denormalized lookup from dim_reason_category |
| severity_level | severity_level | Direct mapping |
| typical_refund_pct | typical_refund_pct | Direct mapping |

**Loading:** `INSERT INTO dim_reason ... ON CONFLICT (reason_id) DO NOTHING`

---

### 1.8 channels.csv → `dim_channel`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| channel_id | int | NOT NULL | - |
| channel_name | str | NOT NULL | app, chat, phone, email |

**Transformations:**
- **Categorical Validation:** `channel_name` must be `"app"`, `"chat"`, `"phone"`, or `"email"`.

**Target Table: `dim_channel`**
| Target Column | Source Column |
|---------------|---------------|
| channel_id | channel_id |
| channel_name | channel_name |

**Loading:** `INSERT INTO dim_channel ... ON CONFLICT (channel_id) DO NOTHING`

---

### 1.9 priorities.csv → `dim_priority`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| priority_id | int | NOT NULL | - |
| priority_code | str | NOT NULL | P1, P2, P3, P4 |
| priority_name | str | NOT NULL | Critical, High, Medium, Low |
| sla_first_response_min | int | nullable | - |
| sla_resolution_min | int | nullable | - |

**Transformations:**
- **Categorical Validation:** Both `priority_code` and `priority_name` are validated against their allowed values.

**Target Table: `dim_priority`**
| Target Column | Source Column |
|---------------|---------------|
| priority_id | priority_id |
| priority_code | priority_code |
| priority_name | priority_name |
| sla_first_response_min | sla_first_response_min |
| sla_resolution_min | sla_resolution_min |

**Loading:** `INSERT INTO dim_priority ... ON CONFLICT (priority_id) DO NOTHING`

---

### 1.10 customers.csv → `dim_customer`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| customer_id | int | NOT NULL | - |
| full_name | str | NOT NULL | - |
| email | str | nullable | - |
| phone | str | nullable | - |
| region_id | float | nullable | - |
| segment_id | int | nullable | - |
| signup_date | datetime | nullable | - |
| gender | str | nullable | male, female |
| created_at | datetime | nullable | - |
| updated_at | datetime | nullable | - |

**Transformations:**

1. **PII Masking (SHA-256):**
   ```python
   # pii_handler.mask_pii() hashes:
   self.pii_columns = {
       "customers": ["email", "phone"],
   }
   ```
   - `email` → `SHA256(email)` (null/NaN/empty left unchanged)
   - `phone` → `SHA256(phone)` (null/NaN/empty left unchanged)
   - `full_name` is **NOT** hashed — kept as plain text per spec

2. **Datetime Parsing:** `signup_date`, `created_at`, `updated_at` converted from string to datetime.

3. **Categorical Validation:** `gender` must be `"male"` or `"female"`.

**Target Table: `dim_customer`**
| Target Column | Source Column | Transformation |
|---------------|---------------|----------------|
| customer_id | customer_id | Direct mapping |
| full_name | full_name | Plain text (not hashed) |
| email | email | **SHA-256 hashed** |
| phone | phone | **SHA-256 hashed** |
| region_id | region_id | Direct mapping |
| segment_id | segment_id | Direct mapping |
| signup_date | signup_date | str → date |
| gender | gender | Categorical validated |
| created_at | created_at | str → timestamp |
| updated_at | updated_at | str → timestamp |
| _loaded_at | - | Auto-set to NOW() |

**Loading:** `INSERT INTO dim_customer ... ON CONFLICT (customer_id) DO NOTHING`

---

### 1.11 restaurants.json → `dim_restaurant`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| restaurant_id | int | NOT NULL | - |
| restaurant_name | str | NOT NULL | - |
| region_id | int | nullable | - |
| category_id | int | nullable | - |
| price_tier | str | nullable | Low, Mid, High |
| rating_avg | float | nullable | - |
| prep_time_avg_min | int | nullable | - |
| is_active | bool | nullable | - |
| created_at | datetime | nullable | - |
| updated_at | datetime | nullable | - |

**Transformations:**

1. **Boolean Parsing:** `is_active` string → Python bool via `schema_validator`.

2. **Categorical Validation:** `price_tier` must be `"Low"`, `"Mid"`, or `"High"`.

3. **Range Validation:** `rating_avg` must be ≤ 5.0.

**Target Table: `dim_restaurant`**
| Target Column | Source Column | Transformation |
|---------------|---------------|----------------|
| restaurant_id | restaurant_id | Direct mapping |
| restaurant_name | restaurant_name | Direct mapping |
| region_id | region_id | Direct mapping |
| category_id | category_id | Direct mapping |
| price_tier | price_tier | Categorical validated |
| rating_avg | rating_avg | Range checked (≤ 5) |
| prep_time_avg_min | prep_time_avg_min | Direct mapping |
| is_active | is_active | str → bool |
| created_at | created_at | str → timestamp |
| updated_at | updated_at | str → timestamp |
| _loaded_at | - | Auto-set to NOW() |

**Loading:** `INSERT INTO dim_restaurant ... ON CONFLICT (restaurant_id) DO NOTHING`

---

### 1.12 drivers.csv → `dim_driver`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| driver_id | int | NOT NULL | - |
| driver_name | str | NOT NULL | - |
| driver_phone | str | nullable | - |
| national_id | int | nullable | - |
| region_id | int | nullable | - |
| shift | str | nullable | morning, evening, night |
| vehicle_type | str | nullable | - |
| hire_date | datetime | nullable | - |
| rating_avg | float | nullable | - |
| on_time_rate | float | nullable | - |
| cancel_rate | float | nullable | - |
| completed_deliveries | int | NOT NULL | - |
| is_active | bool | nullable | - |
| created_at | datetime | nullable | - |
| updated_at | datetime | nullable | - |

**Transformations:**

1. **PII Masking (SHA-256):**
   - `driver_phone` → `SHA256(driver_phone)`
   - `national_id` → `SHA256(national_id)`
   - `driver_name` is **NOT** hashed

2. **Format Validation:**
   - `driver_phone`: Egyptian phone regex `^0?1[0125]\d{8}$` (10-11 digits)
   - `national_id`: Must be exactly 15 characters
   - `hire_date`: String → datetime

3. **Categorical Validation:** `shift` must be `"morning"`, `"evening"`, or `"night"`.

4. **Range Validation:**
   - `rating_avg` ≤ 5.0
   - `on_time_rate` ≤ 1.0
   - `cancel_rate` ≤ 1.0
   - All numeric columns ≥ 0

**Target Table: `dim_driver`**
| Target Column | Source Column | Transformation |
|---------------|---------------|----------------|
| driver_id | driver_id | Direct mapping |
| driver_name | driver_name | Plain text |
| driver_phone | driver_phone | **SHA-256 hashed** |
| national_id | national_id | **SHA-256 hashed** |
| region_id | region_id | Direct mapping |
| shift | shift | Categorical validated |
| vehicle_type | vehicle_type | Direct mapping |
| hire_date | hire_date | str → date |
| rating_avg | rating_avg | Range checked (≤ 5) |
| on_time_rate | on_time_rate | Range checked (≤ 1) |
| cancel_rate | cancel_rate | Range checked (≤ 1) |
| completed_deliveries | completed_deliveries | Range checked (≥ 0) |
| is_active | is_active | str → bool |
| created_at | created_at | str → timestamp |
| updated_at | updated_at | str → timestamp |
| _loaded_at | - | Auto-set to NOW() |

**Loading:** `INSERT INTO dim_driver ... ON CONFLICT (driver_id) DO NOTHING`

---

### 1.13 agents.csv → `dim_agent`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| agent_id | int | NOT NULL | - |
| agent_name | str | NOT NULL | - |
| agent_email | str | nullable | - |
| agent_phone | str | nullable | - |
| team_id | int | nullable | - |
| skill_level | str | nullable | Junior, Mid, Senior, Lead |
| hire_date | datetime | nullable | - |
| avg_handle_time_min | int | nullable | - |
| resolution_rate | float | nullable | - |
| csat_score | float | nullable | - |
| is_active | bool | nullable | - |
| created_at | datetime | nullable | - |
| updated_at | datetime | nullable | - |

**Transformations:**

1. **PII Masking (SHA-256):**
   - `agent_email` → `SHA256(agent_email)`
   - `agent_phone` → `SHA256(agent_phone)`
   - `agent_name` is **NOT** hashed

2. **Format Validation:**
   - `agent_email`: Must match `^[\w\.-]+@[\w\.-]+\.com$`
   - `agent_phone`: Egyptian phone regex

3. **Categorical Validation:** `skill_level` must be `"Junior"`, `"Mid"`, `"Senior"`, or `"Lead"`.

4. **Range Validation:** All numeric columns ≥ 0, rates ≤ 1.0, `csat_score` ≤ 5.0.

**Target Table: `dim_agent`**
| Target Column | Source Column | Transformation |
|---------------|---------------|----------------|
| agent_id | agent_id | Direct mapping |
| agent_name | agent_name | Plain text |
| agent_email | agent_email | **SHA-256 hashed** |
| agent_phone | agent_phone | **SHA-256 hashed** |
| team_id | team_id | Direct mapping |
| skill_level | skill_level | Categorical validated |
| hire_date | hire_date | str → date |
| avg_handle_time_min | avg_handle_time_min | Direct mapping |
| resolution_rate | resolution_rate | Range checked (≤ 1) |
| csat_score | csat_score | Range checked (≤ 5) |
| is_active | is_active | str → bool |
| created_at | created_at | str → timestamp |
| updated_at | updated_at | str → timestamp |
| _loaded_at | - | Auto-set to NOW() |

**Loading:** `INSERT INTO dim_agent ... ON CONFLICT (agent_id) DO NOTHING`

---

## 2. Stream Files → Fact Tables

### 2.1 orders.json → `fact_order`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| order_id | str | NOT NULL | - |
| customer_id | int | nullable | - |
| restaurant_id | int | nullable | - |
| driver_id | int | nullable | - |
| region_id | int | nullable | - |
| order_amount | float | nullable | - |
| delivery_fee | float | nullable | - |
| discount_amount | float | nullable | - |
| total_amount | float | nullable | - |
| order_status | str | nullable | Pending, Preparing, OutForDelivery, Delivered, Cancelled, Refunded |
| payment_method | str | NOT NULL | card, cash, wallet |
| order_created_at | datetime | nullable | - |
| delivered_at | datetime | nullable | - |

**Transformations:**

1. **Date Key Derivation:** `order_created_at` → `date_key` (YYYYMMDD integer):
   ```python
   def _date_key(ts_series):
       return [int(ts.strftime("%Y%m%d")) if pd.notna(ts) else None for ts in ts_series]
   ```

2. **Time Key Derivation:** `order_created_at` → `time_key` (HHMM integer):
   ```python
   def _time_key(ts_series):
       return [int(ts.strftime("%H%M")) if pd.notna(ts) else None for ts in ts_series]
   ```

3. **Column Renaming:** `order_created_at` → `created_at` in target.

4. **Categorical Validation:** `order_status` and `payment_method` validated against allowed values.

5. **Range Validation:** All monetary columns (`order_amount`, `delivery_fee`, `discount_amount`, `total_amount`) must be ≥ 0.

6. **Orphan FK Checks:**
   - `customer_id` → must exist in `dim_customer.customer_id`
   - `restaurant_id` → must exist in `dim_restaurant.restaurant_id`
   - `driver_id` → must exist in `dim_driver.driver_id`
   - `region_id` → must exist in `dim_region.region_id`

**Target Table: `fact_order`**
| Target Column | Source Column | Transformation |
|---------------|---------------|----------------|
| order_sk | - | Auto-increment (BIGSERIAL) |
| order_id | order_id | Direct mapping |
| customer_id | customer_id | FK checked against dim_customer |
| restaurant_id | restaurant_id | FK checked against dim_restaurant |
| driver_id | driver_id | FK checked against dim_driver |
| region_id | region_id | FK checked against dim_region |
| date_key | order_created_at | **Derived**: YYYYMMDD integer |
| time_key | order_created_at | **Derived**: HHMM integer |
| order_status | order_status | Categorical validated |
| payment_method | payment_method | Categorical validated |
| created_at | order_created_at | str → timestamp (renamed) |
| delivered_at | delivered_at | str → timestamp |
| order_amount | order_amount | Range checked (≥ 0) |
| delivery_fee | delivery_fee | Range checked (≥ 0) |
| discount_amount | discount_amount | Range checked (≥ 0) |
| total_amount | total_amount | Range checked (≥ 0) |
| _loaded_at | - | Auto-set to NOW() |

**Loading:** `INSERT INTO fact_order ... ON CONFLICT (order_id) DO NOTHING`

---

### 2.2 tickets.csv → `fact_ticket`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| ticket_id | str | NOT NULL | - |
| order_id | str | nullable | - |
| customer_id | int | nullable | - |
| driver_id | int | nullable | - |
| restaurant_id | int | nullable | - |
| agent_id | int | nullable | - |
| reason_id | int | nullable | - |
| priority_id | int | nullable | - |
| channel_id | int | nullable | - |
| status | str | nullable | Resolved, Closed, Reopened, Open, InProgress |
| refund_amount | float | nullable | - |
| created_at | datetime | nullable | - |
| first_response_at | datetime | nullable | - |
| resolved_at | datetime | nullable | - |
| sla_first_due_at | datetime | nullable | - |
| sla_resolve_due_at | datetime | nullable | - |

**Transformations:**

1. **Region ID Derivation:** `region_id` is NOT in tickets.csv — it's looked up from `fact_order` via `order_id`:
   ```python
   def _fetch_order_region_map(order_ids):
       # Query: SELECT order_id, region_id FROM fact_order WHERE order_id = ANY($1)
       # Returns {order_id: region_id} mapping
   ```

2. **Date/Time Keys:** Derived from `created_at`:
   - `date_key` = YYYYMMDD integer
   - `time_key` = HHMM integer

3. **SLA Metric Calculations (computed at load time):**

   **First Response Time:**
   ```python
   first_response_min = (first_response_at - created_at).total_seconds() / 60
   ```

   **Resolution Time:**
   ```python
   resolution_min = (resolved_at - created_at).total_seconds() / 60
   ```

   **SLA Breach Flags:**
   ```python
   sla_response_breached = first_response_at > sla_first_due_at  # True if breached
   sla_resolution_breached = resolved_at > sla_resolve_due_at    # True if breached
   ```

   **Reopened Flag:**
   ```python
   reopened = (status == "Reopened")
   ```

   > **Note:** If `first_response_at` or `resolved_at` is NULL, the breach flag defaults to `True` (considered breached until resolved).

4. **Categorical Validation:** `status` validated against allowed values.

5. **Range Validation:** `refund_amount` ≥ 0.

6. **Orphan FK Checks:**
   - `order_id` → must exist in `fact_order.order_id`
   - `agent_id` → must exist in `dim_agent.agent_id`

**Target Table: `fact_ticket`**
| Target Column | Source Column | Transformation |
|---------------|---------------|----------------|
| ticket_sk | - | Auto-increment (BIGSERIAL) |
| ticket_id | ticket_id | Direct mapping |
| order_id | order_id | FK checked against fact_order |
| customer_id | customer_id | Direct mapping |
| region_id | - | **Derived**: Looked up from fact_order via order_id |
| restaurant_id | restaurant_id | Direct mapping |
| driver_id | driver_id | Direct mapping |
| agent_id | agent_id | FK checked against dim_agent |
| reason_id | reason_id | Direct mapping |
| priority_id | priority_id | Direct mapping |
| channel_id | channel_id | Direct mapping |
| date_key | created_at | **Derived**: YYYYMMDD integer |
| time_key | created_at | **Derived**: HHMM integer |
| status | status | Categorical validated |
| refund_amount | refund_amount | Range checked (≥ 0) |
| created_at | created_at | str → timestamp |
| first_response_at | first_response_at | str → timestamp |
| resolved_at | resolved_at | str → timestamp |
| sla_first_due_at | sla_first_due_at | str → timestamp |
| sla_resolve_due_at | sla_resolve_due_at | str → timestamp |
| first_response_min | created_at, first_response_at | **Calculated**: (resp - created) in minutes |
| resolution_min | created_at, resolved_at | **Calculated**: (resolved - created) in minutes |
| sla_response_breached | first_response_at, sla_first_due_at | **Calculated**: response > due |
| sla_resolution_breached | resolved_at, sla_resolve_due_at | **Calculated**: resolution > due |
| reopened | status | **Calculated**: status == "Reopened" |
| _loaded_at | - | Auto-set to NOW() |

**Loading:** `INSERT INTO fact_ticket ... ON CONFLICT (ticket_id) DO NOTHING`

---

### 2.3 ticket_events.json → `fact_ticket_event`

**Source Columns:**
| Column | Dtype | Nullable | Allowed Values |
|--------|-------|----------|----------------|
| event_id | str | NOT NULL | - |
| ticket_id | str | nullable | - |
| agent_id | int | nullable | - |
| event_ts | datetime | nullable | - |
| old_status | str | nullable | Open, InProgress, Resolved, Closed, Reopened |
| new_status | str | NOT NULL | Open, InProgress, Resolved, Closed, Reopened |
| notes | str | nullable | - |

**Transformations:**

1. **Date/Time Keys:** Derived from `event_ts`:
   - `date_key` = YYYYMMDD integer
   - `time_key` = HHMM integer

2. **Null Handling:** `old_status` is explicitly allowed to be null (first event for a ticket has no prior state). The `stream_records_validator` skips `old_status` during null checks:
   ```python
   ignore_cols = ['old_status']
   cols_to_check = [col for col in df.columns if col not in ignore_cols]
   ```

3. **Categorical Validation:** Both `old_status` (when not null) and `new_status` must be one of: `Open`, `InProgress`, `Resolved`, `Closed`, `Reopened`.

4. **Orphan FK Checks:**
   - `ticket_id` → must exist in `fact_ticket.ticket_id`
   - `agent_id` → must exist in `dim_agent.agent_id`

**Target Table: `fact_ticket_event`**
| Target Column | Source Column | Transformation |
|---------------|---------------|----------------|
| event_sk | - | Auto-increment (BIGSERIAL) |
| event_id | event_id | Direct mapping |
| ticket_id | ticket_id | FK checked against fact_ticket |
| agent_id | agent_id | FK checked against dim_agent |
| date_key | event_ts | **Derived**: YYYYMMDD integer |
| time_key | event_ts | **Derived**: HHMM integer |
| event_ts | event_ts | str → timestamp |
| old_status | old_status | Categorical validated (nullable) |
| new_status | new_status | Categorical validated (NOT NULL) |
| notes | notes | Direct mapping |
| _loaded_at | - | Auto-set to NOW() |

**Loading:** `INSERT INTO fact_ticket_event ... ON CONFLICT (event_id) DO NOTHING`

---

## 3. Special Transformations

### 3.1 PII Hashing Rules

PII masking is applied AFTER validation but BEFORE database load:

```python
# pii_handler.py configuration
self.pii_columns = {
    "customers": ["email", "phone"],        # full_name kept plain text
    "drivers":   ["driver_phone", "national_id"],  # driver_name kept plain text
    "agents":    ["agent_email", "agent_phone"],    # agent_name kept plain text
}
```

**Hashing Algorithm:** SHA-256
**Null Handling:** NULL, NaN, empty strings, and "N/A" values are NOT hashed — they pass through unchanged.

### 3.2 Float ID Normalization

A critical transformation in `fk_checker.py` handles the case where `schema_validator` converts integer IDs to float:

```python
def _normalize_id(value) -> str:
    """370.0 → "370" to match Postgres integer PK "370" """
    s = str(value).strip()
    if s.endswith(".0"):
        try:
            return str(int(float(s)))
        except (ValueError, OverflowError):
            pass
    return s
```

### 3.3 Null-Equivalent String Handling

The `file_reader.py` reads everything as `str` (to avoid pandas auto-inference). This means:
- JSON `null` → Python `None` → string `"None"`
- Empty values → `""`
- NaN → `"nan"`

Schema validation treats the following as null-equivalent:
```python
NULL_STRINGS = {"None", "nan", "NaN", "NaT", ""}
```

### 3.4 Automatic Timestamps

Every dimension and fact table has a `_loaded_at` column automatically set to `NOW()` at INSERT time via the `dim_loader` and `fact_loader`.

---

## 4. Loading Summary

| Source File | File Type | Target Table | Loader Function | Load Strategy |
|-------------|-----------|--------------|-----------------|---------------|
| cities.csv | batch | (cache only) | `load_cities()` | In-memory cache |
| regions.csv | batch | `dim_region` | `load_regions()` | INSERT ON CONFLICT |
| segments.csv | batch | `dim_segment` | `load_segments()` | INSERT ON CONFLICT |
| categories.csv | batch | `dim_category` | `load_categories()` | INSERT ON CONFLICT |
| teams.csv | batch | `dim_team` | `load_teams()` | INSERT ON CONFLICT |
| reason_categories.csv | batch | `dim_reason_category` | `load_reason_categories()` | INSERT ON CONFLICT |
| reasons.csv | batch | `dim_reason` | `load_reasons()` | INSERT ON CONFLICT |
| channels.csv | batch | `dim_channel` | `load_channels()` | INSERT ON CONFLICT |
| priorities.csv | batch | `dim_priority` | `load_priorities()` | INSERT ON CONFLICT |
| customers.csv | batch | `dim_customer` | `load_customers()` | INSERT ON CONFLICT |
| restaurants.json | batch | `dim_restaurant` | `load_restaurants()` | INSERT ON CONFLICT |
| drivers.csv | batch | `dim_driver` | `load_drivers()` | INSERT ON CONFLICT |
| agents.csv | batch | `dim_agent` | `load_agents()` | INSERT ON CONFLICT |
| orders.json | stream | `fact_order` | `load_orders()` | INSERT ON CONFLICT |
| tickets.csv | stream | `fact_ticket` | `load_tickets()` | INSERT ON CONFLICT |
| ticket_events.json | stream | `fact_ticket_event` | `load_ticket_events()` | INSERT ON CONFLICT |

**All loads use:**
```sql
INSERT INTO {table} ({columns}) VALUES ({placeholders})
ON CONFLICT ({pk_col}) DO NOTHING
```

The `ON CONFLICT DO NOTHING` is a safety net — the validation layer guarantees uniqueness, so in normal operation no conflicts occur.

---

## 5. Post-Load: SLA View

After tickets are loaded, the background `sla_updater_job.py` (runs every 10 minutes) creates/refreshes an analytics view:

```sql
CREATE OR REPLACE VIEW v_ticket_sla_performance AS
SELECT
    ticket_id,
    agent_id,
    created_at,
    first_response_at,
    sla_first_due_at,
    resolved_at,
    sla_resolve_due_at,
    CASE WHEN first_response_at <= sla_first_due_at THEN TRUE ELSE FALSE END AS is_first_response_met,
    CASE WHEN resolved_at <= sla_resolve_due_at THEN TRUE ELSE FALSE END AS is_resolution_met
FROM fact_ticket;
```

This view provides ready-to-query SLA compliance metrics without additional calculation.