from dataclasses import dataclass
from utils.logger import get_logger

logger = get_logger(__name__)

@dataclass
class Column():
    name: str
    dtype: str
    nullable: bool

@dataclass
class Schema():
    """ Expectation for a table"""
    file: str
    primary_key: str
    columns: list[Column]

    def __init__(self, file:str , primary_key: str, columns: list[Column]):
        self.file = file
        self.primary_key = primary_key
        self.columns = columns

    def get_column(self,column_name: str):
        return next((col for col in self.columns if col.name == column_name), None)
    
    def get_required_columns(self):
        return [col.name for col in self.columns if not col.nullable ]

#========================BATCH============================
CITIES = Schema(
    file="cities",
    primary_key="city_id",
    columns=[
        Column(name="city_id", dtype="int64", nullable=False),
        Column(name="city_name", dtype="str", nullable=True),
        Column(name="country", dtype="str", nullable=True),
        Column(name="timezone", dtype="str", nullable=True),
    ],
)


REGIONS = Schema(
    file="regions",
    primary_key="region_id",
    columns=[
        Column("region_id",         "int64",   nullable=False),
        Column("region_name",       "str",   nullable=True),
        Column("city_id",           "int64",   nullable=True),
        Column("delivery_base_fee", "float64", nullable=True),
    ],
)
 
SEGMENTS = Schema(
    file="segments",
    primary_key="segment_id",
    columns=[
        Column("segment_id",       "int64",  nullable=False),
        Column("segment_name",     "str",  nullable=True),
        Column("discount_pct",     "int64",  nullable=True),
        Column("priority_support", "bool", nullable=True),
    ],
)
 
CATEGORIES = Schema(
    file="categories",
    primary_key="category_id",
    columns=[
        Column("category_id",   "int64", nullable=False),
        Column("category_name", "str", nullable=True),
    ],
)
 
TEAMS = Schema(
    file="teams",
    primary_key="team_id",
    columns=[
        Column("team_id",   "int64", nullable=False),
        Column("team_name", "str", nullable=True),
    ],
)
 
REASON_CATEGORIES = Schema(
    file="reason_categories",
    primary_key="reason_category_id",
    columns=[
        Column("reason_category_id", "int64", nullable=False),
        Column("category_name",      "str", nullable=True),
    ],
)
 
REASONS = Schema(
    file="reasons",
    primary_key="reason_id",
    columns=[
        Column("reason_id",          "int64",   nullable=False),
        Column("reason_name",        "str",   nullable=True),
        Column("reason_category_id", "int64",   nullable=True),
        Column("severity_level",     "int64",   nullable=True),
        Column("typical_refund_pct", "float64", nullable=True),
    ]
)
 
CHANNELS = Schema(
    file="channels",
    primary_key="channel_id",
    columns=[
        Column("channel_id",   "int64", nullable=False),
        Column("channel_name", "str", nullable=True),
    ],
)
 
PRIORITIES = Schema(
    file="priorities",
    primary_key="priority_id",
    columns=[
        Column("priority_id",              "int64", nullable=False),
        Column("priority_code",            "str", nullable=True),
        Column("priority_name",            "str", nullable=True),
        Column("sla_first_response_min",   "int64", nullable=True),
        Column("sla_resolution_min",       "int64", nullable=True),
    ],
)

CUSTOMERS = Schema(
    file="customers",
    primary_key="customer_id",
    columns=[
        Column("customer_id",  "int64",      nullable=False),
        Column("full_name",    "str",      nullable=True),
        Column("email",        "str",      nullable=True),
        Column("phone",        "str",      nullable=True),
        Column("region_id",    "float64",      nullable=True),
        Column("segment_id",   "int64",      nullable=True),
        Column("signup_date",  "str", nullable=True),
        Column("gender",       "str",      nullable=True),
        Column("created_at",   "str", nullable=True),
        Column("updated_at",   "str", nullable=True),
    ],
)
 
RESTAURANTS = Schema(
    file="restaurants",
    primary_key="restaurant_id",
    columns=[
        Column("restaurant_id",      "int64",   nullable=False),
        Column("restaurant_name",    "str",   nullable=True),
        Column("region_id",          "int64",   nullable=True),
        Column("category_id",        "int64",   nullable=True),
        Column("price_tier",         "str",   nullable=True),
        Column("rating_avg",         "float64", nullable=True),
        Column("prep_time_avg_min",  "int64",   nullable=True),
        Column("is_active",          "bool",  nullable=True),
        Column("created_at",         "datetime64[us]", nullable=True),
        Column("updated_at",         "datetime64[us]", nullable=True),
    ],
)
 
DRIVERS = Schema(
    file="drivers",
    primary_key="driver_id",
    columns=[
        Column("driver_id",            "int64",   nullable=False),
        Column("driver_name",          "str",   nullable=True),
        Column("driver_phone",         "str",   nullable=True),
        Column("national_id",          "int64",   nullable=True),
        Column("region_id",            "int64",   nullable=True),
        Column("shift",                "str",   nullable=True),
        Column("vehicle_type",         "str",   nullable=True),
        Column("hire_date",            "str", nullable=True),
        Column("rating_avg",           "float64", nullable=True),
        Column("on_time_rate",         "float64", nullable=True),
        Column("cancel_rate",          "float64", nullable=True),
        Column("completed_deliveries", "int64",   nullable=False),
        Column("is_active",            "bool",  nullable=True),
        Column("created_at",           "str", nullable=True),
        Column("updated_at",           "str", nullable=True),
    ],
)
 
AGENTS = Schema(
    file="agents",
    primary_key="agent_id",
    columns=[
        Column("agent_id",             "int64",   nullable=False),
        Column("agent_name",           "str",   nullable=True),
        Column("agent_email",          "str",   nullable=True),
        Column("agent_phone",          "int64",   nullable=True),
        Column("team_id",              "int64",   nullable=True),
        Column("skill_level",          "str",   nullable=True),
        Column("hire_date",            "str", nullable=True),
        Column("avg_handle_time_min",  "int64",   nullable=True),
        Column("resolution_rate",      "float64", nullable=True),
        Column("csat_score",           "float64", nullable=True),
        Column("is_active",            "bool",  nullable=True),
        Column("created_at",           "str", nullable=True),
        Column("updated_at",           "str", nullable=True),
    ],
)

#========================STREAM============================
ORDERS = Schema(
    file="orders",
    primary_key="order_id",
    columns=[
        Column("order_id",        "str",      nullable=False),
        Column("customer_id",     "int64",      nullable=True),
        Column("restaurant_id",   "int64",      nullable=True),
        Column("driver_id",       "int64",      nullable=True),
        Column("region_id",       "int64",      nullable=True),
        Column("order_amount",    "float64",    nullable=True),
        Column("delivery_fee",    "float64",    nullable=True),
        Column("discount_amount", "int64",    nullable=True),
        Column("total_amount",    "float64",    nullable=True),
        Column("order_status",    "str",      nullable=True),
        Column("payment_method",  "str",      nullable=True),
        Column("order_created_at","datetime64[us]", nullable=True),
        Column("delivered_at",    "datetime64[us]", nullable=True),
    ],
)
 
TICKETS = Schema(
    file="tickets",
    primary_key="ticket_id",
    columns=[
        Column("ticket_id",          "str",      nullable=False),
        Column("order_id",           "str",      nullable=True),
        Column("customer_id",        "int64",      nullable=True),
        Column("driver_id",          "int64",      nullable=True),
        Column("restaurant_id",      "int64",      nullable=True),
        Column("agent_id",           "int64",      nullable=True),
        Column("reason_id",          "int64",      nullable=True),
        Column("priority_id",        "int64",      nullable=True),
        Column("channel_id",         "int64",      nullable=True),
        Column("status",             "str",      nullable=True),
        Column("refund_amount",      "float64",    nullable=True),
        Column("created_at",         "str", nullable=True),
        Column("first_response_at",  "str", nullable=True),
        Column("resolved_at",        "str", nullable=True),
        Column("sla_first_due_at",   "str", nullable=True),
        Column("sla_resolve_due_at", "str", nullable=True),
    ],
)
 
TICKET_EVENTS = Schema(
    file="ticket_events",
    primary_key="event_id",
    columns=[
        Column("event_id",   "str", nullable=False),
        Column("ticket_id",  "str", nullable=True),
        Column("agent_id",   "int64", nullable=True),
        Column("event_ts",   "str", nullable=True),
        Column("old_status", "str", nullable=True),
        Column("new_status", "str", nullable=True),
        Column("notes",      "str", nullable=True),
    ],
)

#========================mapping============================
MAPPING: dict[str, Schema] = {
    #BATCH
    "cities":             CITIES,
    "regions":            REGIONS,
    "segments":           SEGMENTS,
    "categories":         CATEGORIES,
    "teams":              TEAMS,
    "reason_categories":  REASON_CATEGORIES,
    "reasons":            REASONS,
    "channels":           CHANNELS,
    "priorities":         PRIORITIES,
    "customers":          CUSTOMERS,
    "restaurants":        RESTAURANTS,
    "drivers":            DRIVERS,
    "agents":             AGENTS,
    #STREAM
    "orders":             ORDERS,
    "tickets":            TICKETS,
    "ticket_events":      TICKET_EVENTS,
}

class schema_tracker():

    def get_schema(self,file:str) -> Schema | None:
        if file not in MAPPING:
            logger.error(f"Didn't find schema for {file}")
            return None
        return MAPPING[file]