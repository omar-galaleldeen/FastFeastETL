from dataclasses import dataclass
from utils.logger import get_logger

logger = get_logger(__name__)

@dataclass
class Column():
    name: str
    dtype: str
    nullable: bool
    allowed_values: list[str] = None

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
    
    def get_categorical_columns(self) -> list[Column]:
        return [col for col in self.columns if col.allowed_values is not None]

#========================BATCH============================
CITIES = Schema(
    file="cities",
    primary_key="city_id",
    columns=[
        Column(name="city_id", dtype="int", nullable=False),
        Column(name="city_name", dtype="str", nullable=False),
        Column(name="country", dtype="str", nullable=False),
        Column(name="timezone", dtype="str", nullable=False),
    ],
)


REGIONS = Schema(
    file="regions",
    primary_key="region_id",
    columns=[
        Column("region_id",         "int",   nullable=False),
        Column("region_name",       "str",   nullable=False),
        Column("city_id",           "int",   nullable=False),
        Column("delivery_base_fee", "float", nullable=False),
    ],
)
 
SEGMENTS = Schema(
    file="segments",
    primary_key="segment_id",
    columns=[
        Column("segment_id",       "int",  nullable=False),
        Column("segment_name",     "str",  nullable=False, allowed_values=["Regular", "VIP"]),
        Column("discount_pct",     "int",  nullable=True),
        Column("priority_support", "bool", nullable=True),
    ],
)
 
CATEGORIES = Schema(
    file="categories",
    primary_key="category_id",
    columns=[
        Column("category_id",   "int", nullable=False),
        Column("category_name", "str", nullable=False),
    ],
)
 
TEAMS = Schema(
    file="teams",
    primary_key="team_id",
    columns=[
        Column("team_id",   "int", nullable=False),
        Column("team_name", "str", nullable=True),
    ],
)
 
REASON_CATEGORIES = Schema(
    file="reason_categories",
    primary_key="reason_category_id",
    columns=[
        Column("reason_category_id", "int", nullable=False),
        Column("category_name",      "str", nullable=False,
               allowed_values=["Delivery", "Food", "Payment"]),
    ],
)
 
REASONS = Schema(
    file="reasons",
    primary_key="reason_id",
    columns=[
        Column("reason_id",          "int",   nullable=False),
        Column("reason_name",        "str",   nullable=True),
        Column("reason_category_id", "int",   nullable=True),
        Column("severity_level",     "int",   nullable=True),
        Column("typical_refund_pct", "float", nullable=True),
    ]
)
 
CHANNELS = Schema(
    file="channels",
    primary_key="channel_id",
    columns=[
        Column("channel_id",   "int", nullable=False),
        Column("channel_name", "str", nullable=False,
                       allowed_values=["app", "chat", "phone", "email"]),
    ],
)
 
PRIORITIES = Schema(
    file="priorities",
    primary_key="priority_id",
    columns=[
        Column("priority_id",              "int", nullable=False),
        Column("priority_code",            "str", nullable=False,
               allowed_values=["P1", "P2", "P3", "P4"]),
        Column("priority_name",            "str", nullable=False,
               allowed_values=["Critical", "High", "Medium", "Low"]),
        Column("sla_first_response_min",   "int", nullable=True),
        Column("sla_resolution_min",       "int", nullable=True),
    ],
)

CUSTOMERS = Schema(
    file="customers",
    primary_key="customer_id",
    columns=[
        Column("customer_id",  "int",      nullable=False),
        Column("full_name",    "str",      nullable=True),
        Column("email",        "str",      nullable=True),
        Column("phone",        "str",      nullable=True),
        Column("region_id",    "float",      nullable=True),
        Column("segment_id",   "int",      nullable=True),
        Column("signup_date",  "datetime", nullable=True),
        Column("gender",       "str",      nullable=True, allowed_values=["male", "female"]),
        Column("created_at",   "datetime", nullable=True),
        Column("updated_at",   "datetime", nullable=True),
    ],
)
 
RESTAURANTS = Schema(
    file="restaurants",
    primary_key="restaurant_id",
    columns=[
        Column("restaurant_id",      "int",   nullable=False),
        Column("restaurant_name",    "str",   nullable=True),
        Column("region_id",          "int",   nullable=True),
        Column("category_id",        "int",   nullable=True),
        Column("price_tier",         "str",   nullable=True,
               allowed_values=["Low", "Mid", "High"]),
        Column("rating_avg",         "float", nullable=True),
        Column("prep_time_avg_min",  "int",   nullable=True),
        Column("is_active",          "bool",  nullable=True),
        Column("created_at",         "datetime", nullable=True),
        Column("updated_at",         "datetime", nullable=True),
    ],
)
 
DRIVERS = Schema(
    file="drivers",
    primary_key="driver_id",
    columns=[
        Column("driver_id",            "int",   nullable=False),
        Column("driver_name",          "str",   nullable=True),
        Column("driver_phone",         "str",   nullable=True),
        Column("national_id",          "int",   nullable=True),
        Column("region_id",            "int",   nullable=True),
        Column("shift",                "str",   nullable=True,
               allowed_values=["morning", "evening", "night"]),
        Column("vehicle_type",         "str",   nullable=True),
        Column("hire_date",            "datetime", nullable=True),
        Column("rating_avg",           "float", nullable=True),
        Column("on_time_rate",         "float", nullable=True),
        Column("cancel_rate",          "float", nullable=True),
        Column("completed_deliveries", "int",   nullable=False),
        Column("is_active",            "bool",  nullable=True),
        Column("created_at",           "datetime", nullable=True),
        Column("updated_at",           "datetime", nullable=True),
    ],
)
 
AGENTS = Schema(
    file="agents",
    primary_key="agent_id",
    columns=[
        Column("agent_id",             "int",   nullable=False),
        Column("agent_name",           "str",   nullable=True),
        Column("agent_email",          "str",   nullable=True),
        Column("agent_phone",          "int",   nullable=True),
        Column("team_id",              "int",   nullable=True),
        Column("skill_level",          "str",   nullable=True,
               allowed_values=["Junior", "Mid", "Senior", "Lead"]),
        Column("hire_date",            "str", nullable=True),
        Column("avg_handle_time_min",  "int",   nullable=True),
        Column("resolution_rate",      "float", nullable=True),
        Column("csat_score",           "float", nullable=True),
        Column("is_active",            "bool",  nullable=True),
        Column("created_at",           "datetime", nullable=True),
        Column("updated_at",           "datetime", nullable=True),
    ],
)

#========================STREAM============================
ORDERS = Schema(
    file="orders",
    primary_key="order_id",
    columns=[
        Column("order_id",        "str",      nullable=False),
        Column("customer_id",     "int",      nullable=True),
        Column("restaurant_id",   "int",      nullable=True),
        Column("driver_id",       "int",      nullable=True),
        Column("region_id",       "int",      nullable=True),
        Column("order_amount",    "float",    nullable=True),
        Column("delivery_fee",    "float",    nullable=True),
        Column("discount_amount", "int",    nullable=True),
        Column("total_amount",    "float",    nullable=True),
        Column("order_status",    "str",      nullable=True,
                allowed_values=["Delivered", "Cancelled", "Refunded"]),
        Column("payment_method",  "str",      nullable=True,
               allowed_values=["card", "cash", "wallet"]),
        Column("order_created_at","datetime", nullable=True),
        Column("delivered_at",    "datetime", nullable=True),
    ],
)
 
TICKETS = Schema(
    file="tickets",
    primary_key="ticket_id",
    columns=[
        Column("ticket_id",          "str",      nullable=False),
        Column("order_id",           "str",      nullable=True),
        Column("customer_id",        "int",      nullable=True),
        Column("driver_id",          "int",      nullable=True),
        Column("restaurant_id",      "int",      nullable=True),
        Column("agent_id",           "int",      nullable=True),
        Column("reason_id",          "int",      nullable=True),
        Column("priority_id",        "int",      nullable=True),
        Column("channel_id",         "int",      nullable=True),
        Column("status",             "str",      nullable=True,
               allowed_values=["Resolved", "Closed", "Reopened", "Open", "InProgress"]),
        Column("refund_amount",      "float",    nullable=True),
        Column("created_at",         "datetime", nullable=True),
        Column("first_response_at",  "datetime", nullable=True),
        Column("resolved_at",        "datetime", nullable=True),
        Column("sla_first_due_at",   "datetime", nullable=True),
        Column("sla_resolve_due_at", "datetime", nullable=True),
    ],
)
 
TICKET_EVENTS = Schema(
    file="ticket_events",
    primary_key="event_id",
    columns=[
        Column("event_id",   "str", nullable=False),
        Column("ticket_id",  "str", nullable=True),
        Column("agent_id",   "int", nullable=True),
        Column("event_ts",   "datetime", nullable=True),
        Column("old_status", "str", nullable=True,
               allowed_values=["Open", "InProgress", "Resolved", "Closed", "Reopened"]),
        Column("new_status", "str", nullable=False,
               allowed_values=["Open", "InProgress", "Resolved", "Closed", "Reopened"]),
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

class schema_registry():

    def get_schema(self,file:str) -> Schema | None:
        if file not in MAPPING:
            logger.error(f"Didn't find schema for {file}")
            return None
        return MAPPING[file]