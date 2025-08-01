import pandas as pd
from sqlalchemy import create_engine, text
import gspread
from google.oauth2.service_account import Credentials
import os
import json

# --- PostgreSQL Connection Info ---
db_user = 'reporting_client1126'
db_pass = 'x297ffY47$Jw'
db_host = '104.131.117.211'
db_port = '5432'
db_name = 'selldo_production'

# --- Google Sheets Settings ---
SPREADSHEET_NAME = "metrics_combined"
WORKSHEET_NAME = "Sheet2"

# --- Create SQLAlchemy DB Engine ---
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# --- SQL Query ---
query = """
WITH date_series AS (
    SELECT generate_series('2023-11-01'::date, CURRENT_DATE, INTERVAL '1 day')::date AS created_date
),
lead_data AS (
    SELECT
        rl.id AS lead_id,
        rl.uuid,
        rl.client_uuid,
        rl.name AS lead_name,
        rl.created_at::date AS lead_created_date,
        rl.reporting_lead_stage_id,
        rl.reporting_sales_id,
        rl.reporting_client_id,
        rl.hotness,
        rl.last_stage_changed_on,
        rl.last_contacted_at,
        rl.total_followup_conducted,
        rl.total_site_visit_conducted,
        rl.next_followup_scheduled_on,
        rl.next_site_visit_scheduled_on,
        rl.touched,
        rl.new_lead,

        rls.name AS lead_stage_name,
        rls.key AS lead_stage_key,

        rcr.created_at::date AS campaign_created_date,
        rcr.reporting_campaign_id,
        rcr.reporting_project_id,
        rcr.reporting_source_id,
        rcr.sub_source,
        rcr.is_new,

        rs.name AS source_name,
        rp.name AS project_name,

        ru.name AS lead_owner_name,

        rsv.scheduled_on::date AS site_visit_scheduled_date,
        rsv.conducted_on::date AS site_visit_conducted_date,
        rsv.confirmed,
        rsv.revisited,

        rbd.booking_date::date AS booking_date,
        rbd.unit_configuration_name AS booked_unit,
        CASE WHEN rbd.id IS NOT NULL THEN TRUE ELSE FALSE END AS booked

    FROM reporting_leads rl
    LEFT JOIN reporting_lead_stages rls ON rl.reporting_lead_stage_id = rls.id
    LEFT JOIN reporting_campaign_responses rcr ON rcr.reporting_lead_id = rl.id
    LEFT JOIN reporting_sources rs ON rcr.reporting_source_id = rs.id
    LEFT JOIN reporting_projects rp ON rcr.reporting_project_id = rp.id
    LEFT JOIN reporting_users ru ON rl.reporting_sales_id = ru.id
    LEFT JOIN reporting_site_visits rsv ON rsv.reporting_lead_id = rl.id
    LEFT JOIN reporting_booking_details rbd ON rbd.reporting_lead_id = rl.id
    WHERE rl.created_at::date >= '2023-11-01'
)
SELECT
    ds.created_date,
    ld.*
FROM date_series ds
LEFT JOIN lead_data ld ON ds.created_date = ld.lead_created_date
ORDER BY ds.created_date, ld.lead_id;
"""

# --- Run Query ---
print("⏳ Running query...")
with engine.connect() as conn:
    df = pd.read_sql_query(text(query), conn)
print(f"✅ Query complete. Rows fetched: {len(df)}")

# --- Authenticate with Google Sheets ---
print("🔐 Authenticating with Google Sheets...")
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# Load credentials from environment variable
service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
gc = gspread.authorize(creds)

print("📄 Opening Google Sheet...")
sheet = gc.open(SPREADSHEET_NAME)
worksheet = sheet.worksheet(WORKSHEET_NAME)

# --- Upload Data to Google Sheet ---
print("📤 Uploading data...")
df = df.astype(str)  # Ensure compatibility with Google Sheets
worksheet.clear()
worksheet.update([df.columns.tolist()] + df.values.tolist())
print("✅ Upload complete to Google Sheet!")
