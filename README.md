# Universe Event Management Scripts

This repository contains Python scripts and SQL files for managing event data from Universe.com. The tools help with:

1. Fetching event orders and ticket data from Universe API
2. Storing data in a PostgreSQL database
3. Exporting event data to CSV
4. Managing ticket rates and categories for analysis

## Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Universe API credentials (Client ID, Client Secret, Refresh Token)
- Required Python packages (see `requirements.txt`)

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Environment Variables

Set the following environment variables or provide them as command-line arguments:

```bash
export UNIVERSE_CLIENT_ID='your_client_id'
export UNIVERSE_CLIENT_SECRET='your_client_secret'
export UNIVERSE_REFRESH_TOKEN='your_refresh_token'
export UNIVERSESCRIPT_POSTGRES_DSN='your_postgres_connection_string'
```

## Scripts

### universe_orders_to_postgres.py

Fetches orders and ticket data from Universe API and stores it in a PostgreSQL database.

**Usage:**
```bash
python universe_orders_to_postgres.py \
    [--client-id UNIVERSE_CLIENT_ID] \
    [--client-secret UNIVERSE_CLIENT_SECRET] \
    [--refresh-token UNIVERSE_REFRESH_TOKEN] \
    [--pg-dsn POSTGRES_DSN] \
    [--limit PAGE_LIMIT] \
    [--backfill-days DAYS] \
    [--include-closed]
```

**Options:**
- `--client-id`: Universe API client ID (or set UNIVERSE_CLIENT_ID environment variable)
- `--client-secret`: Universe API client secret (or set UNIVERSE_CLIENT_SECRET environment variable)
- `--refresh-token`: Universe API refresh token (or set UNIVERSE_REFRESH_TOKEN environment variable)
- `--pg-dsn`: PostgreSQL connection string (or set UNIVERSESCRIPT_POSTGRES_DSN environment variable)
  Example: `postgresql://user:pass@host:5432/db?sslmode=require`
- `--limit`: Number of records per page (default: 10, max: 50, or set UNIVERSESCRIPT_PAGE_LIMIT environment variable)
- `--backfill-days`: Number of days to look back for updates (default: 7, or set UNIVERSESCRIPT_BACKFILL_DAYS environment variable)
- `--include-closed`: Include events with fetch_state other than 'active' in the sync

### universe_orders_to_csv.py

Exports event orders and order items to a CSV file.

**Usage:**
```bash
python universe_orders_to_csv.py \
    --event-id EVENT_ID \
    [--client-id CLIENT_ID] \
    [--client-secret CLIENT_SECRET] \
    [--refresh-token REFRESH_TOKEN] \
    [--outfile ORDERS_CSV]
```

**Options:**
- `--event-id`: Universe event ID (required, can also be set via UNIVERSE_EVENT_ID environment variable)
- `--client-id`: Universe API client ID (can be set via UNIVERSE_CLIENT_ID environment variable)
- `--client-secret`: Universe API client secret (can be set via UNIVERSE_CLIENT_SECRET environment variable)
- `--refresh-token`: Universe API refresh token (can be set via UNIVERSE_REFRESH_TOKEN environment variable)
- `--outfile`: Output CSV file path (default: "orders.csv")

## Database Schema

### Tables

1. **event**: Stores event information
2. **ticket_order**: Contains order details
3. **order_item**: Individual ticket items within orders
4. **rate**: Ticket rate/price information
5. **rate_category**: Categories for ticket rates

### Views

- **v_event_order_items**: Joins events, orders, and order items for reporting
- **v_event_rates**: Joins events and rates for reporting

## Workflow

1. **Database Setup**:
   - Create a new PostgreSQL database (e.g. with supabase)
   - Run the DDL script to create tables and views:
     ```bash
     psql -d your_database -f universe_db_ddl_create_script.sql
     ```

2. **Configuration**:
   - Set up environment variables in `.env` file or export them:
     ```bash
     export UNIVERSE_CLIENT_ID='your_client_id'
     export UNIVERSE_CLIENT_SECRET='your_client_secret'
     export UNIVERSE_REFRESH_TOKEN='your_refresh_token'
     export UNIVERSESCRIPT_POSTGRES_DSN='your_postgres_connection_string'
     ```

3. **Synchronization**:
   - Run the main script to fetch and store data:
     ```bash
     python universe_orders_to_postgres.py --backfill-days 30
     ```
   - For scheduled updates, use the GitHub workflow (`.github/workflows/universe-to-supabase-sync.yml`)

4. **Data Analysis**:
   - Query the database using the provided views
   - Connect your BI tool (like Looker Studio) to the database
   - Use the `v_event_order_items` and `v_event_rates` views for reporting

## Links

- [Universe Developer Documentation](https://developers.universe.com/docs/introduction)
- [Universe GraphQL Code Examples](https://developers.universe.com/docs/code-examples)
- [Universe GraphQL Explorer](https://www.universe.com/graphiql)
- [Supabase](https://supabase.com/)
- [Looker Studio](https://lookerstudio.google.com/)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

```
Copyright [2025] [Alexander Schenk]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```