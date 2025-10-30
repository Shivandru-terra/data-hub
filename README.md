# Data-HUB

## Creating Tables in BigQuery

### Terra Table Creation

To create a table in BigQuery for Terra:
```bash
bq mk --table ai-analytics-463910:Terra.Event "C:\codebase\Data-HUB\app\schema\bq_terra_schema.json"
```

> **Note:** Replace the path with your actual path to the schema file

### Ripple Table Creation

To create a table in BigQuery for Ripple (already present):
```bash
bq mk --table ai-analytics-463910:Ripple.Event "C:\codebase\Data-HUB\app\schema\bq_ripple_schema.json"
```

> **Note:** Replace the path with your actual path to the schema file

## Adding Data from Mixpanel

Once the table is created, follow these steps to add data from Mixpanel:

1. Navigate to the Data-hub directory:
```bash
   cd C:\codebase\Data-hub
```

2. Run the BigQuery service:
```bash
   python -m app.services.big_query
```

3. **Important:** Change the value of `platform` in the `.env` file as per your requirement before running the script

## Configuration

Make sure to update your `.env` file with the appropriate platform value:
- Set `platform=terra` for Terra data
- Set `platform=ripple` for Ripple data