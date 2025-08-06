#!/usr/bin/env python3
import sys
import csv

# Flag to skip header
header_skipped = False

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Use csv reader to handle commas within quotes properly
    try:
        fields = next(csv.reader([line]))
    except Exception as e:
        continue

    # Skip header line
    if not header_skipped and "Region" in fields:
        header_skipped = True
        continue

    # Expecting 14 fields
    if len(fields) != 14:
        continue

    # Strip whitespaces
    fields = [f.strip() for f in fields]

    try:
        region = fields[0]
        country = fields[1]
        item_type = fields[2]
        order_date = fields[5]
        units_sold = fields[8]
        unit_price = fields[9]
        unit_cost = fields[10]
        total_revenue = fields[11]
        total_cost = fields[12]
        total_profit = fields[13]

        # Skip incomplete rows
        if not all([region, country, item_type, order_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit]):
            continue

        # Extract year and month from order date (mm/dd/yyyy)
        date_parts = order_date.split('/')
        if len(date_parts) != 3:
            continue
        month, day, year = date_parts

        # Emit key-value pairs
        print(f"RevenueCountry:{country}\t{total_profit}")
        print(f"SalesTrendProfit:{year}\t{total_profit}")
        print(f"SalesTrendUnits:{year}\t{units_sold}")
        print(f"ItemPerformance:{item_type}\t{units_sold}")
        print(f"DemandForecast:{item_type},{year}-{month}\t{units_sold}")
        print(f"CostEfficiency:{item_type}\t{total_revenue},{total_cost}")

    except Exception as e:
        # Optionally log the error
        continue
