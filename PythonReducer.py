#!/usr/bin/env python3
import sys

from collections import defaultdict

# Store data by key
data = defaultdict(list)

# Read input line by line
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        key, value = line.split('\t', 1)
        data[key].append(value)
    except ValueError:
        continue  # skip malformed lines

# Process each key
for key, values in data.items():
    key_str = key.strip()

    sum_val = 0.0
    total_revenue = 0.0
    total_cost = 0.0

    for val in values:
        try:
            if key_str.startswith(("RevenueCountry:", "SalesTrendProfit:", "SalesTrendUnits:", "ItemPerformance:", "DemandForecast:")):
                sum_val += float(val)

            elif key_str.startswith("CostEfficiency:"):
                parts = val.strip().split(',')
                if len(parts) == 2:
                    revenue = float(parts[0])
                    cost = float(parts[1])
                    total_revenue += revenue
                    total_cost += cost
        except ValueError:
            continue  # skip invalid data

    # Emit results
    if key_str.startswith("CostEfficiency:"):
        margin = ((total_revenue - total_cost) / total_revenue * 100) if total_revenue != 0 else 0
        print(f"{key}\tRevenue: {total_revenue:.2f}, Cost: {total_cost:.2f}, Margin: {margin:.2f}%")
    else:
        print(f"{key}\tTotal: {sum_val:.2f}")
