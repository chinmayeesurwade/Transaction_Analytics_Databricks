# Power BI build specification

Import `transactions_clean_expected.csv` and `customers.csv` for a local preview. For a cloud build, export the corresponding Databricks tables after running the notebook. Create a `Date` table. Relate Customers[customer_id] (one) to Transactions[customer_id] (many), and Date[Date] (one) to Transactions[Transaction Date] (many). Use single-direction filtering. This is a portfolio star schema, with Transactions as the fact table.

## Calculated date column and table

```DAX
Transaction Date = DATEVALUE(Transactions[transaction_ts])
Date = CALENDAR(DATE(2026,7,1), DATE(2026,9,30))
Month = FORMAT('Date'[Date], "YYYY-MM")
```

## Measures

```DAX
Transactions = COUNTROWS(Transactions)
Failed Transactions = CALCULATE([Transactions], Transactions[status] = "Failed")
Failure Rate = DIVIDE([Failed Transactions], [Transactions])
Completed Value INR = CALCULATE(SUM(Transactions[amount_inr]), Transactions[status] = "Completed")
Average Completed Seconds = CALCULATE(AVERAGE(Transactions[processing_seconds]), Transactions[status] = "Completed")
```

Format Failure Rate as percent. Build KPI cards for transactions, failure rate, completed value and average completed seconds; a month trend, channel comparison, segment table and date/channel slicers. Compare totals against `expected_metrics.json` before presenting.

## Performance exercise

Use Power BI Desktop Performance Analyzer on the channel/segment page. Record the visual duration, remove unused columns or reduce high-cardinality labels, rerun the same view, and record the before/after values. Do not claim a speed improvement until measured in Desktop.
