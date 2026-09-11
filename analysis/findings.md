# Findings: what actually drives the German day-ahead price?

**Data**: 2,328 hourly day-ahead prices for the **DE-LU bidding zone**, 8 June – 12 September
2026 (14 complete weeks plus the published part of the current week), ingested from SMARD.de
into this repository's own warehouse and merged with revision-aware upserts.

Every number below is re-runnable from the marts:

```bash
make bi                          # the SQL in analysis/bi_queries.sql
python analysis/make_charts.py   # regenerates the charts in docs/images/
```

## The answer in four numbers

| | |
| --- | --- |
| **€178 vs €38** | evening peak (18–20h) vs midday trough (11–14h) — the duck curve, priced |
| **7.9%** | share of all hours priced below zero (185 hours; the minimum was **−€45.87/MWh**) |
| **20.5% vs 3.1%** | share of hours below zero on weekends vs weekdays |
| **−€0.83 vs €192** | average weekend-midday price vs weekday-evening price — the cheapest window of the week is *free* |

The practical takeaway: a weekend midday is the cheapest window of the German
electricity week. The market paid consumers to use power on average (−€0.83/MWh
over 11:00–14:00), while a weekday evening (18:00–20:00) cost €191.89/MWh, a gap
of about €193/MWh.

## 1. The duck curve, priced

![Average price by hour](../docs/images/findings_duck_curve.png)

Averaged over 97 days, the shape is stark: the **13:00 hour averages €29.23/MWh** while
**20:00 averages €208.72/MWh** — a 7× swing *within the same day*. The solar midday floods
the market; the evening ramp, when solar is gone and demand peaks, is where the price lives.

## 2. Negative prices are a summer, weekend phenomenon

![Negative hours and average price by month](../docs/images/findings_monthly.png)

185 of 2,328 hours (7.9%) cleared below zero over the window, and 35 of 97 days contained
at least one negative hour. The worst days were 13–14 June with **10 negative hours each**
(that Saturday averaged just €26.82/MWh). Monthly counts:

| Month | Negative hours | Avg price | Min price |
| --- | --- | --- | --- |
| June 2026 | 34 | €113.75 | −€45.87 |
| July 2026 | 79 | €105.45 | −€12.28 |
| August 2026 | 54 | €126.89 | −€12.15 |
| September 2026 (partial) | 18 | €137.54 | −€19.00 |

The weekday/weekend split is even sharper than the seasonal one: **20.5% of weekend hours
were negative vs 3.1% on weekdays** — industrial demand disappears exactly when solar
output peaks.

## 3. The week rhythm

![Weekend vs weekday profile](../docs/images/findings_week_rhythm.png)

Weekends average **€87.08/MWh against €130.26 on weekdays (−33%)**, but the difference is
not uniform: it is concentrated midday, where the weekend curve collapses into negative
territory while the weekday curve stays well above zero. A consumer with flexible load
should think in terms of *weekend midday*, not "the weekend".

## 4. What intraday volatility is worth

The average **intraday spread (max − min per local day) is €186.36/MWh** — nearly twice the
average price level. For battery storage, EVs, heat pumps, or any flexible load, this is the
number that matters: it is the value of shifting one megawatt-hour by a few hours.

## Caveats

- 14 weeks is a snapshot, and a summer one. German winter dynamics (heating demand, weak
  solar, Dunkelflaute) look different; the same queries on a winter window are one
  `make backfill` away.
- Only day-ahead prices are analysed here. Intraday and imbalance markets have their own
  dynamics.
- DE-LU is one bidding zone; cross-border effects (flows, market coupling) are out of scope.
- The supply side (actual wind/solar generation) is not in the warehouse yet — negative
  hours are *consistent with* a solar glut, but this document does not attribute them.

## What I would look at next

- Join **DWD open weather** (wind, radiation) to the marts to attribute negative hours to
  renewables directly.
- Ingest the **ENTSO-E generation mix** to quantify the solar/wind contribution per hour.
- Extend the window through winter to see how the duck curve flattens when solar disappears.
