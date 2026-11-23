# Design

<!-- Up to two pages. Replace each prompt with your own text; keep the headings. -->

## 1. Architecture

<!-- Components and data flow, with a simple diagram (ASCII or Mermaid). -->

## 2. Key decisions

<!-- For each: what you chose, the main alternative, and why. One or two lines each. -->

- **Kafka topics and partitioning** (and the ordering that follows):
- **Deduplication** (where, and on which key):
- **Offset commits** relative to database writes:
- **Schema, hypertable and indexes:**

## 3. Ambiguities and how I handled them

<!--
The requirements are incomplete on purpose. List every ambiguity you noticed, not only
the ones we name. For each: the options, what you chose and why, in one or two lines.
If you'd normally ask a product owner, say what you'd ask and what you assumed meanwhile.

At least: the candle price when bid or ask is null; ticks arriving after their candle was
served; ticks flagged mayBeOutOfSequence; empty intervals.
-->

| Ambiguity | Options | Decision and why |
|---|---|---|
| | | |

## 4. Design-only requirements

<!-- One line each: how your design meets it, or what it would take. -->

- **No loss during a deployment of any component:**
- **10-minute database outage:**
- **Volume and retention** (500 symbols, 2,000 ticks/s peak, 1 year):
- **Two future consumers of the live stream:**
- **Graceful shutdown:**

## 5. Where data can still be lost

<!-- Be specific: which component, in which situation, and roughly how much. -->
