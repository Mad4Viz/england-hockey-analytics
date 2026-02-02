# England Hockey Analytics - Semantic Layer

This document defines the data boundaries for the England Hockey Analytics tool.
It prevents hallucination by explicitly documenting what data exists and what doesn't.

**Upload this file to Claude Projects to ground responses.**

---

## Data Inventory

### Tables Available
<!-- Fill after Phase 6: dbt Staging - list all dim_, fct_, agg_ tables -->

---

## Data We HAVE
<!-- Fill after Phase 4: Extract - based on what we actually scraped -->

---

## Data We DON'T HAVE
<!-- Fill after Phase 4: Extract - document limitations honestly -->

---

## Valid Questions
<!-- Fill after Phase 6: dbt Marts - questions that CAN be answered -->

---

## Invalid Questions (Decline These)
<!-- Fill after Phase 4: Extract - questions that CANNOT be answered due to data gaps -->

---

## Data Freshness
<!-- Document how often the pipeline runs and data is refreshed -->

---

## Source Attribution

Data sourced from: [England Hockey](https://www.englandhockey.co.uk/)

---

## How to Use This Document

1. Upload to Claude Projects as context
2. Claude reads this before answering questions
3. If a question falls under "Invalid Questions", Claude should decline
4. All answers must be traceable to data in "Tables Available"
