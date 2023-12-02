# Technical Assessment Homework (RevoData)

## Introduction

This project will let you showcase your technical skills in data engineering, applied to a real-world dataset.

Let's imagine you're a very successful (and rich) data engineer who wants to reinvest their life's earnings in Amsterdam property, for a passive source of income. You are looking to buy houses and apartments, then either rent them out long term, or through Airbnb.

Skilled data engineer that you are, you have already extracted a dataset on house location and per-night prices from Airbnb, and you have a second data stream coming in from Kamernet, which you are actively scraping.

## Goals

You want to have an idea of which post codes are better suited for investment, and from those, in which it's more profitable to rent long term, or through Airbnb.

## Deliverables

At a minimum, you will build a (set of) data pipeline(s) that:

- Ingest the rental data scraped from Kamernet (data/rentals.json)
- Ingest the data from Airbnb (data/airbnb.csv)
- Clean both datasets
- Follow the Databricks Medallion Architecture
- Calculate the potential average revenue per house, per postcode (rental and Airbnb), as derived from the data

It is expected that you deliver as part of the project:

- Exploratory notebooks with data checks (in the `/scratch` folder)
- Notebook(s) for your pipeline (in the `/src` folder)
- Unit tests for your pipeline (in the `/src/tests` folder)
- Documentation for your pipeline (in the `/docs` folder, explain the why, not the how)
- An export of the data sets produced by your pipeline, (in the `/data/output` folder, Parquet format)
- Configurations for your pipeline jobs (in the `/resources` folder)

Save everything locally into the current Git repository, zip it up, and share it with us.

### Stretch Goals

Following are a number of stretch goals of increasing difficulty that willl give us an idea of how far you can go. We **do not expect** that you'll be able to achieve all of these in the given time, so pick and choose whatever suits you best.

#### Level 1 / Engineer

- [ ] Build a CI-CD pipeline that deploys your data pipeline
- [ ] Run tests in your CICD pipeline
- [ ] Use pre-commit hooks to ensure code quality

#### Level 2 / Artist

- [ ] Build a Databricks visualization or dashboard showing the potential revenue per postcode (rental and Airbnb)
- [ ] Create diagrams of the data flows and of your CICD pipeline

#### Level 3 / Future-Proof

- [ ] Use Delta Live Tables to build your pipelines
- [ ] Use expectations (if using DLT) or another framework (if not), to ensure data quality
- [ ] Deploy your pipeline using Databricks Asset Bundles

#### Level 4 / Steampunk IED

- [ ] Load the data from `rentals.json` one record at a time with streaming ingestion
- [ ] Update the gold layer table(s) in real time as new streaming data arrives

#### Level 5 / Powerlevel 10K

- [ ] Use the `/data/geo/post_codes.geojson` geographic dataset to enrich the Airbnb data with missing postcodes,
- [ ] - or - Query an external API such as [public.opendatasoft.com](https://public.opendatasoft.com/explore/dataset/georef-netherlands-postcode-pc4/api/) to fill in the missing postcodes using a UDF
- [ ] Use the `/data/geo/amsterdam_areas.geojson` geographic dataset for your visualization

## Review

Once you have completed this project, we shall review it together. We are paying special attention to the pipeline logic you followed, and to what you did when things went south. And with this data we're giving you, things will go pretty south, so be creative with your workarounds and maybe even move the goalposts a bit in your favour.

Finally, as a side-note, we're also using ChatGPT. It's great. So don't be shy about employing it if you can, we want to see that you've also internalized whatever lessons you've learned from its input.

**Good luck, and see you on the other side!**
