# Instructions for Adding a New Ad Platform to MCDM

This guide explains how to integrate data from a new ad platform into the Marketing Common Data Model (MCDM) using dbt.

## Steps

### 1. **Prepare the Raw Data Source**
   - Ensure the new ad platform's data is available in BigQuery (e.g., `src_ads_new_platform_data`).

### 2. **Create a CTE in `paid_ads_performance.sql`**
   - Open `paid_ads_performance.sql` and add a CTE (Common Table Expression) for the new platform using existing CTEs as templates.
   - Match fields to the MCDM structure, casting and renaming as needed.

   Example:

   ```sql
   new_platform_data as (
       select
           cast(ad_id as string) as ad_id,
           clicks,
           comments,
           date,
           impressions,
           spend,
           total_conversions,
           -- add other necessary fields and set nulls where data is missing
           null as add_to_cart,
           null as installs,
           null as likes,
           null as link_clicks,
           null as placement_id,
           null as post_click_conversions,
           null as post_view_conversions,
           null as posts,
           null as registrations,
           null as revenue,
           null as shares,
           null as video_views
       from {{ ref('src_ads_new_platform_data') }}
   ),

### 3. **Add to Final Union**
   - nclude the new CTE in the unified_data union query.

### 4. **Run and Verify**
   - Run the model in dbt Cloud with this command 'paid_ads_performance.sql'
    