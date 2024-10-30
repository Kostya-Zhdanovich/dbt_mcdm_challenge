with
    tiktok_data as (
        select
            cast(ad_id as string) as ad_id,
            add_to_cart,
            cast(adgroup_id as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            channel,
            clicks,
            cast(null as int64) as comments,
            cast(null as string) as creative_id,
            date,
            cast(null as int64) as engagements,
            impressions,
            rt_installs + skan_app_install as installs,
            cast(null as int64) as likes,
            cast(null as int64) as link_clicks,
            cast(null as string) as placement_id,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            purchase,
            registrations,
            cast(null as int64) as revenue,
            cast(null as int64) as shares,
            spend,
            conversions + skan_conversion as total_conversions,
            video_views
        from {{ ref('src_ads_tiktok_ads_all_data') }}
    ),

    bing_data as (
        select
            cast(ad_id as string) as ad_id,
            cast(null as int64) as ad_to_cart,
            cast(adset_id as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            channel,
            clicks,
            cast(null as int64) as comments,
            cast(null as string) as creative_id,
            date,
            cast(null as int64) as engagements,
            imps as impressions,
            cast(null as int64) as installs,
            cast(null as int64) as likes,
            cast(null as int64) as link_clicks,
            cast(null as string) as placement_id,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            cast(null as int64) as purchase,
            cast(null as int64) as registrations,
            revenue,
            cast(null as int64) as shares,
            spend,
            conv as total_conversions,
            cast(null as int64) as video_views
        from {{ ref("src_ads_bing_all_data") }}
    ),

    twitter_data as (
        select
            cast(null as string) as ad_id,
            cast(null as int64) as add_to_cart,
            cast(null as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            channel,
            clicks,
            comments,
            cast(null as string) as creative_id,
            date,
            engagements,
            impressions,
            cast(null as int64) as installs,
            likes,
            url_clicks as link_clicks,
            cast(null as string) as placement_id,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            cast(null as int64) as purchase,
            cast(null as int64) as registrations,
            cast(null as int64) as revenue,
            retweets as shares,
            spend,
            cast(null as int) as total_conversions,
            video_total_views as video_views
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
    ),

    facebook_data as (
        select
            cast(ad_id as string) as ad_id,
            add_to_cart,
            cast(adset_id as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            channel,
            clicks,
            comments,
            cast(creative_id as string) as creative_id,
            date,
            clicks + comments + likes + shares + views as engagements,
            impressions,
            mobile_app_install as installs,
            likes,
            inline_link_clicks as link_clicks,
            cast(null as string) as placement_id,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            purchase_value as purchase,
            complete_registration as registrations,
            cast(null as int64) as revenue,
            shares,
            spend,
            purchase as total_conversions,
            cast(null as int64) as video_views
             from {{ ref('src_ads_creative_facebook_all_data') }}
    ),

    unified_data as (
        select *
        from tiktok_data
        union all
        select *
        from facebook_data
        union all
        select *
        from bing_data
        union all
        select *
        from twitter_data
    )

select *
from unified_data
