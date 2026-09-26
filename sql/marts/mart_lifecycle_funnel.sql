-- Per-wallet passthrough of funnel flags/eligibility timestamps. Kept at
-- wallet grain (rather than pre-aggregated) so Python can apply "as of"
-- maturity filtering before computing stage conversion — see
-- eventstream.analytics.lifecycle.funnel_conversion.
CREATE OR REPLACE TABLE mart_lifecycle_funnel AS
SELECT * FROM int_wallet_funnel_flags;
