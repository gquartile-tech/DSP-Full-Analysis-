from __future__ import annotations

from dataclasses import dataclass

STATUS_OK      = "OK"
STATUS_PARTIAL = "PARTIAL"
STATUS_FLAG    = "FLAG"

SCORING_EXCLUDED: set = set()

MAX_FINDINGS = 24

PRIORITY_POINTS = {10: -30, 9: -27, 8: -24, 7: -21, 6: -18, 5: -15, 4: -12, 3: -9, 2: -6, 1: 0}
IMPACT_LABEL    = {10: 'Critical', 9: 'High', 8: 'High', 7: 'Medium', 6: 'Medium', 5: 'Medium', 4: 'Low', 3: 'Low', 2: 'Visibility', 1: 'Visibility'}

IMPORTANCE = {
    'H001': 10,
    'H002': 10,
    'H003':  8,
    'H004':  7,
    'H005':  1,
    'H006':  1,
    'H007':  1,
    'H008':  1,
    'H014':  9,
    'H016':  9,
    'H017':  8,
}

CONTROL_NAMES = {
    'H001': 'Total ROAS vs Target',
    'H002': 'Ad Spend Pacing vs Budget Target',
    'H003': 'YoY Ad Sales Growth',
    'H004': 'YoY Ad Spend Growth',
    'H005': 'CTR Trend (MoM)',
    'H006': 'CVR Trend (MoM)',
    'H007': 'CPM Trend (MoM)',
    'H008': 'DPVR Stable or Improving',
    'H014': 'GGS Commitment On Track',
    'H016': 'Financial Status Clear (No Overdue Payments)',
    'H017': 'Churn Score Reviewed',
}

WHY = {
    'H001': 'ROAS below target is the most direct signal that DSP spend is not generating the expected return. When the gap persists beyond one review period without explanation, the strategy needs to change.',
    'H002': 'A ±10% pacing gap is acceptable variation. Beyond that, either the account is leaving committed budget unspent or it is overrunning — both need to be managed proactively, not discovered at month end.',
    'H003': 'DSP is a channel that compounds over time as audiences build. Flat YoY sales on DSP means the channel is not compounding — it is treading water at best, declining at worst.',
    'H004': 'Spend decline on a scaling account is a strategy failure. Spend decline on an efficiency account may be intentional — but only if documented and ROAS is improving.',
    'H005': 'CTR is the signal that tells you whether the creative is still resonating with the audience. A sustained decline means you are paying the same or more to reach people who are increasingly indifferent to the ad.',
    'H006': 'CVR decline on retargeting means the people being targeted are less and less likely to buy. This happens when the retargeting pool is exhausted, pricing becomes uncompetitive, or the product page degrades.',
    'H007': 'CPM rising while ROAS falls means the account is competing for expensive inventory that is not converting.',
    'H008': 'Declining DPVR means the audience is seeing the ad but not engaging with the product page — a creative or product relevance problem that accumulates cost without results.',
    'H014': 'Missing the GGS commitment triggers penalties that increase the effective cost of DSP inventory for all Quartile accounts — not just the one that missed.',
    'H016': 'An account with overdue payments is at risk of being paused. DSP campaigns cannot run without billing current, and a pause mid-flight damages campaign continuity and ROAS.',
    'H017': 'A declining ChurnZero score aggregates multiple health indicators simultaneously. Ignoring it means churn risk escalates to a point where it cannot be addressed before the client makes a decision.',
}

SOURCES = {
    'H001': '02_DSP_Date_Range_KPIs · ROAS + 14_DSP_Project_on_SF · Target_ROAS__c',
    'H002': '02_DSP_Date_Range_KPIs · AdSpend + 14_DSP_Project_on_SF · daily_target_spend__c',
    'H003': '04_DSP_Monthly_YoY_Comparison · YoY_AdSales (latest month)',
    'H004': '04_DSP_Monthly_YoY_Comparison · YoY_AdSpend (latest month)',
    'H005': '03_DSP_L24M_Monthly_Performance · CTR (last 3 months)',
    'H006': '03_DSP_L24M_Monthly_Performance · CR (last 3 months)',
    'H007': '03_DSP_L24M_Monthly_Performance · CPM (last 3 months)',
    'H008': '02_DSP_Date_Range_KPIs · DPV / Impressions + 03_DSP_L24M_Monthly_Performance',
    'H014': 'Planilha CoE v2 — Account Health: GGS Target',
    'H016': 'Planilha CoE v2 — Account Health: Financial (Stripe)',
    'H017': '13_Client_Success_Insights · CSM_Churn_Risk__c / Account_Risk_Score__c',
}


@dataclass(frozen=True)
class ControlResult:
    status: str
    what:   str = ''
    why:    str = ''
    source: str = ''
