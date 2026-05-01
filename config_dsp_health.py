from __future__ import annotations

from dataclasses import dataclass

STATUS_OK      = "OK"
STATUS_PARTIAL = "PARTIAL"
STATUS_FLAG    = "FLAG"

SCORING_EXCLUDED: set = set()

MAX_FINDINGS = 24

# Aligned with Framework / Mastery / Strategy (was -30 at top, now -18)
PRIORITY_POINTS = {10: -18, 9: -15, 8: -13, 7: -11, 6: -9, 5: -7, 4: -5, 3: -3, 2: -2, 1: 0}
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
    'H001': 'Total KPI vs Target',
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
    'H001': 'The primary KPI is the agreed measure of DSP efficiency with the client. When it breaches the target, the strategy is not delivering what was promised — and every week without action compounds the gap.',
    'H002': 'A ±10% pacing gap is acceptable variation. Beyond that, either the account is leaving committed budget unspent or it is overrunning — both need to be managed proactively, not discovered at month end.',
    'H003': 'DSP is a channel that compounds over time as audiences build. Flat YoY sales on DSP means the channel is not compounding — it is treading water at best, declining at worst.',
    'H004': 'Spend decline on a scaling account is a strategy failure. Spend decline on an efficiency account may be intentional — but only if documented and the primary KPI is improving.',
    'H005': 'CTR is the signal that tells you whether the creative is still resonating with the audience. A sustained decline means you are paying the same or more to reach people who are increasingly indifferent to the ad.',
    'H006': 'CVR decline on retargeting means the people being targeted are less and less likely to buy. This happens when the retargeting pool is exhausted, pricing becomes uncompetitive, or the product page degrades.',
    'H007': 'CPM rising while the primary KPI deteriorates means the account is competing for expensive inventory that is not converting.',
    'H008': 'Declining DPVR means the audience is seeing the ad but not engaging with the product page — a creative or product relevance problem that accumulates cost without results.',
    'H014': 'Missing the GGS commitment triggers penalties that increase the effective cost of DSP inventory for all Quartile accounts — not just the one that missed.',
    'H016': 'An account with overdue payments is at risk of being paused. DSP campaigns cannot run without billing current, and a pause mid-flight damages campaign continuity and performance.',
    'H017': 'A declining ChurnZero score aggregates multiple health indicators simultaneously. Ignoring it means churn risk escalates to a point where it cannot be addressed before the client makes a decision.',
}

SOURCES = {
    'H001': '02_DSP_Date_Range_KPIs · ROAS/ACoS + 13_Client_Success_Insights · Primary_Spend_KPI__c + 14_DSP_Project_on_SF · Target_ROAS__c / Target_ACoS__c',
    'H002': '02_DSP_Date_Range_KPIs · AdSpend + 14_DSP_Project_on_SF · daily_target_spend__c',
    'H003': '05_DSP_Yearly_KPIs · YoY_AdSales',
    'H004': '05_DSP_Yearly_KPIs · YoY_AdSpend',
    'H005': '03_DSP_L24M_Monthly_Performance · CTR (last 3 months)',
    'H006': '03_DSP_L24M_Monthly_Performance · CR (last 3 months)',
    'H007': '03_DSP_L24M_Monthly_Performance · CPM (last 3 months)',
    'H008': '02_DSP_Date_Range_KPIs · DPV / Impressions + 03_DSP_L24M_Monthly_Performance',
    'H014': '13_Client_Success_Insights · Amazon_GGS__c',
    'H016': 'Manual verification — Stripe/billing system',
    'H017': '13_Client_Success_Insights · CSM_Churn_Risk__c / Account_Risk_Score__c',
}

ACTION = {
    'H001': {
        'FLAG':    'Present the KPI gap to the client before the next call — include the gap %, the root cause (creative, audience exhaustion, CPM, or funnel mix), and a specific change you are making this week. Do not wait for month end.',
        'PARTIAL': 'Monitor daily for the next 7 days. If the gap does not close, escalate to the strategist and prepare a client-facing explanation before the next touchpoint.',
        'OK':      'No action required. Document current performance in the QR notes so the trend is visible if it shifts next period.',
    },
    'H002': {
        'FLAG':    'Identify whether the gap is caused by a delivery issue (paused line items, budget caps hit) or a target mismatch in Salesforce. Fix the root cause today and update the daily target if it no longer reflects the agreed budget.',
        'PARTIAL': 'Flag the pacing deviation to the strategist. Check for any capped orders and confirm the monthly budget is still aligned with what the client approved.',
        'OK':      'Pacing is on track. No action required.',
    },
    'H003': {
        'FLAG':    'Diagnose the source of the YoY decline before the next QR: is it audience pool exhaustion, reduced budget, product issues, or increased competition? Present the root cause and a recovery plan — not just the number.',
        'PARTIAL': 'Note the slight YoY decline in your QR narrative. Confirm whether it is seasonal or structural, and document your read in Salesforce CS Notes.',
        'OK':      'YoY sales are growing. Highlight this in the client QR as evidence the channel is compounding.',
    },
    'H004': {
        'FLAG':    'Confirm with the client whether the spend decline is intentional. If yes, document it in Salesforce. If no, identify the delivery or budget constraint causing it and resolve this week.',
        'PARTIAL': 'Verify the spend trend aligns with the current strategy phase. If this is an efficiency phase, document it explicitly so the next reviewer does not flag it as a problem.',
        'OK':      'Spend trajectory is healthy. No action required.',
    },
    'H005': {
        'PARTIAL': 'Review creative assets — test a new format or refresh the existing one. CTR decline over 3 consecutive months signals audience fatigue, not a seasonal blip.',
        'OK':      'CTR trend is stable or improving. Keep monitoring monthly.',
    },
    'H006': {
        'PARTIAL': 'Check whether the retargeting pool has been refreshed recently. If the same audience has been targeted for 60+ days without replenishment, pause and rebuild the segment before spend continues.',
        'OK':      'CVR trend is stable or improving. No action required.',
    },
    'H007': {
        'PARTIAL': 'Rising CPM without a corresponding improvement in the primary KPI means inventory cost is increasing without delivering returns. Review bid strategy and consider narrowing targeting to reduce competition for expensive placements.',
        'OK':      'CPM trend is stable or improving. No action required.',
    },
    'H008': {
        'PARTIAL': 'Review the creative and the product detail page. Declining DPVR means the ad is being seen but not driving engagement — either the creative is not relevant to the audience or the PDP is not compelling enough to click through.',
        'OK':      'DPVR is stable or improving. No action required.',
    },
    'H014': {
        'FLAG':    'Escalate GGS enrollment to the DSP strategist immediately. An account not enrolled in GGS is not contributing to the media commitment and may be missing access to preferred inventory pricing.',
        'PARTIAL': 'Confirm current spend trajectory against the GGS commitment target with the strategist. If pacing suggests the commitment will be missed, raise it now — not at quarter end.',
        'OK':      'GGS enrollment confirmed. Verify spend trajectory is on pace with the strategist during the next sync.',
    },
    'H016': {
        'FLAG':    'Contact the client today about the overdue payment. DSP campaigns risk being paused without notice if billing is not resolved. Do not wait for the next scheduled call.',
        'OK':      'Financial status is clear. No action required.',
    },
    'H017': {
        'FLAG':    'Schedule a dedicated risk call with the client this week — not the next QR. Identify the top two drivers of the high churn score and bring a specific retention proposal, not a check-in.',
        'PARTIAL': 'Document the medium churn risk and its primary driver in Salesforce. Build a retention action into the next QR agenda so it is addressed on the call, not after.',
        'OK':      'Churn risk is low. Continue regular QR cadence and monitor for any shift in the score.',
    },
}


@dataclass(frozen=True)
class ControlResult:
    status: str
    what:   str = ''
    why:    str = ''
    source: str = ''
    action: str = ''
