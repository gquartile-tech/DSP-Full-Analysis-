from __future__ import annotations

from dataclasses import dataclass

STATUS_OK      = "OK"
STATUS_PARTIAL = "PARTIAL"
STATUS_FLAG    = "FLAG"

# M008 and M009 are manual on-call controls. Always OK in scoring.
SCORING_EXCLUDED = {'M008', 'M009'}

MAX_FINDINGS = 24

PRIORITY_POINTS = {10: -18, 9: -15, 8: -13, 7: -11, 6: -9, 5: -7, 4: -5, 3: -3, 2: -2, 1: 0}
IMPACT_LABEL    = {10: 'Critical', 9: 'High', 8: 'High', 7: 'Medium', 6: 'Medium', 5: 'Medium', 4: 'Low', 3: 'Low', 2: 'Visibility', 1: 'Visibility'}

IMPORTANCE = {
    'M001': 10,
    'M002':  9,
    'M003':  3,
    'M004':  8,
    'M005':  8,
    'M006':  8,
    'M007':  7,
    'M008':  8,
    'M009':  7,
}

CONTROL_NAMES = {
    'M001': 'Clear Objective Documented',
    'M002': 'ROAS / Efficiency Target Documented',
    'M003': 'Daily Spend Target Set',
    'M004': 'Client Success Plan Completed',
    'M005': 'Client Journey Map Completed',
    'M006': 'Account Risk Status Up to Date',
    'M007': 'Gong Call Cadence — Last 90 Days',
    'M008': 'CSM Can Articulate DSP Strategy',
    'M009': 'CSM–Strategist Alignment Clear',
}

WHY = {
    'M001': 'An account without a documented objective will be optimized toward the last thing someone mentioned in a call. That is not strategy — it is reactive management that erodes client confidence.',
    'M002': 'Without a documented ROAS target, the client and team have no shared definition of what good looks like. Any ROAS becomes acceptable by default, which means underperformance can persist without being flagged.',
    'M003': 'An empty or inaccurate daily target means pacing issues go undetected until the end of the month. At that point, the options are overspend or a gap in coverage — both of which are avoidable.',
    'M004': 'A CSP that is incomplete or stale is not a CSP — it is a form that was filled out once and forgotten. When the CSP does not reflect reality, the whole account narrative breaks down during a review.',
    'M005': 'The Journey Map is what tells the team whether to push growth or protect retention. Without it, strategy decisions are made without a framework, leading to inconsistent execution across the account lifecycle.',
    'M006': 'An outdated risk flag in either direction is harmful. A false clean status means a struggling account gets no attention. A stale red flag creates unnecessary urgency for an account that already recovered.',
    'M007': '90 days without a call means two full reporting cycles passed without a documented strategy conversation. By then, market conditions, product catalog, and client priorities may all have shifted.',
    'M008': 'Manual review required during the QR call — CSM articulation quality cannot be checked from system data.',
    'M009': 'Manual review required during the QR call — CSM–strategist alignment cannot be checked from system data.',
}

SOURCES = {
    'M001': '14_DSP_Project_on_SF · CS_Notes__c + Target_ROAS__c',
    'M002': '14_DSP_Project_on_SF · Target_ROAS__c',
    'M003': '14_DSP_Project_on_SF · daily_target_spend__c',
    'M004': '13_Client_Success_Insights · Primary_Objective__c / Current_Challenges__c / Near_Term_3_Month_Considerations__c',
    'M005': '12_Client_Journey_Insights · StatusS1–S4 / StrategyS1–S4',
    'M006': '14_DSP_Project_on_SF · At_Risk__c + Risk_Reason_Notes__c',
    'M007': '11_Gong_Call_Insights · Gong__Call_Start__c',
    'M008': 'Manual review — QR presentation call',
    'M009': 'Manual review — QR presentation call',
}


@dataclass(frozen=True)
class ControlResult:
    status: str
    what:   str = ''
    why:    str = ''
    source: str = ''
