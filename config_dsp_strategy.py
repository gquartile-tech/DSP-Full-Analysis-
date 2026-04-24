from __future__ import annotations

from dataclasses import dataclass

STATUS_OK      = "OK"
STATUS_PARTIAL = "PARTIAL"
STATUS_FLAG    = "FLAG"

SCORING_EXCLUDED: set = set()

MAX_FINDINGS = 24

PRIORITY_POINTS = {10: -18, 9: -15, 8: -13, 7: -11, 6: -9, 5: -7, 4: -5, 3: -3, 2: -2, 1: 0}
IMPACT_LABEL    = {10: 'Critical', 9: 'High', 8: 'High', 7: 'Medium', 6: 'Medium', 5: 'Medium', 4: 'Low', 3: 'Low', 2: 'Visibility', 1: 'Visibility'}

IMPORTANCE = {
    'S001': 10,
    'S002':  8,
    'S003':  8,
    'S004':  9,
    'S005':  8,
}

CONTROL_NAMES = {
    'S001': 'Budget Split Aligned with Funnel Strategy',
    'S002': 'ROAS Target Achievable Given Funnel Mix',
    'S003': 'Best-Selling ASINs Are Promoted',
    'S004': 'NTB Purchase Rate Above 50%',
    'S005': 'DSP Budget Is Min 15% of PPC Spend',
}

WHY = {
    'S001': 'Budget allocation is the most direct expression of strategy. An account that says it is scaling but spends 85% on retargeting is not executing its strategy — it is spending on the easiest audience while ignoring the growth imperative.',
    'S002': 'An upper funnel order that runs at ROAS 1.5x is not failing — it is building the audience pool that the lower funnel will convert next month. Evaluating it against a 4x target makes every upper funnel investment look like a loss.',
    'S003': 'Budget on low-velocity products produces low ROAS and does not protect the category rank that drives organic sales. Every dollar on a slow-selling ASIN is a dollar not defending the account\'s best revenue drivers.',
    'S004': 'Upper funnel DSP that does not bring in new customers is just expensive reach against an existing base. The entire value proposition of awareness spend is audience expansion — if NTB rate is low, that value is not being delivered.',
    'S005': 'At less than 15% of PPC spend, DSP is operating as a side experiment rather than a core channel. Audience pools are too small to generate meaningful incremental impact and ROAS benchmarks become statistically unreliable.',
}

SOURCES = {
    'S001': '07_DSP_Spend_by_Strategy_&_Funn · AdSpend by FunnelStage',
    'S002': '07_DSP_Spend_by_Strategy_&_Funn · ROAS by FunnelStage + 14_DSP_Project_on_SF · Target_ROAS__c',
    'S003': '08_DSP_ASIN_Level_Report · ASIN + AdSpend cross vs best sellers',
    'S004': '02_DSP_Date_Range_KPIs · NTBRate + 07_DSP_Spend_by_Strategy_&_Funn · NTBPurchases',
    'S005': '10_DSP_vs_PPC_Comparison · AdSpendUSD (DSP) vs AdSpendUSD (PPC)',
}


@dataclass(frozen=True)
class ControlResult:
    status: str
    what:   str = ''
    why:    str = ''
    source: str = ''
