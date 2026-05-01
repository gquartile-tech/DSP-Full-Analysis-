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

ACTION = {
    'S001': {
        'FLAG':    'Bring the budget reallocation proposal to the next QR with numbers. Show the client exactly what % is currently going to each funnel layer, what the recommended split is, and what outcome you expect from the change.',
        'PARTIAL': 'Confirm with the strategist whether the current funnel split is intentional. If not, align on a rebalancing plan before the next review period.',
        'OK':      'Budget split is aligned with the stated strategy. Document it in QR notes as evidence of strategic execution.',
    },
    'S002': {
        'FLAG':    'Set a separate ROAS expectation for upper funnel orders in the client conversation. Blended ROAS will always look weak when upper funnel spend is high — the client needs to understand what each layer is supposed to deliver.',
        'PARTIAL': 'Review the attribution window setting with the strategist. A tighter window will suppress upper funnel ROAS artificially. If it needs adjustment, make the change and explain the impact to the client.',
        'OK':      'ROAS target is achievable given the current funnel mix. No action required.',
    },
    'S003': {
        'FLAG':    'Add the top-selling ASINs without DSP spend to an existing lower funnel order this week. Defending your best organic sellers with retargeting spend is the highest-ROI DSP action available for most accounts.',
        'PARTIAL': 'Review the unprotected top ASINs with the strategist and agree on which to add to a retargeting order. Prioritize by organic rank sensitivity.',
        'OK':      'Best-selling ASINs are covered by DSP. Review ASIN coverage quarterly as the product mix evolves.',
    },
    'S004': {
        'FLAG':    'Audit the upper funnel audiences currently in use. If NTB rate is critically low, the account is reaching existing buyers instead of new ones — the audience definition or exclusion logic needs to be rebuilt.',
        'PARTIAL': 'Review upper funnel audience segments with the strategist. Check whether purchase exclusions are applied correctly and whether the prospecting pool is large enough to drive genuine new customer acquisition.',
        'OK':      'NTB rate is above 50%. Highlight this in the QR as evidence that upper funnel spend is working.',
    },
    'S005': {
        'FLAG':    'Present the DSP-to-PPC ratio to the client and make the case for increasing DSP investment. At less than 8% of PPC spend, DSP cannot build meaningful audiences — it is running below the threshold where it can influence the funnel.',
        'PARTIAL': 'Include a DSP budget increase recommendation in the next QR. Show the client what a 15% ratio would look like in absolute spend terms and what outcomes it would enable.',
        'OK':      'DSP budget is at or above 15% of PPC spend. No action required.',
    },
}


@dataclass(frozen=True)
class ControlResult:
    status: str
    what:   str = ''
    why:    str = ''
    source: str = ''
    action: str = ''
