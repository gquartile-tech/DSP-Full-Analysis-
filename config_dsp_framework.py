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
    'F001':  8,
    'F002':  9,
    'F003':  3,
    'F004':  7,
    'F005':  9,
    'F006':  7,
    'F007':  6,
    'F008':  6,
    'F009': 10,
    'F010':  9,
    'F011':  9,
    'F012':  8,
    'F013':  7,
}

CONTROL_NAMES = {
    'F001': 'Order Naming Convention',
    'F002': 'Order Goal / KPI Alignment',
    'F003': 'Agency Fee Correctly Applied',
    'F004': 'Line Item Naming Convention',
    'F005': 'Audience / Contextual Targeting Aligned',
    'F006': 'Frequency Cap Set (1–5)',
    'F007': 'Viewability Setting Optimized',
    'F008': 'Device Targeting Matches Strategy',
    'F009': 'Merchant Token (PPC & DSP Overlap Tag)',
    'F010': 'AMC Entity Connected',
    'F011': 'All Active Orders Delivering',
    'F012': 'Line Item Delivery Balanced Across Orders',
    'F013': 'Add to Cart Rate Healthy (≥ 5% of DPV)',
}

WHY = {
    'F001': 'Orders that do not follow the naming standard make it impossible to quickly identify funnel coverage gaps or misallocated budget during a review.',
    'F002': 'A mismatched goal causes the DSP bidder to optimize for the wrong outcome, which inflates or deflates reported performance without reflecting real business results.',
    'F003': 'A fee set too low means Quartile is subsidizing the client\'s media cost. A fee set too high creates billing disputes and erodes client trust.',
    'F004': 'Line items without clear naming hide which audiences are spending, which are converting, and where budget concentration is building up unnoticed.',
    'F005': 'Targeting misalignment is one of the most common causes of wasted DSP spend. Retargeting audiences on awareness orders reach the wrong pool; broad contextual on retargeting orders miss intent signals entirely.',
    'F006': 'Uncapped or high-frequency delivery means the same users are seeing the same ad repeatedly. Past a threshold, additional impressions do not convert — they just increase cost and reduce brand perception.',
    'F007': 'Paying for impressions that are never seen is pure waste. Low viewability also distorts CTR and DPVR benchmarks, making performance appear worse than it is.',
    'F008': 'Unintentional all-device targeting on lower funnel orders dilutes spend across devices with lower purchase intent, increasing CPO and reducing ROAS.',
    'F009': 'Double-counting attributed sales between PPC and DSP distorts the actual efficiency of both channels and creates inaccurate reporting for the client.',
    'F010': 'Without AMC, audience overlap analysis, path-to-purchase reports, and custom segments built from first-party data are all unavailable.',
    'F011': 'An active order not delivering means budget is going unspent and funnel coverage has a hole. In a lower funnel order, this directly costs attributed revenue.',
    'F012': 'An account where one order absorbs 80%+ of budget is effectively a single-order strategy. If that order\'s audience exhausts or CPM rises, there is no fallback.',
    'F013': 'ATC rate below 5% on retargeting means users are visiting the PDP but not adding — typically a PDP issue, price positioning problem, or an exhausted retargeting audience.',
}

SOURCES = {
    'F001': '06_DSP_Order_Report · OrderName',
    'F002': '06_DSP_Order_Report · FunnelStage + 14_DSP_Project_on_SF · Target_ROAS__c',
    'F003': '15_Customer_Journey_Funnel_Segm · AgencyFee',
    'F004': '09_DSP_LineItem_Report · LineItemName',
    'F005': '09_DSP_LineItem_Report · Strategy + FunnelStage',
    'F006': '15_Customer_Journey_Funnel_Segm · LineItem level check',
    'F007': '15_Customer_Journey_Funnel_Segm · LineItem level check',
    'F008': '09_DSP_LineItem_Report · LineItemName (inferred)',
    'F009': 'Planilha CoE v2 — Advertiser: Merchant token',
    'F010': 'Planilha CoE v2 — AMC Entity: AMC Connection + Quartile Connection',
    'F011': '06_DSP_Order_Report · AdSpend / 15_Customer_Journey_Funnel_Segm',
    'F012': '06_DSP_Order_Report · AdSpend per order / total AdSpend',
    'F013': '02_DSP_Date_Range_KPIs · AddToCart / DPV',
}


@dataclass(frozen=True)
class ControlResult:
    status: str
    what:   str = ''
    why:    str = ''
    source: str = ''
