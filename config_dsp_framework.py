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
    'F003': '15_Customer_Journey_Funnel_Segm · AgencyFee / TotalCost',
    'F004': '09_DSP_LineItem_Report · LineItemName',
    'F005': '09_DSP_LineItem_Report · Strategy + FunnelStage',
    'F006': '15_Customer_Journey_Funnel_Segm · LineItem level check',
    'F007': '15_Customer_Journey_Funnel_Segm · ViewableImpressions / Impressions',
    'F008': '09_DSP_LineItem_Report · LineItemName (inferred)',
    'F009': 'Planilha CoE v2 — Advertiser: Merchant token',
    'F010': 'Planilha CoE v2 — AMC Entity: AMC Connection + Quartile Connection',
    'F011': '06_DSP_Order_Report · AdSpend / 15_Customer_Journey_Funnel_Segm',
    'F012': '06_DSP_Order_Report · AdSpend per order / total AdSpend',
    'F013': '02_DSP_Date_Range_KPIs · AddToCart / DPV',
}

ACTION = {
    'F001': {
        'FLAG':    'Rename the flagged orders following the standard (funnel stage + strategy type) before the next QR. Brief the strategist so they can confirm the naming matches the actual campaign intent.',
        'PARTIAL': 'Fix the non-compliant order names this week. Consistent naming is a prerequisite for any funnel coverage analysis during a QR.',
        'OK':      'Naming convention is compliant. No action required.',
    },
    'F002': {
        'FLAG':    'Open each flagged order in the DSP console and confirm the goal setting matches its funnel stage. Misaligned goals are actively harming bid optimization — this needs to be corrected before the next delivery cycle.',
        'PARTIAL': 'Review the flagged orders with the strategist. If the FunnelStage label is wrong in the export, correct it in the DSP console. If the goal is wrong, fix it immediately.',
        'OK':      'Goal alignment is confirmed. No action required.',
    },
    'F003': {
        'FLAG':    'Identify which orders have the incorrect fee applied and escalate to the DSP operations team to correct it in the console. Document the correction and confirm with the client if billing is affected.',
        'PARTIAL': 'Flag the fee discrepancy to the DSP operations team. Verify whether the variance is a rounding issue or a genuine misconfiguration before escalating to the client.',
        'OK':      'Agency fee is correctly applied. No action required.',
    },
    'F004': {
        'FLAG':    'Rename the flagged line items to include the audience type and funnel stage. Without this, budget concentration analysis during a QR is impossible — you cannot tell which segment is spending.',
        'PARTIAL': 'Update the non-compliant line item names this week. Focus first on active line items with the highest spend.',
        'OK':      'Line item naming is compliant. No action required.',
    },
    'F005': {
        'FLAG':    'Pause the misaligned line items immediately and rebuild them with the correct audience strategy for their funnel stage. Retargeting audiences on upper funnel orders are wasting budget on already-converted users.',
        'PARTIAL': 'Review the flagged line items with the strategist. Confirm the intended targeting strategy and update the line item settings before the next delivery cycle.',
        'OK':      'Targeting alignment is confirmed. No action required.',
    },
    'F006': {
        'FLAG':    'Check frequency caps on all active line items in the DSP console and apply a 1–5 cap where missing. Uncapped delivery is burning budget on the same users without incremental conversion.',
        'PARTIAL': 'Verify frequency caps with the strategist, especially on lower funnel line items where over-exposure is most costly.',
        'OK':      'Frequency caps are set. No action required.',
    },
    'F007': {
        'FLAG':    'Review viewability settings in the DSP console. Low viewability means budget is being spent on placements users never see — tighten inventory targeting or increase the viewability threshold.',
        'PARTIAL': 'Discuss the viewability rate with the strategist. If below 50%, consider adjusting bid or supply source settings before the next campaign cycle.',
        'OK':      'Viewability is at an acceptable level. No action required.',
    },
    'F008': {
        'FLAG':    'Check device targeting on lower funnel line items in the DSP console. All-device targeting on conversion-focused orders dilutes spend across low-intent devices. Restrict to desktop or mobile based on the account\'s conversion data.',
        'PARTIAL': 'Confirm with the strategist whether all-device targeting is intentional on the flagged line items. Document the decision if it is.',
        'OK':      'Device targeting is appropriate. No action required.',
    },
    'F009': {
        'FLAG':    'Escalate merchant token configuration to the DSP operations team immediately. Until the overlap tag is set, PPC and DSP are double-counting attributed sales, making both channels appear more efficient than they are.',
        'OK':      'Merchant token is set. No action required.',
    },
    'F010': {
        'FLAG':    'Raise AMC connection with the DSP strategist as a priority item for the next QR. Without AMC, audience overlap analysis and path-to-purchase reporting are unavailable — capabilities the client is paying for.',
        'OK':      'AMC is connected. No action required.',
    },
    'F011': {
        'FLAG':    'Investigate why the flagged orders are not delivering. Check for exhausted budgets, paused line items, audience size issues, or bid floors. Restore delivery before raising it with the client.',
        'OK':      'All orders are delivering. No action required.',
    },
    'F012': {
        'FLAG':    'Discuss budget rebalancing with the strategist before the next QR. A single order absorbing 80%+ of spend is not a funnel strategy — it is single-point-of-failure execution. Identify which other funnel layers need investment.',
        'PARTIAL': 'Flag the concentration to the strategist and confirm it is intentional. If not, redistribute budget across funnel layers before the next review.',
        'OK':      'Budget is distributed across orders. No action required.',
    },
    'F013': {
        'FLAG':    'Review the product detail page for the flagged ASINs — check images, copy, pricing, and review count. If the PDP is strong, the retargeting audience may be exhausted and needs to be refreshed.',
        'PARTIAL': 'Monitor ATC rate weekly. If it does not recover within two weeks, escalate to the strategist to diagnose whether it is a PDP, pricing, or audience issue.',
        'OK':      'ATC rate is healthy. No action required.',
    },
}


@dataclass(frozen=True)
class ControlResult:
    status: str
    what:   str = ''
    why:    str = ''
    source: str = ''
    action: str = ''
