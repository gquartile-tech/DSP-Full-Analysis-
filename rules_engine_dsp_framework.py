from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from config_dsp_framework import (
    CONTROL_NAMES, IMPACT_LABEL, IMPORTANCE, PRIORITY_POINTS,
    SCORING_EXCLUDED, SOURCES, WHY, ControlResult,
    STATUS_OK, STATUS_PARTIAL, STATUS_FLAG,
)
from reader_databricks_dsp import DSPContext, clean_text, money_str, pct_str, to_float, trim, _find_col

# Naming convention signals — order names should contain at least one of these
FUNNEL_KEYWORDS = {
    'upper', 'lower', 'mid', 'awareness', 'prospecting', 'rmkt', 'retargeting',
    'performance', 'consideration', 'funnel', 'new', 'lapsed', 'loyal', 'engaged',
}
STRATEGY_KEYWORDS = {'rmkt', 'cat', 'comp', 'brand', 'category', 'lifestyle', 'contextual'}

# Line item naming signals
LI_AUDIENCE_KEYWORDS = {
    'retention', 'remarketing', 'lapsed', 'new', 'prospecting', 'in-market',
    'lifestyle', 'contextual', 'purchase', 'dpv', 'display', 'video',
}

# Funnel labels considered "upper" and "lower" for goal alignment
LOWER_FUNNEL_LABELS = {'lower funnel', 'retargeting', 'rmkt', 'performance'}
UPPER_FUNNEL_LABELS = {'upper funnel', 'awareness', 'prospecting'}


def _is_named(name: str, keyword_set: set) -> bool:
    n = name.strip().lower()
    return any(k in n for k in keyword_set)


def _pct_named(names, keyword_set) -> tuple:
    """Returns (count_valid, count_invalid, invalid_list)."""
    valid, invalid = 0, []
    for n in names:
        if _is_named(n, keyword_set):
            valid += 1
        else:
            invalid.append(n)
    return valid, len(invalid), invalid


def evaluate_all(ctx: DSPContext) -> Dict[str, ControlResult]:
    r: Dict[str, ControlResult] = {}

    # ─────────────────────────────────────────────────────────────────────
    # F001 — Order Naming Convention
    # ─────────────────────────────────────────────────────────────────────
    if ctx.df06 is None or ctx.df06.empty:
        r['F001'] = ControlResult(STATUS_PARTIAL, 'No order data available (06_DSP_Order_Report). Naming convention could not be checked.', WHY['F001'], SOURCES['F001'])
    else:
        name_col = _find_col(ctx.df06, ['OrderName'])
        if name_col is None:
            r['F001'] = ControlResult(STATUS_PARTIAL, 'OrderName column not found in 06_DSP_Order_Report.', WHY['F001'], SOURCES['F001'])
        else:
            names = [clean_text(v) for v in ctx.df06[name_col].dropna().tolist() if clean_text(v)]
            total = len(names)
            valid, invalid_count, invalid = _pct_named(names, FUNNEL_KEYWORDS | STRATEGY_KEYWORDS)
            if total == 0:
                r['F001'] = ControlResult(STATUS_PARTIAL, 'No active orders found.', WHY['F001'], SOURCES['F001'])
            elif invalid_count == 0:
                r['F001'] = ControlResult(STATUS_OK, f'All {total} orders follow the naming standard with identifiable funnel and strategy labels.', WHY['F001'], SOURCES['F001'])
            elif invalid_count / total <= 0.25:
                r['F001'] = ControlResult(STATUS_PARTIAL, f'{total} orders reviewed. {invalid_count} do not follow naming standard: {", ".join(invalid[:5])}.', WHY['F001'], SOURCES['F001'])
            else:
                r['F001'] = ControlResult(STATUS_FLAG, f'{total} orders reviewed. {invalid_count} do not follow naming standard: {", ".join(invalid[:5])}.', WHY['F001'], SOURCES['F001'])

    # ─────────────────────────────────────────────────────────────────────
    # F002 — Order Goal / KPI Alignment
    # ─────────────────────────────────────────────────────────────────────
    if ctx.df06 is None or ctx.df06.empty:
        r['F002'] = ControlResult(STATUS_PARTIAL, 'No order data available to check goal alignment.', WHY['F002'], SOURCES['F002'])
    else:
        funnel_col = _find_col(ctx.df06, ['FunnelStage'])
        name_col   = _find_col(ctx.df06, ['OrderName'])
        if funnel_col is None:
            r['F002'] = ControlResult(STATUS_PARTIAL, 'FunnelStage column not found in 06_DSP_Order_Report. Goal alignment cannot be verified.', WHY['F002'], SOURCES['F002'])
        else:
            identified = ctx.df06[ctx.df06[funnel_col].astype(str).str.lower().str.strip() != 'not identified']
            not_id     = ctx.df06[ctx.df06[funnel_col].astype(str).str.lower().str.strip() == 'not identified']
            not_id_names = [clean_text(v) for v in not_id[name_col].tolist()] if name_col else []
            total = len(ctx.df06)
            not_id_count = len(not_id)
            if not_id_count == 0:
                r['F002'] = ControlResult(STATUS_OK, f'All {total} orders have an identifiable funnel stage. No goal mismatches detected.', WHY['F002'], SOURCES['F002'])
            elif not_id_count / total <= 0.25:
                r['F002'] = ControlResult(STATUS_PARTIAL, f'{not_id_count} of {total} orders have FunnelStage = "Not Identified": {", ".join(not_id_names[:4])}. Review goal settings on these orders.', WHY['F002'], SOURCES['F002'])
            else:
                r['F002'] = ControlResult(STATUS_FLAG, f'{not_id_count} of {total} orders have FunnelStage = "Not Identified": {", ".join(not_id_names[:4])}. Goal alignment cannot be confirmed.', WHY['F002'], SOURCES['F002'])

    # ─────────────────────────────────────────────────────────────────────
    # F003 — Agency Fee Correctly Applied
    # ─────────────────────────────────────────────────────────────────────
    # df15 not available as separate sheet in the export — mark as manual
    r['F003'] = ControlResult(STATUS_OK, 'Agency fee check requires manual verification in the DSP console (15_Customer_Journey_Funnel_Segm not included in export). Confirm 9.75% is applied to all active orders.', WHY['F003'], SOURCES['F003'])

    # ─────────────────────────────────────────────────────────────────────
    # F004 — Line Item Naming Convention
    # ─────────────────────────────────────────────────────────────────────
    if ctx.df09 is None or ctx.df09.empty:
        r['F004'] = ControlResult(STATUS_PARTIAL, 'No line item data available (09_DSP_LineItem_Report).', WHY['F004'], SOURCES['F004'])
    else:
        li_name_col = _find_col(ctx.df09, ['LineItemName'])
        if li_name_col is None:
            r['F004'] = ControlResult(STATUS_PARTIAL, 'LineItemName column not found in 09_DSP_LineItem_Report.', WHY['F004'], SOURCES['F004'])
        else:
            names = [clean_text(v) for v in ctx.df09[li_name_col].dropna().unique().tolist() if clean_text(v)]
            total = len(names)
            valid, invalid_count, invalid = _pct_named(names, LI_AUDIENCE_KEYWORDS | FUNNEL_KEYWORDS)
            if total == 0:
                r['F004'] = ControlResult(STATUS_PARTIAL, 'No active line items found.', WHY['F004'], SOURCES['F004'])
            elif invalid_count == 0:
                r['F004'] = ControlResult(STATUS_OK, f'All {total} line items have audience or funnel stage identifiers in their names.', WHY['F004'], SOURCES['F004'])
            elif invalid_count / total <= 0.3:
                r['F004'] = ControlResult(STATUS_PARTIAL, f'{total} line items reviewed. {invalid_count} lack audience/funnel identifiers: {", ".join(invalid[:5])}.', WHY['F004'], SOURCES['F004'])
            else:
                r['F004'] = ControlResult(STATUS_FLAG, f'{total} line items reviewed. {invalid_count} do not follow the naming standard: {", ".join(invalid[:5])}.', WHY['F004'], SOURCES['F004'])

    # ─────────────────────────────────────────────────────────────────────
    # F005 — Audience / Contextual Targeting Aligned
    # ─────────────────────────────────────────────────────────────────────
    if ctx.df09 is None or ctx.df09.empty:
        r['F005'] = ControlResult(STATUS_PARTIAL, 'No line item data available for targeting alignment check.', WHY['F005'], SOURCES['F005'])
    else:
        strategy_col = _find_col(ctx.df09, ['Strategy'])
        funnel_col   = _find_col(ctx.df09, ['FunnelStage'])
        li_name_col  = _find_col(ctx.df09, ['LineItemName'])
        if strategy_col is None or funnel_col is None:
            r['F005'] = ControlResult(STATUS_PARTIAL, 'Strategy or FunnelStage column not found in 09_DSP_LineItem_Report. Targeting alignment cannot be checked automatically.', WHY['F005'], SOURCES['F005'])
        else:
            df = ctx.df09.copy()
            df['_strategy'] = df[strategy_col].astype(str).str.lower().str.strip()
            df['_funnel']   = df[funnel_col].astype(str).str.lower().str.strip()
            # Flag: RMKT strategy on upper funnel order
            mismatched = df[
                (df['_strategy'].str.contains('rmkt|retargeting', na=False)) &
                (df['_funnel'].str.contains('upper|awareness|prospecting', na=False))
            ]
            total = len(df)
            bad = len(mismatched)
            if bad == 0:
                r['F005'] = ControlResult(STATUS_OK, f'All {total} line items have targeting strategy consistent with their funnel stage.', WHY['F005'], SOURCES['F005'])
            else:
                bad_names = [clean_text(v) for v in mismatched[li_name_col].tolist()] if li_name_col else []
                r['F005'] = ControlResult(STATUS_FLAG, f'{bad} line item(s) use a retargeting strategy on an upper funnel order: {", ".join(bad_names[:4])}. This wastes impressions on already-converted users.', WHY['F005'], SOURCES['F005'])

    # ─────────────────────────────────────────────────────────────────────
    # F006 — Frequency Cap Set (manual — not in export)
    # ─────────────────────────────────────────────────────────────────────
    r['F006'] = ControlResult(STATUS_OK, 'Frequency cap check requires manual verification in the DSP console. Confirm all active line items have frequency caps set between 1–5.', WHY['F006'], SOURCES['F006'])

    # ─────────────────────────────────────────────────────────────────────
    # F007 — Viewability Setting Optimized (manual)
    # ─────────────────────────────────────────────────────────────────────
    r['F007'] = ControlResult(STATUS_OK, 'Viewability setting check requires manual verification in the DSP console. Confirm threshold meets the recommended standard on all line items.', WHY['F007'], SOURCES['F007'])

    # ─────────────────────────────────────────────────────────────────────
    # F008 — Device Targeting Matches Strategy (manual)
    # ─────────────────────────────────────────────────────────────────────
    r['F008'] = ControlResult(STATUS_OK, 'Device targeting check requires manual verification in the DSP console. Confirm conversion-focused line items are not using default all-device targeting.', WHY['F008'], SOURCES['F008'])

    # ─────────────────────────────────────────────────────────────────────
    # F009 — Merchant Token (manual — not in export)
    # ─────────────────────────────────────────────────────────────────────
    r['F009'] = ControlResult(STATUS_OK, 'Merchant token check requires manual verification in the DSP advertiser settings. Confirm the token is set to remove PPC/DSP attribution overlap.', WHY['F009'], SOURCES['F009'])

    # ─────────────────────────────────────────────────────────────────────
    # F010 — AMC Entity Connected (manual)
    # ─────────────────────────────────────────────────────────────────────
    r['F010'] = ControlResult(STATUS_OK, 'AMC entity connection requires manual verification. Confirm the AMC entity is linked and Quartile has access to run custom queries.', WHY['F010'], SOURCES['F010'])

    # ─────────────────────────────────────────────────────────────────────
    # F011 — All Active Orders Delivering
    # ─────────────────────────────────────────────────────────────────────
    if ctx.df06 is None or ctx.df06.empty:
        r['F011'] = ControlResult(STATUS_PARTIAL, 'No order data available to check delivery status.', WHY['F011'], SOURCES['F011'])
    else:
        spend_col = _find_col(ctx.df06, ['AdSpend'])
        name_col  = _find_col(ctx.df06, ['OrderName'])
        if spend_col is None:
            r['F011'] = ControlResult(STATUS_PARTIAL, 'AdSpend column not found in 06_DSP_Order_Report.', WHY['F011'], SOURCES['F011'])
        else:
            df = ctx.df06.copy()
            df['_spend'] = pd.to_numeric(df[spend_col], errors='coerce').fillna(0)
            zero_spend = df[df['_spend'] == 0]
            total = len(df)
            zero_count = len(zero_spend)
            if zero_count == 0:
                r['F011'] = ControlResult(STATUS_OK, f'All {total} orders recorded spend in the evaluation window. No delivery gaps detected.', WHY['F011'], SOURCES['F011'])
            else:
                zero_names = [clean_text(v) for v in zero_spend[name_col].tolist()] if name_col else []
                r['F011'] = ControlResult(STATUS_FLAG, f'{zero_count} of {total} orders have zero spend in the evaluation window: {", ".join(zero_names[:4])}. These orders may not be delivering.', WHY['F011'], SOURCES['F011'])

    # ─────────────────────────────────────────────────────────────────────
    # F012 — Line Item Delivery Balanced Across Orders
    # ─────────────────────────────────────────────────────────────────────
    if ctx.df06 is None or ctx.df06.empty:
        r['F012'] = ControlResult(STATUS_PARTIAL, 'No order data available to check budget concentration.', WHY['F012'], SOURCES['F012'])
    else:
        spend_col = _find_col(ctx.df06, ['AdSpend'])
        name_col  = _find_col(ctx.df06, ['OrderName'])
        if spend_col is None:
            r['F012'] = ControlResult(STATUS_PARTIAL, 'AdSpend column not found.', WHY['F012'], SOURCES['F012'])
        else:
            df = ctx.df06.copy()
            df['_spend'] = pd.to_numeric(df[spend_col], errors='coerce').fillna(0)
            total = df['_spend'].sum()
            if total == 0:
                r['F012'] = ControlResult(STATUS_PARTIAL, 'Total DSP spend is zero in the evaluation window. Budget concentration cannot be calculated.', WHY['F012'], SOURCES['F012'])
            else:
                df['_share'] = df['_spend'] / total * 100
                top = df.nlargest(1, '_spend').iloc[0]
                top_name = clean_text(top[name_col]) if name_col else 'Unknown order'
                top_share = float(top['_share'])
                if top_share >= 80:
                    r['F012'] = ControlResult(STATUS_FLAG, f'"{top_name}" accounts for {top_share:.0f}% of total DSP spend ({money_str(float(top["_spend"]))} of {money_str(total)}). Single-order concentration is too high.', WHY['F012'], SOURCES['F012'])
                elif top_share >= 65:
                    r['F012'] = ControlResult(STATUS_PARTIAL, f'"{top_name}" accounts for {top_share:.0f}% of total DSP spend. Concentration is elevated but within a manageable threshold.', WHY['F012'], SOURCES['F012'])
                else:
                    r['F012'] = ControlResult(STATUS_OK, f'Budget is spread across orders. Top order "{top_name}" accounts for {top_share:.0f}% of total spend ({money_str(total)} total).', WHY['F012'], SOURCES['F012'])

    # ─────────────────────────────────────────────────────────────────────
    # F013 — Add to Cart Rate Healthy (≥ 5% of DPV)
    # ─────────────────────────────────────────────────────────────────────
    atc = ctx.add_to_cart
    dpv = ctx.dpv
    if atc is None or dpv is None or dpv == 0:
        r['F013'] = ControlResult(STATUS_PARTIAL, f'Add-to-cart or DPV data not available in 02_DSP_Date_Range_KPIs. AddToCart: {atc}, DPV: {dpv}.', WHY['F013'], SOURCES['F013'])
    else:
        atc_rate = atc / dpv * 100
        msg = f'ATC rate: {atc_rate:.1f}% (AddToCart: {int(atc):,}, DPV: {int(dpv):,}). Benchmark: ≥5%.'
        if atc_rate >= 5:
            r['F013'] = ControlResult(STATUS_OK, msg, WHY['F013'], SOURCES['F013'])
        elif atc_rate >= 3:
            r['F013'] = ControlResult(STATUS_PARTIAL, msg + ' ATC rate is below 5% — review PDP quality and retargeting audience freshness.', WHY['F013'], SOURCES['F013'])
        else:
            r['F013'] = ControlResult(STATUS_FLAG, msg + ' ATC rate is critically low. Users are visiting the product page but not adding to cart.', WHY['F013'], SOURCES['F013'])

    return r


def compute_score(results: Dict[str, ControlResult]):
    findings, total_penalty = [], 0.0
    for cid, res in results.items():
        imp = IMPORTANCE[cid]
        pen = 0.0
        if cid not in SCORING_EXCLUDED:
            if res.status == STATUS_FLAG:
                pen = PRIORITY_POINTS[imp]
            elif res.status == STATUS_PARTIAL:
                pen = PRIORITY_POINTS[imp] * 0.5
        total_penalty += pen
        findings.append({'cid': cid, 'name': CONTROL_NAMES[cid], 'status': res.status,
                          'what': res.what, 'why': res.why,
                          'importance': imp, 'impact': IMPACT_LABEL[imp], 'penalty': pen})
    score = 100 + total_penalty
    grade = _grade(score)
    findings.sort(key=lambda x: (0 if x['status'] == STATUS_FLAG else 1, x['penalty']))
    return total_penalty, score, grade, findings


def _grade(score: float) -> str:
    if score >= 75:
        return 'Is Compliant'
    if score >= 40:
        return 'Need Improvement'
    return 'Non-Compliant'
