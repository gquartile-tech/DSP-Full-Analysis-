from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from config_dsp_health import (
    ACTION, CONTROL_NAMES, IMPACT_LABEL, IMPORTANCE, PRIORITY_POINTS,
    SCORING_EXCLUDED, SOURCES, WHY, ControlResult,
    STATUS_OK, STATUS_PARTIAL, STATUS_FLAG,
)
from reader_databricks_dsp import DSPContext, clean_text, money_str, roas_str, pct_str, to_float, trim, trend_direction


def _action(cid: str, status: str) -> str:
    """Return the correct action string for a control + status. Falls back gracefully."""
    ctrl_actions = ACTION.get(cid, {})
    return ctrl_actions.get(status, ctrl_actions.get('OK', ''))


def evaluate_all(ctx: DSPContext) -> Dict[str, ControlResult]:
    r: Dict[str, ControlResult] = {}

    # ─────────────────────────────────────────────────────────────────────
    # H001 — Primary KPI vs Target  (was: ROAS only)
    # Now adapts to primary_kpi: ROAS / ACOS / TACOS
    # ─────────────────────────────────────────────────────────────────────
    kpi = getattr(ctx, 'primary_kpi', 'ROAS').upper()

    if kpi == 'ACOS':
        target  = getattr(ctx, 'acos_constraint', None)
        # Get actual ACoS from df02
        actual  = None
        if ctx.df02 is not None and not ctx.df02.empty:
            from reader_databricks_dsp import _find_col
            acos_col = _find_col(ctx.df02, ['ACoS', 'acos'])
            if acos_col:
                actual = to_float(ctx.df02.iloc[0][acos_col])
            # Fallback: derive from ROAS
            if actual is None and ctx.roas and ctx.roas > 0:
                actual = 1 / ctx.roas * 100

        if target is None:
            st   = STATUS_PARTIAL
            what = f'Primary KPI: ACoS. ACoS constraint is not set in Salesforce. Actual ACoS: {f"{actual:.1f}%" if actual else "Not available"}. Cannot evaluate against target.'
        elif actual is None:
            st   = STATUS_PARTIAL
            what = f'Primary KPI: ACoS. Target: {target:.1f}%. Actual ACoS not available in 02_DSP_Date_Range_KPIs.'
        else:
            gap_pct = (actual - target) / target * 100  # positive = worse (over target)
            direction = 'above' if actual > target else 'below'
            what = f'Primary KPI: ACoS. Target: {target:.1f}%. Actual: {actual:.1f}%. Gap: {actual - target:+.1f}pp ({direction} target).'
            if actual <= target * 1.10:
                st = STATUS_OK
            elif actual <= target * 1.25:
                st = STATUS_PARTIAL
                what += ' ACoS is more than 10% above target.'
            else:
                st = STATUS_FLAG
                what += ' ACoS is more than 25% above target. Immediate strategy review required.'

    elif kpi == 'TACOS':
        target = getattr(ctx, 'tacos_constraint', None)
        actual = None
        if ctx.df02 is not None and not ctx.df02.empty:
            from reader_databricks_dsp import _find_col
            tacos_col = _find_col(ctx.df02, ['TACoS', 'tacos'])
            if tacos_col:
                actual = to_float(ctx.df02.iloc[0][tacos_col])

        if target is None:
            st   = STATUS_PARTIAL
            what = f'Primary KPI: TACoS. TACoS constraint is not set in Salesforce. Actual TACoS: {f"{actual:.1f}%" if actual else "Not available"}.'
        elif actual is None:
            st   = STATUS_PARTIAL
            what = f'Primary KPI: TACoS. Target: {target:.1f}%. Actual TACoS not available in 02_DSP_Date_Range_KPIs.'
        else:
            what = f'Primary KPI: TACoS. Target: {target:.1f}%. Actual: {actual:.1f}%. Gap: {actual - target:+.1f}pp.'
            if actual <= target * 1.10:
                st = STATUS_OK
            elif actual <= target * 1.25:
                st = STATUS_PARTIAL
                what += ' TACoS is more than 10% above target.'
            else:
                st = STATUS_FLAG
                what += ' TACoS is more than 25% above target. Immediate strategy review required.'

    else:  # ROAS (default)
        target = ctx.target_roas
        # Also check Target_ACoS__c as ROAS equivalent if target_roas missing
        if target is None:
            tacos_t = getattr(ctx, 'target_tacos', None)
            acos_t  = getattr(ctx, 'acos_constraint', None)

        if target is None:
            st   = STATUS_PARTIAL
            what = f'Primary KPI: ROAS. Target ROAS is not set in Salesforce. Current ROAS: {roas_str(ctx.roas)}. Cannot evaluate performance against target.'
        elif ctx.roas is None:
            st   = STATUS_PARTIAL
            what = f'Primary KPI: ROAS. Target: {target:.2f}x. Current ROAS not available in 02_DSP_Date_Range_KPIs.'
        else:
            gap_pct = (ctx.roas - target) / target * 100
            what = f'Primary KPI: ROAS. Target: {target:.2f}x. Actual: {ctx.roas:.2f}x. Gap: {gap_pct:+.1f}%.'
            if gap_pct >= -10:
                st = STATUS_OK
            elif gap_pct >= -25:
                st = STATUS_PARTIAL
                what += ' ROAS is more than 10% below target.'
            else:
                st = STATUS_FLAG
                what += ' ROAS is more than 25% below target. Immediate strategy review required.'

    r['H001'] = ControlResult(st, what, WHY['H001'], SOURCES['H001'], _action('H001', st))

    # ─────────────────────────────────────────────────────────────────────
    # H002 — Ad Spend Pacing vs Budget Target
    # Fixed: uses ctx.window_days instead of hardcoded 30
    # ─────────────────────────────────────────────────────────────────────
    if ctx.daily_target is None or ctx.daily_target == 0:
        st   = STATUS_PARTIAL
        what = f'Daily spend target is not set in Salesforce. Actual L{ctx.window_days or 30}d spend: {money_str(ctx.ad_spend)}. Pacing cannot be evaluated.'
    elif ctx.ad_spend is None or not ctx.window_days:
        st   = STATUS_PARTIAL
        what = f'Daily spend target: {money_str(ctx.daily_target)}/day. Actual spend data not available.'
    else:
        period_target = ctx.daily_target * ctx.window_days
        dev_pct = (ctx.ad_spend - period_target) / period_target * 100
        direction = 'above' if dev_pct > 0 else 'below'
        what = (f'Period target: {money_str(period_target)} ({money_str(ctx.daily_target)}/day × {ctx.window_days}d). '
                f'Actual spend: {money_str(ctx.ad_spend)}. '
                f'Deviation: {abs(dev_pct):.1f}% {direction} target.')
        if abs(dev_pct) <= 10:
            st = STATUS_OK
        elif abs(dev_pct) <= 25:
            st = STATUS_PARTIAL
        else:
            st = STATUS_FLAG
            what += ' Pacing deviation exceeds 25%. Investigate delivery or budget cap settings.'

    r['H002'] = ControlResult(st, what, WHY['H002'], SOURCES['H002'], _action('H002', st))

    # ─────────────────────────────────────────────────────────────────────
    # H003 — YoY Ad Sales Growth  (now uses df05 when available)
    # ─────────────────────────────────────────────────────────────────────
    yoy_sales = _get_yoy_from_df05(ctx, 'AdSales') or ctx.yoy_sales_growth

    if yoy_sales is None:
        st   = STATUS_PARTIAL
        what = 'YoY ad sales data not available. Prior year data may not yet exist for this account.'
    else:
        pct_display = f'{yoy_sales * 100:+.1f}%' if abs(yoy_sales) <= 10 else f'{yoy_sales:+.1f}%'
        # df05 stores as decimal fraction; df04 stores as % already — normalise
        display_val = yoy_sales * 100 if abs(yoy_sales) <= 1 else yoy_sales
        what = f'YoY Ad Sales growth: {display_val:+.1f}%.'
        if display_val >= 0:
            st = STATUS_OK
            what += ' Ad sales are growing year over year.'
        elif display_val >= -15:
            st = STATUS_PARTIAL
            what += ' Ad sales declined slightly vs prior year.'
        else:
            st = STATUS_FLAG
            what += ' Significant YoY decline. Diagnose whether this is audience saturation, product, or budget-driven.'

    r['H003'] = ControlResult(st, what, WHY['H003'], SOURCES['H003'], _action('H003', st))

    # ─────────────────────────────────────────────────────────────────────
    # H004 — YoY Ad Spend Growth  (now uses df05 when available)
    # ─────────────────────────────────────────────────────────────────────
    yoy_spend = _get_yoy_from_df05(ctx, 'AdSpend') or ctx.yoy_spend_growth

    if yoy_spend is None:
        st   = STATUS_PARTIAL
        what = 'YoY ad spend data not available.'
    else:
        display_val = yoy_spend * 100 if abs(yoy_spend) <= 1 else yoy_spend
        obj = trim(ctx.primary_objective, 80) or 'Not documented'
        what = f'YoY Ad Spend growth: {display_val:+.1f}%. Strategy objective: "{obj}".'
        if display_val >= 0:
            st = STATUS_OK
        else:
            st = STATUS_PARTIAL
            what += ' Spend is declining vs prior year. Confirm this is intentional based on the current strategy phase.'

    r['H004'] = ControlResult(st, what, WHY['H004'], SOURCES['H004'], _action('H004', st))

    # ─────────────────────────────────────────────────────────────────────
    # H005 — CTR Trend (MoM)  — Fixed: declining → PARTIAL
    # ─────────────────────────────────────────────────────────────────────
    valid_ctr = [v for v in ctx.ctr_trend if v is not None]
    if len(valid_ctr) < 2:
        st   = STATUS_OK
        what = 'Insufficient monthly CTR data to calculate trend (fewer than 2 months available).'
    else:
        direction = trend_direction(ctx.ctr_trend)
        trend_str = ' → '.join([f'{v:.3f}%' if v is not None else 'N/A' for v in ctx.ctr_trend])
        what = f'CTR trend (last 3 months): {trend_str}. Direction: {direction}.'
        st = STATUS_PARTIAL if direction == 'declining' else STATUS_OK

    r['H005'] = ControlResult(st, what, WHY['H005'], SOURCES['H005'], _action('H005', st))

    # ─────────────────────────────────────────────────────────────────────
    # H006 — CVR Trend (MoM)  — Fixed: declining → PARTIAL
    # ─────────────────────────────────────────────────────────────────────
    valid_cvr = [v for v in ctx.cvr_trend if v is not None]
    if len(valid_cvr) < 2:
        st   = STATUS_OK
        what = 'Insufficient monthly CVR data to calculate trend.'
    else:
        direction = trend_direction(ctx.cvr_trend)
        trend_str = ' → '.join([f'{v:.2f}' if v is not None else 'N/A' for v in ctx.cvr_trend])
        what = f'CVR trend (last 3 months): {trend_str}. Direction: {direction}.'
        st = STATUS_PARTIAL if direction == 'declining' else STATUS_OK

    r['H006'] = ControlResult(st, what, WHY['H006'], SOURCES['H006'], _action('H006', st))

    # ─────────────────────────────────────────────────────────────────────
    # H007 — CPM Trend (MoM)  — Fixed: rising → PARTIAL (higher CPM = worse)
    # ─────────────────────────────────────────────────────────────────────
    valid_cpm = [v for v in ctx.cpm_trend if v is not None]
    if len(valid_cpm) < 2:
        st   = STATUS_OK
        what = 'Insufficient monthly CPM data to calculate trend.'
    else:
        direction = trend_direction(ctx.cpm_trend)
        trend_str = ' → '.join([f'${v:.2f}' if v is not None else 'N/A' for v in ctx.cpm_trend])
        what = f'CPM trend (last 3 months): {trend_str}. Direction: {direction}.'
        # CPM rising is the concern — flag it as PARTIAL
        st = STATUS_PARTIAL if direction == 'improving' else STATUS_OK
        # Note: trend_direction returns 'improving' when values go up (higher CPM = bad)
        # Re-evaluate: 'improving' in trend_direction means values increased — for CPM that's bad
        if direction == 'declining':   # CPM going down = good
            st = STATUS_OK
        elif direction == 'improving': # CPM going up = concern
            st = STATUS_PARTIAL
            what += ' Rising CPM reduces inventory efficiency. Review bid strategy.'
        else:
            st = STATUS_OK

    r['H007'] = ControlResult(st, what, WHY['H007'], SOURCES['H007'], _action('H007', st))

    # ─────────────────────────────────────────────────────────────────────
    # H008 — DPVR Stable or Improving  — Fixed: declining → PARTIAL
    # ─────────────────────────────────────────────────────────────────────
    if ctx.dpv is not None and ctx.impressions is not None and ctx.impressions > 0:
        current_dpvr = ctx.dpv / ctx.impressions * 100
        valid_dpvr = [v for v in ctx.dpvr_trend if v is not None]
        direction = trend_direction(ctx.dpvr_trend) if len(valid_dpvr) >= 2 else 'insufficient data'
        what = f'Current DPVR: {current_dpvr:.3f}% (DPV: {int(ctx.dpv):,}, Impressions: {int(ctx.impressions):,}). MoM trend: {direction}.'
        st = STATUS_PARTIAL if direction == 'declining' else STATUS_OK
        if direction == 'declining':
            what += ' Declining DPVR signals creative or product relevance issues.'
    else:
        st   = STATUS_OK
        what = 'DPVR data not available in 02_DSP_Date_Range_KPIs.'

    r['H008'] = ControlResult(st, what, WHY['H008'], SOURCES['H008'], _action('H008', st))

    # ─────────────────────────────────────────────────────────────────────
    # H014 — GGS Commitment  — Now uses Amazon_GGS__c from sheet 13
    # ─────────────────────────────────────────────────────────────────────
    ggs_enrolled = _get_ggs_enrolled(ctx)

    if ggs_enrolled is None:
        st   = STATUS_PARTIAL
        what = 'GGS enrollment status not found in 13_Client_Success_Insights (Amazon_GGS__c). Verify manually.'
    elif not ggs_enrolled:
        st   = STATUS_FLAG
        what = 'Account is not enrolled in GGS. Non-enrolled accounts do not contribute to the media commitment and may not have access to preferred inventory pricing.'
    else:
        st   = STATUS_OK
        what = 'Account is enrolled in GGS (Amazon_GGS__c = True). Verify spend trajectory is on pace with the GGS commitment target during the next strategist sync.'

    r['H014'] = ControlResult(st, what, WHY['H014'], SOURCES['H014'], _action('H014', st))

    # ─────────────────────────────────────────────────────────────────────
    # H016 — Financial Status (still manual — no Stripe sheet in DSP export)
    # ─────────────────────────────────────────────────────────────────────
    r['H016'] = ControlResult(
        STATUS_OK,
        'Financial status requires manual verification in Stripe. Confirm no overdue DSP invoices or disputed payments before the QR.',
        WHY['H016'], SOURCES['H016'],
        _action('H016', STATUS_OK),
    )

    # ─────────────────────────────────────────────────────────────────────
    # H017 — Churn Score Reviewed
    # ─────────────────────────────────────────────────────────────────────
    churn      = clean_text(ctx.churn_risk).strip() if ctx.churn_risk else ''
    risk_score = ctx.account_risk_score
    score_str  = str(risk_score) if risk_score is not None else 'Not set'

    if not churn and risk_score is None:
        st   = STATUS_PARTIAL
        what = 'ChurnZero risk field (CSM_Churn_Risk__c) and Account_Risk_Score__c are both empty. Churn risk cannot be assessed.'
    else:
        risk_lower = churn.lower()
        if 'high' in risk_lower:
            st   = STATUS_FLAG
            what = f'Churn risk: {churn}. Risk score: {score_str}. High churn risk requires immediate action — review all health indicators and escalate.'
        elif 'medium' in risk_lower or 'mod' in risk_lower:
            st   = STATUS_PARTIAL
            what = f'Churn risk: {churn}. Risk score: {score_str}. Monitor closely and address the primary drivers before the next QR.'
        else:
            st   = STATUS_OK
            what = f'Churn risk: {churn}. Risk score: {score_str}.'

    r['H017'] = ControlResult(st, what, WHY['H017'], SOURCES['H017'], _action('H017', st))

    return r


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_yoy_from_df05(ctx: DSPContext, metric: str) -> Optional[float]:
    """
    Pull YoY value from df05 (05_DSP_Yearly_KPIs) by metric name.
    df05 has columns: metric, ThisPeriod, PreviousPeriod, YoY
    Returns the YoY decimal fraction (e.g. -0.43 for -43%) or None.
    """
    if ctx.df05 is None or ctx.df05.empty:
        return None
    from reader_databricks_dsp import _find_col
    metric_col = _find_col(ctx.df05, ['metric'])
    yoy_col    = _find_col(ctx.df05, ['YoY'])
    if metric_col is None or yoy_col is None:
        return None
    mask = ctx.df05[metric_col].astype(str).str.lower().str.strip() == metric.lower()
    matches = ctx.df05[mask]
    if matches.empty:
        return None
    val = to_float(matches.iloc[0][yoy_col])
    return val


def _get_ggs_enrolled(ctx: DSPContext) -> Optional[bool]:
    """Return True/False/None for Amazon_GGS__c from df13."""
    if ctx.df13 is None or ctx.df13.empty:
        return None
    from reader_databricks_dsp import _find_col, _row_val, _latest_row_by_modstamp
    row = _latest_row_by_modstamp(ctx.df13)
    val = _row_val(row, ['Amazon_GGS__c'])
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in {'true', '1', 'yes'}:
        return True
    if s in {'false', '0', 'no'}:
        return False
    return None


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
        findings.append({
            'cid': cid, 'name': CONTROL_NAMES[cid], 'status': res.status,
            'what': res.what, 'why': res.why, 'action': res.action,
            'importance': imp, 'impact': IMPACT_LABEL[imp], 'penalty': pen,
        })
    score = 100 + total_penalty
    grade = _grade(score)
    findings.sort(key=lambda x: (0 if x['status'] == STATUS_FLAG else 1, x['penalty']))
    return total_penalty, score, grade, findings


def _grade(score: float) -> str:
    if score >= 75:
        return 'Healthy'
    if score >= 40:
        return 'Needs Attention'
    return 'At Risk'
