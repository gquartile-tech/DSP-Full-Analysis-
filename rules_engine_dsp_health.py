from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from config_dsp_health import (
    CONTROL_NAMES, IMPACT_LABEL, IMPORTANCE, PRIORITY_POINTS,
    SCORING_EXCLUDED, SOURCES, WHY, ControlResult,
    STATUS_OK, STATUS_PARTIAL, STATUS_FLAG,
)
from reader_databricks_dsp import DSPContext, clean_text, money_str, roas_str, pct_str, to_float, trim, trend_direction


def evaluate_all(ctx: DSPContext) -> Dict[str, ControlResult]:
    r: Dict[str, ControlResult] = {}

    # ─────────────────────────────────────────────────────────────────────
    # H001 — Total ROAS vs Target
    # ─────────────────────────────────────────────────────────────────────
    if ctx.target_roas is None:
        r['H001'] = ControlResult(STATUS_PARTIAL, f'Target ROAS is not set in Salesforce. Current ROAS: {roas_str(ctx.roas)}. Cannot evaluate performance against target.', WHY['H001'], SOURCES['H001'])
    elif ctx.roas is None:
        r['H001'] = ControlResult(STATUS_PARTIAL, f'Target ROAS: {ctx.target_roas:.2f}x. Current ROAS data not available in 02_DSP_Date_Range_KPIs.', WHY['H001'], SOURCES['H001'])
    else:
        gap_pct = (ctx.roas - ctx.target_roas) / ctx.target_roas * 100
        msg = f'Target ROAS: {ctx.target_roas:.2f}x. Current ROAS: {ctx.roas:.2f}x. Gap: {gap_pct:+.1f}%.'
        if gap_pct >= -10:
            r['H001'] = ControlResult(STATUS_OK, msg, WHY['H001'], SOURCES['H001'])
        elif gap_pct >= -25:
            r['H001'] = ControlResult(STATUS_PARTIAL, msg + ' ROAS is more than 10% below target.', WHY['H001'], SOURCES['H001'])
        else:
            r['H001'] = ControlResult(STATUS_FLAG, msg + ' ROAS is more than 25% below target. Immediate strategy review required.', WHY['H001'], SOURCES['H001'])

    # ─────────────────────────────────────────────────────────────────────
    # H002 — Ad Spend Pacing vs Budget Target
    # ─────────────────────────────────────────────────────────────────────
    if ctx.daily_target is None or ctx.daily_target == 0:
        r['H002'] = ControlResult(STATUS_PARTIAL, f'Daily spend target is not set in Salesforce. Actual L{ctx.window_days or 30} spend: {money_str(ctx.ad_spend)}. Pacing cannot be evaluated.', WHY['H002'], SOURCES['H002'])
    elif ctx.ad_spend is None or ctx.window_days is None:
        r['H002'] = ControlResult(STATUS_PARTIAL, f'Daily spend target: {money_str(ctx.daily_target)}/day. Actual spend data not available.', WHY['H002'], SOURCES['H002'])
    else:
        monthly_target = ctx.daily_target * 30
        dev_pct = (ctx.ad_spend - monthly_target) / monthly_target * 100
        direction = 'above' if dev_pct > 0 else 'below'
        msg = (f'Monthly spend target: {money_str(monthly_target)} ({money_str(ctx.daily_target)}/day × 30). '
               f'Actual L{ctx.window_days} spend: {money_str(ctx.ad_spend)}. '
               f'Deviation: {abs(dev_pct):.1f}% {direction} target.')
        if abs(dev_pct) <= 10:
            r['H002'] = ControlResult(STATUS_OK, msg, WHY['H002'], SOURCES['H002'])
        elif abs(dev_pct) <= 25:
            r['H002'] = ControlResult(STATUS_PARTIAL, msg, WHY['H002'], SOURCES['H002'])
        else:
            r['H002'] = ControlResult(STATUS_FLAG, msg + ' Pacing deviation exceeds 25%. Investigate delivery or budget cap settings.', WHY['H002'], SOURCES['H002'])

    # ─────────────────────────────────────────────────────────────────────
    # H003 — YoY Ad Sales Growth
    # ─────────────────────────────────────────────────────────────────────
    if ctx.yoy_sales_growth is None:
        r['H003'] = ControlResult(STATUS_PARTIAL, 'YoY ad sales data not available in 04_DSP_Monthly_YoY_Comparison. Prior year data may not yet exist for this account.', WHY['H003'], SOURCES['H003'])
    else:
        msg = f'YoY Ad Sales growth (latest month): {ctx.yoy_sales_growth:+.1f}%.'
        if ctx.yoy_sales_growth >= 0:
            r['H003'] = ControlResult(STATUS_OK, msg + ' Ad sales are growing year over year.', WHY['H003'], SOURCES['H003'])
        elif ctx.yoy_sales_growth >= -15:
            r['H003'] = ControlResult(STATUS_PARTIAL, msg + ' Ad sales declined slightly vs prior year.', WHY['H003'], SOURCES['H003'])
        else:
            r['H003'] = ControlResult(STATUS_FLAG, msg + ' Significant YoY decline. Diagnose whether this is audience saturation, product, or budget-driven.', WHY['H003'], SOURCES['H003'])

    # ─────────────────────────────────────────────────────────────────────
    # H004 — YoY Ad Spend Growth
    # ─────────────────────────────────────────────────────────────────────
    if ctx.yoy_spend_growth is None:
        r['H004'] = ControlResult(STATUS_PARTIAL, 'YoY ad spend data not available in 04_DSP_Monthly_YoY_Comparison.', WHY['H004'], SOURCES['H004'])
    else:
        obj = trim(ctx.primary_objective, 80) or 'Not documented'
        msg = f'YoY Ad Spend growth (latest month): {ctx.yoy_spend_growth:+.1f}%. Strategy objective: "{obj}".'
        if ctx.yoy_spend_growth >= 0:
            r['H004'] = ControlResult(STATUS_OK, msg, WHY['H004'], SOURCES['H004'])
        else:
            r['H004'] = ControlResult(STATUS_PARTIAL, msg + ' Spend is declining vs prior year. Confirm this is intentional based on the current strategy phase.', WHY['H004'], SOURCES['H004'])

    # ─────────────────────────────────────────────────────────────────────
    # H005 — CTR Trend (MoM) — Visibility only
    # ─────────────────────────────────────────────────────────────────────
    if len([v for v in ctx.ctr_trend if v is not None]) < 2:
        r['H005'] = ControlResult(STATUS_OK, 'Insufficient monthly CTR data to calculate trend (fewer than 2 months available).', WHY['H005'], SOURCES['H005'])
    else:
        vals = [v for v in ctx.ctr_trend if v is not None]
        direction = trend_direction(ctx.ctr_trend)
        trend_str = ' → '.join([f'{v:.3f}%' if v is not None else 'N/A' for v in ctx.ctr_trend])
        r['H005'] = ControlResult(STATUS_OK, f'CTR trend (last 3 months): {trend_str}. Direction: {direction}.', WHY['H005'], SOURCES['H005'])

    # ─────────────────────────────────────────────────────────────────────
    # H006 — CVR Trend (MoM) — Visibility only
    # ─────────────────────────────────────────────────────────────────────
    if len([v for v in ctx.cvr_trend if v is not None]) < 2:
        r['H006'] = ControlResult(STATUS_OK, 'Insufficient monthly CVR data to calculate trend.', WHY['H006'], SOURCES['H006'])
    else:
        direction = trend_direction(ctx.cvr_trend)
        trend_str = ' → '.join([f'{v:.1f}' if v is not None else 'N/A' for v in ctx.cvr_trend])
        r['H006'] = ControlResult(STATUS_OK, f'CVR trend (last 3 months): {trend_str}. Direction: {direction}.', WHY['H006'], SOURCES['H006'])

    # ─────────────────────────────────────────────────────────────────────
    # H007 — CPM Trend (MoM) — Visibility only
    # ─────────────────────────────────────────────────────────────────────
    if len([v for v in ctx.cpm_trend if v is not None]) < 2:
        r['H007'] = ControlResult(STATUS_OK, 'Insufficient monthly CPM data to calculate trend.', WHY['H007'], SOURCES['H007'])
    else:
        direction = trend_direction(ctx.cpm_trend)
        trend_str = ' → '.join([f'${v:.2f}' if v is not None else 'N/A' for v in ctx.cpm_trend])
        r['H007'] = ControlResult(STATUS_OK, f'CPM trend (last 3 months): {trend_str}. Direction: {direction}.', WHY['H007'], SOURCES['H007'])

    # ─────────────────────────────────────────────────────────────────────
    # H008 — DPVR Stable or Improving — Visibility only
    # ─────────────────────────────────────────────────────────────────────
    if ctx.dpv is not None and ctx.impressions is not None and ctx.impressions > 0:
        current_dpvr = ctx.dpv / ctx.impressions * 100
        direction = trend_direction(ctx.dpvr_trend) if len([v for v in ctx.dpvr_trend if v is not None]) >= 2 else 'insufficient data'
        r['H008'] = ControlResult(STATUS_OK, f'Current DPVR: {current_dpvr:.3f}% (DPV: {int(ctx.dpv):,}, Impressions: {int(ctx.impressions):,}). MoM trend: {direction}.', WHY['H008'], SOURCES['H008'])
    else:
        r['H008'] = ControlResult(STATUS_OK, 'DPVR data not available in 02_DSP_Date_Range_KPIs.', WHY['H008'], SOURCES['H008'])

    # ─────────────────────────────────────────────────────────────────────
    # H014 — GGS Commitment On Track (manual)
    # ─────────────────────────────────────────────────────────────────────
    r['H014'] = ControlResult(STATUS_OK, 'GGS commitment check requires manual verification against the Amazon media commitment target. Confirm current spend trajectory is on pace to meet the agreed GGS for this period.', WHY['H014'], SOURCES['H014'])

    # ─────────────────────────────────────────────────────────────────────
    # H016 — Financial Status Clear (manual)
    # ─────────────────────────────────────────────────────────────────────
    r['H016'] = ControlResult(STATUS_OK, 'Financial status check requires manual verification in Stripe/billing system. Confirm no overdue DSP invoices or disputed payments.', WHY['H016'], SOURCES['H016'])

    # ─────────────────────────────────────────────────────────────────────
    # H017 — Churn Score Reviewed
    # ─────────────────────────────────────────────────────────────────────
    churn = clean_text(ctx.churn_risk).strip() if ctx.churn_risk else ''
    score = ctx.account_risk_score
    if not churn and score is None:
        r['H017'] = ControlResult(STATUS_PARTIAL, 'ChurnZero risk field (CSM_Churn_Risk__c) and Account_Risk_Score__c are both empty. Churn risk cannot be assessed.', WHY['H017'], SOURCES['H017'])
    else:
        score_str = str(score) if score is not None else 'Not set'
        risk_lower = churn.lower()
        if 'high' in risk_lower:
            r['H017'] = ControlResult(STATUS_FLAG, f'Churn risk: {churn}. Risk score: {score_str}. High churn risk requires immediate action — review all health indicators and escalate.', WHY['H017'], SOURCES['H017'])
        elif 'medium' in risk_lower or 'mod' in risk_lower:
            r['H017'] = ControlResult(STATUS_PARTIAL, f'Churn risk: {churn}. Risk score: {score_str}. Monitor closely and address the primary drivers before the next QR.', WHY['H017'], SOURCES['H017'])
        else:
            r['H017'] = ControlResult(STATUS_OK, f'Churn risk: {churn}. Risk score: {score_str}.', WHY['H017'], SOURCES['H017'])

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
        return 'Healthy'
    if score >= 40:
        return 'Needs Attention'
    return 'At Risk'
