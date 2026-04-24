from __future__ import annotations

from datetime import date, timedelta
from typing import Dict

from config_dsp_mastery import (
    CONTROL_NAMES, IMPACT_LABEL, IMPORTANCE, PRIORITY_POINTS,
    SCORING_EXCLUDED, SOURCES, WHY, ControlResult,
    STATUS_OK, STATUS_PARTIAL, STATUS_FLAG,
)
from reader_databricks_dsp import DSPContext, clean_text, money_str, roas_str, trim, to_float


def evaluate_all(ctx: DSPContext) -> Dict[str, ControlResult]:
    r: Dict[str, ControlResult] = {}

    # ─────────────────────────────────────────────────────────────────────
    # M001 — Clear Objective Documented
    # ─────────────────────────────────────────────────────────────────────
    obj = trim(ctx.primary_objective, 300)
    notes = trim(ctx.cs_notes, 300)
    roas_doc = roas_str(ctx.target_roas)

    if obj and len(obj) > 20:
        r['M001'] = ControlResult(
            STATUS_OK,
            f'Objective documented in Salesforce: "{obj}". Target ROAS: {roas_doc}.',
            WHY['M001'], SOURCES['M001'],
        )
    elif obj or notes:
        snippet = obj or notes
        r['M001'] = ControlResult(
            STATUS_PARTIAL,
            f'A partial objective exists ("{trim(snippet, 180)}") but it lacks a measurable KPI target or timeframe. Target ROAS: {roas_doc}.',
            WHY['M001'], SOURCES['M001'],
        )
    else:
        r['M001'] = ControlResult(
            STATUS_FLAG,
            f'No objective is documented in Salesforce (Primary_Objective__c and CS_Notes__c are both empty). Target ROAS: {roas_doc}.',
            WHY['M001'], SOURCES['M001'],
        )

    # ─────────────────────────────────────────────────────────────────────
    # M002 — ROAS / Efficiency Target Documented
    # ─────────────────────────────────────────────────────────────────────
    if ctx.target_roas is not None and ctx.target_roas > 0:
        current = roas_str(ctx.roas)
        gap = None
        if ctx.roas is not None:
            gap = ((ctx.roas - ctx.target_roas) / ctx.target_roas) * 100
        gap_str = f'{gap:+.1f}%' if gap is not None else 'unknown'
        r['M002'] = ControlResult(
            STATUS_OK,
            f'Target ROAS set at {ctx.target_roas:.2f}x. Current ROAS: {current}. Gap vs target: {gap_str}.',
            WHY['M002'], SOURCES['M002'],
        )
    else:
        r['M002'] = ControlResult(
            STATUS_FLAG,
            'Target_ROAS__c is not set in the DSP Project record. The account has no documented efficiency guardrail.',
            WHY['M002'], SOURCES['M002'],
        )

    # ─────────────────────────────────────────────────────────────────────
    # M003 — Daily Spend Target Set
    # ─────────────────────────────────────────────────────────────────────
    if ctx.daily_target is not None and ctx.daily_target > 0:
        if ctx.ad_spend is not None and ctx.window_days and ctx.window_days > 0:
            actual_daily = ctx.ad_spend / ctx.window_days
            dev = abs(actual_daily - ctx.daily_target) / ctx.daily_target * 100
            direction = 'below' if actual_daily < ctx.daily_target else 'above'
            msg = (f'Daily spend target: {money_str(ctx.daily_target)}/day. '
                   f'Actual L{ctx.window_days} daily avg: {money_str(actual_daily)}. '
                   f'Deviation: {dev:.0f}% {direction} target.')
            if dev <= 15:
                r['M003'] = ControlResult(STATUS_OK, msg, WHY['M003'], SOURCES['M003'])
            elif dev <= 30:
                r['M003'] = ControlResult(STATUS_PARTIAL, msg, WHY['M003'], SOURCES['M003'])
            else:
                r['M003'] = ControlResult(STATUS_FLAG, msg, WHY['M003'], SOURCES['M003'])
        else:
            r['M003'] = ControlResult(
                STATUS_OK,
                f'Daily spend target set at {money_str(ctx.daily_target)}/day. Actual spend data not available to verify pacing.',
                WHY['M003'], SOURCES['M003'],
            )
    else:
        r['M003'] = ControlResult(
            STATUS_FLAG,
            'daily_target_spend__c is not set in the DSP Project record. Pacing cannot be monitored against a target.',
            WHY['M003'], SOURCES['M003'],
        )

    # ─────────────────────────────────────────────────────────────────────
    # M004 — Client Success Plan Completed
    # ─────────────────────────────────────────────────────────────────────
    has_obj  = bool(ctx.primary_objective and len(ctx.primary_objective) > 15)
    has_chal = bool(ctx.current_challenges and len(ctx.current_challenges) > 15)
    has_near = bool(ctx.near_term and len(ctx.near_term) > 15)
    filled = sum([has_obj, has_chal, has_near])

    obj_s  = 'Present' if has_obj  else 'Missing'
    chal_s = 'Present' if has_chal else 'Missing'
    near_s = 'Present' if has_near else 'Missing'
    msg = (f'CSP fields — Objective: {obj_s} | Challenges: {chal_s} | Near-term plan: {near_s}.')

    if filled == 3:
        r['M004'] = ControlResult(STATUS_OK, msg, WHY['M004'], SOURCES['M004'])
    elif filled >= 1:
        r['M004'] = ControlResult(STATUS_PARTIAL, msg + ' Complete the missing fields before the next QR.', WHY['M004'], SOURCES['M004'])
    else:
        r['M004'] = ControlResult(STATUS_FLAG, msg + ' All three CSP fields are empty.', WHY['M004'], SOURCES['M004'])

    # ─────────────────────────────────────────────────────────────────────
    # M005 — Client Journey Map Completed
    # ─────────────────────────────────────────────────────────────────────
    if ctx.journey_stage and ctx.journey_stage.lower() not in {'', 'none', 'nan'}:
        strat = trim(ctx.journey_strategy, 200) if ctx.journey_strategy else 'Not documented'
        r['M005'] = ControlResult(
            STATUS_OK,
            f'Journey Map stage: {ctx.journey_stage}. Strategy for this stage: "{strat}".',
            WHY['M005'], SOURCES['M005'],
        )
    else:
        r['M005'] = ControlResult(
            STATUS_FLAG,
            'No active Journey Map stage is set in Salesforce. The team cannot determine where this client is in their lifecycle.',
            WHY['M005'], SOURCES['M005'],
        )

    # ─────────────────────────────────────────────────────────────────────
    # M006 — Account Risk Status Up to Date
    # ─────────────────────────────────────────────────────────────────────
    at_risk = ctx.at_risk
    notes = trim(ctx.risk_notes, 200)

    if at_risk is True:
        if notes:
            r['M006'] = ControlResult(
                STATUS_OK,
                f'Account is flagged At Risk. Risk notes documented: "{notes}".',
                WHY['M006'], SOURCES['M006'],
            )
        else:
            r['M006'] = ControlResult(
                STATUS_PARTIAL,
                'Account is flagged At Risk but Risk_Reason_Notes__c is empty. The flag is set but the reason is not explained.',
                WHY['M006'], SOURCES['M006'],
            )
    elif at_risk is False:
        r['M006'] = ControlResult(
            STATUS_OK,
            'At Risk flag is False and no risk notes are present. Account status appears clean.',
            WHY['M006'], SOURCES['M006'],
        )
    else:
        r['M006'] = ControlResult(
            STATUS_PARTIAL,
            'At_Risk__c field is not populated in the DSP Project record. Risk status is unknown.',
            WHY['M006'], SOURCES['M006'],
        )

    # ─────────────────────────────────────────────────────────────────────
    # M007 — Gong Call Cadence — Last 90 Days
    # ─────────────────────────────────────────────────────────────────────
    if ctx.last_call_date is None:
        r['M007'] = ControlResult(
            STATUS_FLAG,
            'No Gong calls were found for this account. Client contact cadence cannot be confirmed.',
            WHY['M007'], SOURCES['M007'],
        )
    elif ctx.days_since_call is not None and ctx.days_since_call > 90:
        r['M007'] = ControlResult(
            STATUS_FLAG,
            f'Last Gong call: {ctx.last_call_date} ({ctx.days_since_call} days ago). This exceeds the 90-day threshold. Calls in last 90 days: {ctx.calls_l90d}.',
            WHY['M007'], SOURCES['M007'],
        )
    elif ctx.days_since_call is not None and ctx.days_since_call > 60:
        r['M007'] = ControlResult(
            STATUS_PARTIAL,
            f'Last Gong call: {ctx.last_call_date} ({ctx.days_since_call} days ago). Approaching the 90-day limit. Calls in last 90 days: {ctx.calls_l90d}.',
            WHY['M007'], SOURCES['M007'],
        )
    else:
        r['M007'] = ControlResult(
            STATUS_OK,
            f'Last Gong call: {ctx.last_call_date} ({ctx.days_since_call} days ago). Calls in last 90 days: {ctx.calls_l90d}.',
            WHY['M007'], SOURCES['M007'],
        )

    # ─────────────────────────────────────────────────────────────────────
    # M008 / M009 — Manual on-call controls (always OK)
    # ─────────────────────────────────────────────────────────────────────
    r['M008'] = ControlResult(STATUS_OK, 'To be reviewed during the QR presentation call.', WHY['M008'], SOURCES['M008'])
    r['M009'] = ControlResult(STATUS_OK, 'To be reviewed during the QR presentation call.', WHY['M009'], SOURCES['M009'])

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
        if cid in SCORING_EXCLUDED:
            continue
        findings.append({'cid': cid, 'name': CONTROL_NAMES[cid], 'status': res.status,
                          'what': res.what, 'why': res.why,
                          'importance': imp, 'impact': IMPACT_LABEL[imp], 'penalty': pen})
    score = 100 + total_penalty
    grade = _grade(score)
    findings.sort(key=lambda x: (0 if x['status'] == STATUS_FLAG else 1, x['penalty']))
    return total_penalty, score, grade, findings


def _grade(score: float) -> str:
    if score >= 75:
        return 'Compliant'
    if score >= 40:
        return 'Needs Attention'
    return 'Not Compliant'
