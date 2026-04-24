from __future__ import annotations

from typing import Dict

import pandas as pd

from config_dsp_strategy import (
    CONTROL_NAMES, IMPACT_LABEL, IMPORTANCE, PRIORITY_POINTS,
    SCORING_EXCLUDED, SOURCES, WHY, ControlResult,
    STATUS_OK, STATUS_PARTIAL, STATUS_FLAG,
)
from reader_databricks_dsp import DSPContext, clean_text, money_str, roas_str, pct_str, to_float, trim, _find_col


def evaluate_all(ctx: DSPContext) -> Dict[str, ControlResult]:
    r: Dict[str, ControlResult] = {}

    # ─────────────────────────────────────────────────────────────────────
    # S001 — Budget Split Aligned with Funnel Strategy
    # ─────────────────────────────────────────────────────────────────────
    if ctx.upper_pct is None:
        r['S001'] = ControlResult(STATUS_PARTIAL, 'Funnel spend data not available (07_DSP_Spend_by_Strategy_&_Funn). Cannot calculate budget split by funnel stage.', WHY['S001'], SOURCES['S001'])
    else:
        obj = trim(ctx.primary_objective, 100) or 'Not documented'
        upper = ctx.upper_pct or 0
        mid   = ctx.mid_pct   or 0
        lower = ctx.lower_pct or 0
        other = 100 - upper - mid - lower
        msg = (f'Funnel spend distribution: Upper {upper:.0f}% | Mid {mid:.0f}% | Lower {lower:.0f}%'
               + (f' | Unclassified {other:.0f}%' if other > 5 else '') + f'. Strategy objective: "{obj}".')

        # Flag if >80% in one layer without a clear strategy rationale
        if lower >= 80 and 'retarget' not in obj.lower() and 'retention' not in obj.lower():
            r['S001'] = ControlResult(STATUS_FLAG, msg + ' Over 80% of spend is in lower funnel. The account is not building future audiences.', WHY['S001'], SOURCES['S001'])
        elif upper >= 70 and lower < 15:
            r['S001'] = ControlResult(STATUS_PARTIAL, msg + ' Upper funnel is dominant with minimal lower funnel. Confirm that retargeting audiences have been built.', WHY['S001'], SOURCES['S001'])
        elif other >= 40:
            r['S001'] = ControlResult(STATUS_PARTIAL, msg + ' A large share of spend is unclassified by funnel stage. Review order FunnelStage settings.', WHY['S001'], SOURCES['S001'])
        else:
            r['S001'] = ControlResult(STATUS_OK, msg, WHY['S001'], SOURCES['S001'])

    # ─────────────────────────────────────────────────────────────────────
    # S002 — ROAS Target Achievable Given Funnel Mix
    # ─────────────────────────────────────────────────────────────────────
    if ctx.target_roas is None or ctx.upper_pct is None:
        r['S002'] = ControlResult(STATUS_PARTIAL, f'Target ROAS or funnel split data not available. Target ROAS: {roas_str(ctx.target_roas)}. Funnel mix check skipped.', WHY['S002'], SOURCES['S002'])
    else:
        upper = ctx.upper_pct or 0
        blended = roas_str(ctx.roas)
        upper_roas = roas_str(ctx.upper_roas)
        lower_roas = roas_str(ctx.lower_roas)
        msg = (f'Target ROAS: {ctx.target_roas:.2f}x. Blended ROAS: {blended}. '
               f'Upper funnel share: {upper:.0f}%. Upper ROAS: {upper_roas}. Lower ROAS: {lower_roas}.')

        # If upper funnel is dominant and target ROAS is strict (>4x), flag
        if upper >= 40 and ctx.target_roas >= 4 and ctx.roas is not None and ctx.roas < ctx.target_roas * 0.8:
            r['S002'] = ControlResult(STATUS_FLAG, msg + f' A ROAS target of {ctx.target_roas:.2f}x is difficult to achieve with {upper:.0f}% upper funnel spend. Consider a blended target or separate reporting per layer.', WHY['S002'], SOURCES['S002'])
        elif upper >= 30 and ctx.target_roas >= 5:
            r['S002'] = ControlResult(STATUS_PARTIAL, msg + f' Target ROAS of {ctx.target_roas:.2f}x may be challenging given the current upper funnel investment. Review attribution window settings.', WHY['S002'], SOURCES['S002'])
        else:
            r['S002'] = ControlResult(STATUS_OK, msg, WHY['S002'], SOURCES['S002'])

    # ─────────────────────────────────────────────────────────────────────
    # S003 — Best-Selling ASINs Are Promoted
    # ─────────────────────────────────────────────────────────────────────
    if ctx.df08 is None or ctx.df08.empty:
        r['S003'] = ControlResult(STATUS_PARTIAL, 'ASIN level data not available (08_DSP_ASIN_Level_Report). Cannot verify promoted ASIN coverage.', WHY['S003'], SOURCES['S003'])
    else:
        asin_col   = _find_col(ctx.df08, ['ASIN'])
        spend_col  = _find_col(ctx.df08, ['AdSpend'])
        sales_col  = _find_col(ctx.df08, ['AdSales'])

        if spend_col is None:
            r['S003'] = ControlResult(STATUS_PARTIAL, 'AdSpend column not found in 08_DSP_ASIN_Level_Report.', WHY['S003'], SOURCES['S003'])
        else:
            df = ctx.df08.copy()
            df['_spend'] = pd.to_numeric(df[spend_col], errors='coerce').fillna(0)
            df['_sales'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0) if sales_col else 0

            # ASINs with spend = currently promoted
            promoted = df[df['_spend'] > 0]
            no_spend = df[df['_spend'] == 0]
            promoted_count = len(promoted)
            total_count = len(df)

            # Top ASINs by sales — check if they have spend
            if sales_col:
                top_by_sales = df.nlargest(5, '_sales')
                top_not_promoted = top_by_sales[top_by_sales['_spend'] == 0]
                top_asin_list = [clean_text(v) for v in top_not_promoted[asin_col].tolist()] if asin_col else []
                if len(top_not_promoted) == 0:
                    r['S003'] = ControlResult(STATUS_OK, f'{promoted_count} ASINs are receiving DSP spend. All top 5 ASINs by sales have active promotion.', WHY['S003'], SOURCES['S003'])
                elif len(top_not_promoted) <= 2:
                    r['S003'] = ControlResult(STATUS_PARTIAL, f'{len(top_not_promoted)} of the top 5 ASINs by sales have no DSP spend: {", ".join(top_asin_list[:3])}. Consider adding to a lower funnel order.', WHY['S003'], SOURCES['S003'])
                else:
                    r['S003'] = ControlResult(STATUS_FLAG, f'{len(top_not_promoted)} of the top 5 ASINs by sales have no DSP promotion: {", ".join(top_asin_list[:3])}. Budget is going to lower-priority products.', WHY['S003'], SOURCES['S003'])
            else:
                r['S003'] = ControlResult(STATUS_OK, f'{promoted_count} ASINs have active DSP spend out of {total_count} total in the report.', WHY['S003'], SOURCES['S003'])

    # ─────────────────────────────────────────────────────────────────────
    # S004 — NTB Purchase Rate Above 50%
    # ─────────────────────────────────────────────────────────────────────
    if ctx.ntb_rate is None:
        r['S004'] = ControlResult(STATUS_PARTIAL, 'NTB purchase rate not available in 02_DSP_Date_Range_KPIs. Check that NTBPurchases and Conversions columns are populated.', WHY['S004'], SOURCES['S004'])
    else:
        ntb = ctx.ntb_rate
        ntb_count = int(ctx.ntb_purchases) if ctx.ntb_purchases is not None else 0
        total_conv_str = f'NTB rate: {ntb:.1f}% ({ntb_count:,} NTB of {int(ctx.ntb_purchases / ntb * 100) if ntb > 0 else 0:,} total purchases).'
        if ntb >= 50:
            r['S004'] = ControlResult(STATUS_OK, total_conv_str + ' NTB rate meets the ≥50% benchmark for non-retargeting orders.', WHY['S004'], SOURCES['S004'])
        elif ntb >= 30:
            r['S004'] = ControlResult(STATUS_PARTIAL, total_conv_str + ' NTB rate is below 50%. Upper funnel orders may be over-indexing on existing buyers.', WHY['S004'], SOURCES['S004'])
        else:
            r['S004'] = ControlResult(STATUS_FLAG, total_conv_str + ' NTB rate is critically low. The account is not acquiring new customers despite upper funnel spend.', WHY['S004'], SOURCES['S004'])

    # ─────────────────────────────────────────────────────────────────────
    # S005 — DSP Budget Is Min 15% of PPC Spend
    # ─────────────────────────────────────────────────────────────────────
    dsp = ctx.dsp_spend_total
    ppc = ctx.ppc_spend_total

    if dsp is None or ppc is None:
        r['S005'] = ControlResult(STATUS_PARTIAL, 'DSP vs PPC comparison data not available (10_DSP_vs_PPC_Comparison). Cannot verify the 15% ratio.', WHY['S005'], SOURCES['S005'])
    elif ppc == 0:
        r['S005'] = ControlResult(STATUS_PARTIAL, f'DSP spend: {money_str(dsp)}. PPC spend is zero or not identified in 10_DSP_vs_PPC_Comparison. Ratio cannot be calculated.', WHY['S005'], SOURCES['S005'])
    else:
        ratio = dsp / ppc * 100
        msg = f'DSP spend: {money_str(dsp)}. PPC spend: {money_str(ppc)}. DSP as % of PPC: {ratio:.1f}%. Benchmark: ≥15%.'
        if ratio >= 15:
            r['S005'] = ControlResult(STATUS_OK, msg, WHY['S005'], SOURCES['S005'])
        elif ratio >= 8:
            r['S005'] = ControlResult(STATUS_PARTIAL, msg + ' DSP is below 15% of PPC spend. Funnel influence is limited at this ratio.', WHY['S005'], SOURCES['S005'])
        else:
            r['S005'] = ControlResult(STATUS_FLAG, msg + ' DSP represents less than 8% of PPC spend. The channel cannot meaningfully influence the full funnel at this level.', WHY['S005'], SOURCES['S005'])

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
        return 'Compliant'
    if score >= 40:
        return 'Needs Attention'
    return 'Not Compliant'
