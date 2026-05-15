"""
CoE Analysis Tool — Flask backend
Supports: Amazon Account Health, Amazon Account Mastery, DSP (Framework / Mastery / Health / Strategy)
Run:  python app.py
Open: http://127.0.0.1:8502
"""

from __future__ import annotations

import gc
import os
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path

from flask import Flask, Response, jsonify, render_template, request, send_file
from werkzeug.utils import secure_filename

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.resolve()
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"

UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(BASE_DIR))

MIN_OUTPUT_BYTES = 5_000

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024


# ── Template registry ──────────────────────────────────────────────────────────
TEMPLATES = {
    # Amazon agents
    "health":   BASE_DIR / "CoE_Account_Health_Analysis_Templates.xlsm",
    "mastery":  BASE_DIR / "CoE_Account_Mastery_Analysis_Templates.xlsm",
    # DSP agents
    "dsp_framework": BASE_DIR / "CoE_DSP_Framework_Analysis_Templates.xlsm",
    "dsp_mastery":   BASE_DIR / "CoE_DSP_Account_Mastery_Analysis_Templates.xlsm",
    "dsp_health":    BASE_DIR / "CoE_DSP_Account_Health_Analysis_Templates.xlsm",
    "dsp_strategy":  BASE_DIR / "CoE_DSP_Account_Strategy_Analysis_Templates.xlsm",
}


def _safe_fn(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r'[^a-zA-Z0-9 \-_]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name or "UNKNOWN_ACCOUNT"


# ── Amazon Health ─────────────────────────────────────────────────────────────

def run_amazon_health(input_path: str) -> dict:
    from reader_databricks_health import load_databricks_context
    from rules_engine_health import evaluate_all
    from writer_account_health import write_account_health_output

    template = TEMPLATES["health"]
    if not template.exists():
        raise FileNotFoundError(f"Template not found: {template}")

    ctx = load_databricks_context(input_path)
    hash_name = getattr(ctx, "hash_name", "") or "UNKNOWN_ACCOUNT"
    safe_hash = _safe_fn(hash_name)
    results, ctx = evaluate_all(ctx)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"{safe_hash} - Account Health Analysis - {ts}.xlsm"
    out_path = OUTPUT_DIR / out_name

    write_account_health_output(str(template), str(out_path), ctx=ctx, results=results)
    _verify_output(out_path)

    ok = sum(1 for r in results.values() if r.status == "OK")
    flag = sum(1 for r in results.values() if r.status == "FLAG")
    partial = sum(1 for r in results.values() if r.status == "PARTIAL")

    info = {
        "download_filename": out_name,
        "account": hash_name,
        "pillar": "Account Health",
        "platform": "Amazon",
        "ok": ok, "flag": flag, "partial": partial,
        "flag_ids":    [c for c, r in results.items() if r.status == "FLAG"],
        "partial_ids": [c for c, r in results.items() if r.status == "PARTIAL"],
    }
    del ctx, results
    gc.collect()
    return info


# ── Amazon Mastery ────────────────────────────────────────────────────────────

def run_amazon_mastery(input_path: str) -> dict:
    from reader_databricks_mastery import load_databricks_context
    from rules_engine_mastery import build_summary, compute_score, evaluate_all
    from writer_account_mastery import write_mastery_output

    template = TEMPLATES["mastery"]
    if not template.exists():
        raise FileNotFoundError(f"Template not found: {template}")

    ctx = load_databricks_context(input_path)
    results = evaluate_all(ctx)
    summary = build_summary(ctx, results)
    penalty, score, grade, findings = compute_score(results)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_hash = _safe_fn(ctx.hash_name)
    out_name = f"{safe_hash} - Account Mastery Analysis - {ts}.xlsm"
    out_path = OUTPUT_DIR / out_name

    write_mastery_output(str(template), str(out_path), summary, results, penalty, score, grade, findings, ctx)
    _verify_output(out_path)

    info = {
        "download_filename": out_name,
        "account": ctx.hash_name,
        "pillar": "Account Mastery",
        "platform": "Amazon",
        "score": round(score, 1),
        "grade": grade,
        "ok":      sum(1 for r in results.values() if r.status == "OK"),
        "flag":    sum(1 for r in results.values() if r.status == "FLAG"),
        "partial": sum(1 for r in results.values() if r.status == "PARTIAL"),
        "flag_ids":    [c for c, r in results.items() if r.status == "FLAG"],
        "partial_ids": [c for c, r in results.items() if r.status == "PARTIAL"],
    }
    del ctx, results, summary
    gc.collect()
    return info


# ── DSP agents ────────────────────────────────────────────────────────────────

def _run_dsp_all(input_path: str) -> dict:
    """Load DSP context once, run all four pillars sequentially, return combined result."""
    from reader_databricks_dsp import load_dsp_context
    from rules_engine_dsp_framework import evaluate_all as fw_eval, compute_score as fw_score
    from rules_engine_dsp_health import evaluate_all as ah_eval, compute_score as ah_score
    from rules_engine_dsp_mastery import evaluate_all as am_eval, compute_score as am_score
    from rules_engine_dsp_strategy import evaluate_all as st_eval, compute_score as st_score
    from writer_dsp_framework import write_dsp_framework_output
    from writer_dsp_health import write_dsp_health_output
    from writer_dsp_mastery import write_dsp_mastery_output
    from writer_dsp_strategy import write_dsp_strategy_output

    for key in ("dsp_framework", "dsp_mastery", "dsp_health", "dsp_strategy"):
        if not TEMPLATES[key].exists():
            raise FileNotFoundError(f"Template not found: {TEMPLATES[key]}")

    ctx = load_dsp_context(input_path)
    safe_hash = _safe_fn(ctx.hash_name)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    pillars = []

    def _pillar_info(results, score, grade, pillar_label, out_name):
        return {
            "pillar": pillar_label,
            "download_filename": out_name,
            "download_url": f"/download/{out_name}",
            "score": round(score, 1),
            "grade": grade,
            "ok":      sum(1 for r in results.values() if r.status == "OK"),
            "flag":    sum(1 for r in results.values() if r.status == "FLAG"),
            "partial": sum(1 for r in results.values() if r.status == "PARTIAL"),
            "flag_ids":    [c for c, r in results.items() if r.status == "FLAG"],
            "partial_ids": [c for c, r in results.items() if r.status == "PARTIAL"],
        }

    # Framework
    fw_res = fw_eval(ctx)
    _, fw_s, fw_g, _ = fw_score(fw_res)
    fw_name = f"{safe_hash} - DSP Framework Analysis - {ts}.xlsm"
    write_dsp_framework_output(str(TEMPLATES["dsp_framework"]), str(OUTPUT_DIR / fw_name), fw_res, ctx)
    _verify_output(OUTPUT_DIR / fw_name)
    pillars.append(_pillar_info(fw_res, fw_s, fw_g, "Framework", fw_name))
    del fw_res; gc.collect()

    # Account Health
    ah_res = ah_eval(ctx)
    _, ah_s, ah_g, _ = ah_score(ah_res)
    ah_name = f"{safe_hash} - DSP Health Analysis - {ts}.xlsm"
    write_dsp_health_output(str(TEMPLATES["dsp_health"]), str(OUTPUT_DIR / ah_name), ah_res, ctx)
    _verify_output(OUTPUT_DIR / ah_name)
    pillars.append(_pillar_info(ah_res, ah_s, ah_g, "Account Health", ah_name))
    del ah_res; gc.collect()

    # Account Mastery
    am_res = am_eval(ctx)
    _, am_s, am_g, _ = am_score(am_res)
    am_name = f"{safe_hash} - DSP Mastery Analysis - {ts}.xlsm"
    write_dsp_mastery_output(str(TEMPLATES["dsp_mastery"]), str(OUTPUT_DIR / am_name), am_res, ctx)
    _verify_output(OUTPUT_DIR / am_name)
    pillars.append(_pillar_info(am_res, am_s, am_g, "Account Mastery", am_name))
    del am_res; gc.collect()

    # Strategy
    st_res = st_eval(ctx)
    _, st_s, st_g, _ = st_score(st_res)
    st_name = f"{safe_hash} - DSP Strategy Analysis - {ts}.xlsm"
    write_dsp_strategy_output(str(TEMPLATES["dsp_strategy"]), str(OUTPUT_DIR / st_name), st_res, ctx)
    _verify_output(OUTPUT_DIR / st_name)
    pillars.append(_pillar_info(st_res, st_s, st_g, "Strategy", st_name))
    del st_res

    account = ctx.hash_name
    del ctx; gc.collect()

    return {"account": account, "platform": "DSP", "pillars": pillars}


def _run_dsp_agent(input_path: str, pillar_key: str, pillar_label: str,
                   output_suffix: str) -> dict:
    """Runs all pillars, returns the requested one — preserves individual route compat."""
    all_res = _run_dsp_all(input_path)
    for p in all_res["pillars"]:
        if p["pillar"] == pillar_label:
            return {**p, "account": all_res["account"], "platform": "DSP"}
    p = all_res["pillars"][0]
    return {**p, "account": all_res["account"], "platform": "DSP"}


def _verify_output(path: Path):
    size = path.stat().st_size if path.exists() else 0
    if not path.exists() or size < MIN_OUTPUT_BYTES:
        raise RuntimeError(f"Output file missing or too small ({size} bytes): {path}")


# ── Generic upload handler ────────────────────────────────────────────────────

def _handle_upload(request, run_fn):
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded."}), 400
    uploaded = request.files["file"]
    if not uploaded.filename:
        return jsonify({"error": "No file selected."}), 400
    _, ext = os.path.splitext(uploaded.filename.lower())
    if ext not in {".xlsx", ".xlsm"}:
        return jsonify({"error": "Only .xlsx or .xlsm files accepted."}), 400

    safe_name  = secure_filename(uploaded.filename)
    input_path = str(UPLOAD_DIR / safe_name)
    uploaded.save(input_path)

    try:
        info = run_fn(input_path)
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Analysis failed: {e}"}), 500
    finally:
        try:
            os.remove(input_path)
        except Exception:
            pass
        gc.collect()

    info["download_url"] = f"/download/{info['download_filename']}"
    return jsonify(info)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# Amazon routes
@app.route("/analyze", methods=["POST"])
def analyze():
    return _handle_upload(request, run_amazon_health)


@app.route("/analyze/mastery", methods=["POST"])
def analyze_mastery():
    return _handle_upload(request, run_amazon_mastery)


# DSP routes
@app.route("/analyze/dsp/framework", methods=["POST"])
def analyze_dsp_framework():
    return _handle_upload(request, lambda p: _run_dsp_agent(p, "dsp_framework", "Framework", "DSP Framework Analysis"))


@app.route("/analyze/dsp/mastery", methods=["POST"])
def analyze_dsp_mastery():
    return _handle_upload(request, lambda p: _run_dsp_agent(p, "dsp_mastery", "Account Mastery", "DSP Mastery Analysis"))


@app.route("/analyze/dsp/health", methods=["POST"])
def analyze_dsp_health():
    return _handle_upload(request, lambda p: _run_dsp_agent(p, "dsp_health", "Account Health", "DSP Health Analysis"))


@app.route("/analyze/dsp/strategy", methods=["POST"])
def analyze_dsp_strategy():
    return _handle_upload(request, lambda p: _run_dsp_agent(p, "dsp_strategy", "Strategy", "DSP Strategy Analysis"))


@app.route("/analyze/dsp/all", methods=["POST"])
def analyze_dsp_all():
    """Single endpoint that runs all four DSP pillars sequentially and returns combined JSON.
    Response shape: { account, platform, pillars: [ {pillar, download_filename, download_url,
    score, grade, ok, flag, partial, flag_ids, partial_ids}, ... ] }
    """
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded."}), 400
    uploaded = request.files["file"]
    if not uploaded.filename:
        return jsonify({"error": "No file selected."}), 400
    _, ext = os.path.splitext(uploaded.filename.lower())
    if ext not in {".xlsx", ".xlsm"}:
        return jsonify({"error": "Only .xlsx or .xlsm files accepted."}), 400

    safe_name  = secure_filename(uploaded.filename)
    input_path = str(UPLOAD_DIR / safe_name)
    uploaded.save(input_path)

    try:
        result = _run_dsp_all(input_path)
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Analysis failed: {e}"}), 500
    finally:
        try:
            os.remove(input_path)
        except Exception:
            pass
        gc.collect()

    return jsonify(result)


# Download route
@app.route("/download/<path:filename>")
def download(filename):
    from urllib.parse import unquote
    filename = unquote(filename)
    p = OUTPUT_DIR / filename

    if not p.exists():
        xlsm_files = sorted(OUTPUT_DIR.glob("*.xlsm"), key=lambda f: f.stat().st_mtime, reverse=True)
        if xlsm_files:
            p = xlsm_files[0]
            filename = p.name
        else:
            return f"No output files found in {OUTPUT_DIR}", 404

    data = p.read_bytes()
    return Response(
        data,
        mimetype="application/vnd.ms-excel.sheet.macroEnabled.12",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(data)),
        }
    )


@app.route("/favicon.ico")
def favicon():
    return "", 204


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n  CoE Analysis Tool")
    print("  ─────────────────────────────────────────────────")
    for key, tmpl in TEMPLATES.items():
        status = "✓" if tmpl.exists() else "✗ MISSING"
        print(f"  [{status}] {key}: {tmpl.name}")
    print(f"\n  Outputs : {OUTPUT_DIR}")
    print("  Open → http://127.0.0.1:8502\n")
    app.run(host="127.0.0.1", port=8502, debug=True)
