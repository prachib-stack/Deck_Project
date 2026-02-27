"""
Fraud Detection Dashboard
Displays: (1) CRN Analysis, (2) Cancellation Analysis, (3) Duplicate Records, (4) Data Management
Data source: CSV upload processed for fraud indicators
"""

import csv
import json
import os
import time
import threading
from collections import defaultdict
from flask import Flask, render_template_string, jsonify, request as flask_request, send_file
from werkzeug.utils import secure_filename

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CSV_PATH = os.path.join(DATA_DIR, "duplicates.csv")
CRN_RATIO_PATH = os.path.join(DATA_DIR, "crn_ratio.json")
SOURCE_CSV_PATH = os.path.join(DATA_DIR, "source_data.csv")

os.makedirs(DATA_DIR, exist_ok=True)

KEY_COLS = ["BuyerDtls_Gstin", "SellerDtls_Gstin", "DocDtls_Dt", "DocDtls_No"]
DISPLAY_COLS_PREFERRED = ["DocDtls_No", "DocDtls_Dt", "DocDtls_Typ", "SellerDtls_Gstin", "SellerDtls_LglNm", "BuyerDtls_Gstin", "ValDtls_TotInvVal"]

_cache = {}
# Track processing status
_status = {"status": "Idle", "progress": 0, "message": ""}

def load_duplicates():
    if "dup_rows" in _cache: return _cache["dup_rows"], _cache["dup_display_cols"], _cache["dup_stats"]
    if not os.path.exists(CSV_PATH): return [], [], {}
    rows = []
    try:
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f); [rows.append(r) for r in reader]
    except: return [], [], {}
    groups = defaultdict(list)
    for i, row in enumerate(rows):
        vals = tuple((row.get(k) or "").strip() for k in KEY_COLS)
        if all(vals): groups[vals].append(i)
    dup_rows = []
    group_id = 0; total_value = 0.0; unique_sellers = set()
    for key in sorted(groups.keys()):
        indices = groups[key]
        if len(indices) <= 1: continue
        for idx in indices:
            row = rows[idx]; row["_group_id"] = group_id; row["_group_size"] = len(indices); dup_rows.append(row)
            try: total_value += float(row.get("ValDtls_TotInvVal") or 0)
            except: pass
            s = (row.get("SellerDtls_Gstin") or "").strip()
            if s: unique_sellers.add(s)
        group_id += 1
    stats = {"total_rows": len(dup_rows), "num_groups": group_id, "total_value": total_value, "unique_sellers": len(unique_sellers)}
    _cache["dup_rows"], _cache["dup_display_cols"], _cache["dup_stats"] = dup_rows, DISPLAY_COLS_PREFERRED, stats
    return dup_rows, DISPLAY_COLS_PREFERRED, stats

def load_crn_ratios():
    if "crn_data" in _cache: return _cache["crn_data"], _cache["crn_stats"]
    if not os.path.exists(CRN_RATIO_PATH): return [], {}
    try:
        with open(CRN_RATIO_PATH, "r") as f: data = json.load(f)
    except: return [], {}
    stats = {"total_sellers": len(data), "high_crn": sum(1 for x in data if x.get("crn_ratio", 0) > 0.5), "high_can": sum(1 for x in data if x.get("can_ratio", 0) > 0.2), "total_crn_val": sum(x.get("total_crn_val", 0) for x in data)}
    _cache["crn_data"], _cache["crn_stats"] = data, stats
    return data, stats

def get_col(row, keys):
    for k in keys:
        if k in row: return row[k]
    # Try case-insensitive and underscores/spaces
    row_keys = {rk.lower().replace("_", "").replace(" ", ""): rk for rk in row.keys()}
    for k in keys:
        target = k.lower().replace("_", "").replace(" ", "")
        if target in row_keys: return row[row_keys[target]]
    return ""

def process_raw_csv(filepath):
    global _status
    _status = {"status": "In Progress", "progress": 10, "message": "Reading file..."}
    rows = []
    
    # Define mappings for different CSV formats
    col_map = {
        "DocDtls_Dt": ["DocDtls_Dt", "Document Date", "Date"],
        "DocDtls_No": ["DocDtls_No", "Document Number", "Invoice Number"],
        "SellerDtls_Gstin": ["SellerDtls_Gstin", "GSTIN", "Seller GSTIN"],
        "BuyerDtls_Gstin": ["BuyerDtls_Gstin", "Buyer GSTIN", "Receiver GSTIN"],
        "ValDtls_TotInvVal": ["ValDtls_TotInvVal", "Total Invoice Value", "Value"],
        "DocDtls_Typ": ["DocDtls_Typ", "Document Type", "Type"],
        "SellerDtls_LglNm": ["SellerDtls_LglNm", "Seller Legal Name", "Supplier Name"],
        "Status": ["Status", "Document Status", "IRN Generated Status"],
        "CancelledStatus": ["Cancelled Status", "Cancellation Status", "Cancel Status"]
    }

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            raw_rows = list(reader)
        
        _status = {"status": "In Progress", "progress": 25, "message": f"Processing {len(raw_rows)} rows..."}
        
        for row in raw_rows:
            # Map columns to standard names
            mapped_row = {std: get_col(row, options) for std, options in col_map.items()}
            
            # Only include February data (robust check for 02, Feb, etc)
            dt = mapped_row["DocDtls_Dt"].lower()
            is_feb = any(x in dt for x in ["-02-", "/02/", "feb", "/2/"]) or dt.startswith("02/") or dt.startswith("02-")
            
            if is_feb:
                rows.append(mapped_row)
        
        if not rows:
            _status = {"status": "Completed", "progress": 100, "message": "No February data found in file."}
            return

        _status = {"status": "In Progress", "progress": 40, "message": "Analyzing duplicates..."}
        groups = defaultdict(list)
        for row in rows:
            # Now row is already mapped
            vals = tuple((row.get(k) or "").strip() for k in KEY_COLS)
            if all(vals): groups[vals].append(row)
        
        dup_list = []; dup_count_by_seller = defaultdict(int)
        for key, group in groups.items():
            if len(group) > 1:
                dup_list.extend(group)
                for r in group: 
                    gstin = r.get("SellerDtls_Gstin").strip()
                    dup_count_by_seller[gstin] += 1
        
        _status = {"status": "In Progress", "progress": 60, "message": "Saving analysis results..."}
        with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
            # Use standard fieldnames
            writer = csv.DictWriter(f, fieldnames=list(col_map.keys())); writer.writeheader(); writer.writerows(dup_list)
        
        sellers = defaultdict(lambda: {"inv_val": 0.0, "crn_val": 0.0, "inv_count": 0, "crn_count": 0, "can_count": 0, "name": ""})
        for row in rows:
            gstin = row["SellerDtls_Gstin"].strip()
            if not gstin: continue
            
            sellers[gstin]["name"] = row["SellerDtls_LglNm"] or "Unknown"
            doc_type = row["DocDtls_Typ"].upper()
            cancelled_status = (row.get("CancelledStatus") or "").strip().upper()
            try:
                v = row["ValDtls_TotInvVal"]
                if isinstance(v, str): v = v.replace(",", "").strip()
                val = float(v or 0)
            except: val = 0

            # Count cancellations from dedicated "Cancelled Status" column
            if cancelled_status == "CANCELLED":
                sellers[gstin]["can_count"] += 1
            
            # Count generated/active invoices
            if doc_type in ["INV", "INVOICE"]:
                sellers[gstin]["inv_val"] += val
                sellers[gstin]["inv_count"] += 1
            elif doc_type in ["CRN", "CREDIT NOTE"]:
                sellers[gstin]["crn_val"] += val
                sellers[gstin]["crn_count"] += 1
        
        ratio_output = []
        for gstin, d in sellers.items():
            crn_ratio = d["crn_val"] / d["inv_val"] if d["inv_val"] > 0 else 0
            can_ratio = d["can_count"] / d["inv_count"] if d["inv_count"] > 0 else 0
            dups = dup_count_by_seller[gstin]
            crn_reasons = []; can_reasons = []
            crn_score = 0; can_score = 0
            if crn_ratio > 0.5: crn_reasons.append(f"• High CRN Ratio ({crn_ratio:.2f})"); crn_score += 50
            if crn_ratio > 1.0: crn_reasons.append("• CRN exceeds Invoices"); crn_score += 30
            if can_ratio > 0.2: can_reasons.append(f"• High CAN Rate ({can_ratio:.1%})"); can_score += 50
            if dups > 0: msg = f"• Part of {dups} Duplicates"; crn_reasons.append(msg); can_reasons.append(msg); crn_score += 20; can_score += 20
            ratio_output.append({"gstin": gstin, "name": d["name"], "crn_ratio": crn_ratio, "can_ratio": can_ratio, "crn_score": min(crn_score, 100), "can_score": min(can_score, 100), "crn_reason": " ".join(crn_reasons) or "-", "can_reason": " ".join(can_reasons) or "-", "inv_count": d["inv_count"], "crn_count": d["crn_count"], "can_count": d["can_count"], "total_inv_val": d["inv_val"], "total_crn_val": d["crn_val"]})
        
        with open(CRN_RATIO_PATH, "w") as f: json.dump(ratio_output, f, indent=2)
        _cache.clear()
        _status = {"status": "Completed", "progress": 100, "message": f"Analysis finished. Processed {len(rows)} February records."}
    except Exception as e:
        _status = {"status": "Error", "progress": 0, "message": str(e)}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8"><title>Fraud Detection Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css">
    <style>
        :root { --dark: #1a1a2e; --accent: #e94560; --blue: #0f3460; }
        body { background: #f0f2f5; font-size: 13px; }
        .header-bar { background: linear-gradient(135deg, var(--dark) 0%, var(--blue) 100%); color: white; padding: 18px 0; margin-bottom: 24px; box-shadow: 0 4px 12px rgba(0,0,0,0.15); }
        .nav-tabs .nav-link { color: #555; font-weight: 500; border: none; padding: 10px 20px; }
        .nav-tabs .nav-link.active { color: var(--accent); border-bottom: 3px solid var(--accent); background: transparent; font-weight: 700; }
        .stat-card { background: white; border-radius: 10px; padding: 16px 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); text-align: center; border-left: 4px solid var(--blue); }
        .table-container { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
        .risk-score { padding: 3px 8px; border-radius: 5px; font-weight: bold; }
        .score-red { background: #dc3545; color: white; }
        .score-orange { background: #fd7e14; color: white; }
        .score-yellow { background: #ffc107; color: #333; }
        .reason-text { font-size: 11px; white-space: normal !important; color: #555; min-width: 250px; display: block; }
        .status-badge { font-size: 12px; padding: 5px 10px; border-radius: 20px; }
        .status-idle { background: #e9ecef; color: #6c757d; }
        .status-inprogress { background: #cfe2ff; color: #084298; }
        .status-completed { background: #d1e7dd; color: #0f5132; }
        .status-error { background: #f8d7da; color: #842029; }
    </style>
</head>
<body>
    <div class="header-bar text-center"><h1>Fraud Detection Dashboard</h1></div>
    <div class="container-fluid px-4">
        <ul class="nav nav-tabs" id="mainTabs">
            <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tabCRN">Credit Note Risk</button></li>
            <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabCAN">Cancellation Risk</button></li>
            <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabDuplicates">Duplicate Records</button></li>
            <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabUpload">Data Management</button></li>
        </ul>
        <div class="tab-content mt-3">
            <div class="tab-pane fade show active" id="tabCRN">
                <div class="row g-3 mb-4 text-center">
                    <div class="col-md-4"><div class="stat-card"><h3>{{ crn_stats.high_crn }}</h3><small>HIGH CRN RATIO SELLERS</small></div></div>
                    <div class="col-md-4"><div class="stat-card"><h3>{{ "{:,.0f}".format(crn_stats.total_crn_val) }}</h3><small>TOTAL CRN VALUE (&#8377;)</small></div></div>
                    <div class="col-md-4"><div class="stat-card"><h3>{{ crn_stats.total_sellers }}</h3><small>SELLERS ANALYZED</small></div></div>
                </div>
                <div class="table-container"><table id="crnTable" class="table table-sm table-bordered" style="width:100%">
                    <thead><tr><th>Score</th><th>Reasons</th><th>Seller GSTIN</th><th>Seller Name</th><th>Ratio</th><th>INV Qty</th><th>INV Value</th></tr></thead>
                </table></div>
            </div>
            <div class="tab-pane fade" id="tabCAN">
                <div class="row g-3 mb-4 text-center">
                    <div class="col-md-6"><div class="stat-card danger"><h3>{{ crn_stats.high_can }}</h3><small>HIGH CANCELLATION SELLERS</small></div></div>
                    <div class="col-md-6"><div class="stat-card"><h3>{{ crn_stats.total_sellers }}</h3><small>TOTAL SELLERS ANALYZED</small></div></div>
                </div>
                <div class="table-container"><table id="canTable" class="table table-sm table-bordered" style="width:100%">
                    <thead><tr><th>Score</th><th>Reasons</th><th>Seller GSTIN</th><th>Seller Name</th><th>Ratio</th><th>INV Qty</th><th>CAN Qty</th></tr></thead>
                </table></div>
            </div>
            <div class="tab-pane fade" id="tabDuplicates">
                <div class="table-container"><table id="dupTable" class="table table-sm table-bordered" style="width:100%">
                    <thead><tr><th>Group</th><th>#</th>{% for col in display_cols %}<th>{{ col }}</th>{% endfor %}</tr></thead>
                </table></div>
            </div>
            <div class="tab-pane fade" id="tabUpload"><div class="row justify-content-center"><div class="col-md-6"><div class="table-container text-center"><h5>Process New Data</h5><button class="btn btn-primary p-5 w-100" id="uploadBtn" onclick="$('#fileInput').click()"><i class="bi bi-cloud-arrow-up"></i><br>Upload Source CSV</button><input type="file" id="fileInput" class="d-none" accept=".csv"><div id="statusArea" class="mt-4" style="display:none;"><div class="d-flex justify-content-between mb-1"><span id="statusLabel" class="status-badge">Idle</span><span id="statusPercent">0%</span></div><div class="progress" style="height: 10px;"><div id="progressBar" class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 0%"></div></div><p id="statusMsg" class="mt-2 text-muted small"></p></div></div></div></div></div>
        </div>
    </div>
    <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
    function fmtNum(n) { return (n == null || n === '-') ? '-' : Number(n).toLocaleString('en-IN'); }
    function sCls(s) { if (s > 70) return 'score-red'; if (s > 30) return 'score-orange'; return 'score-yellow'; }
    $(document).ready(function() {
        $('#crnTable').DataTable({
            processing: true, serverSide: true, ajax: { url: '/api/ratios?type=crn', type: 'GET' },
            columns: [
                { data: 'crn_score', defaultContent: '0', render: function(d) { return '<span class="risk-score ' + sCls(d) + '">' + d + '</span>'; } },
                { data: 'crn_reason', defaultContent: '-', className: 'reason-text' }, { data: 'gstin', defaultContent: '-' }, { data: 'name', defaultContent: '-' },
                { data: 'crn_ratio', defaultContent: '0', render: function(d) { return d.toFixed(4); } }, { data: 'inv_count', defaultContent: '0' }, { data: 'total_inv_val', defaultContent: '0', render: function(d) { return fmtNum(d); } }
            ], pageLength: 50, order: [[0, 'desc']], scrollX: true
        });
        var canInit = false; $('button[data-bs-target="#tabCAN"]').on('shown.bs.tab', function() {
            if (canInit) return; canInit = true;
            $('#canTable').DataTable({
                processing: true, serverSide: true, ajax: { url: '/api/ratios?type=can', type: 'GET' },
                columns: [
                    { data: 'can_score', defaultContent: '0', render: function(d) { return '<span class="risk-score ' + sCls(d) + '">' + d + '</span>'; } },
                    { data: 'can_reason', defaultContent: '-', className: 'reason-text' }, { data: 'gstin', defaultContent: '-' }, { data: 'name', defaultContent: '-' },
                    { data: 'can_ratio', defaultContent: '0', render: function(d) { return d.toFixed(4); } }, { data: 'inv_count', defaultContent: '0' }, { data: 'can_count', defaultContent: '0' }
                ], pageLength: 50, order: [[0, 'desc']], scrollX: true
            });
        });
        var dupInit = false; $('button[data-bs-target="#tabDuplicates"]').on('shown.bs.tab', function() {
            if (dupInit) return; dupInit = true;
            $('#dupTable').DataTable({
                processing: true, serverSide: true, ajax: { url: '/api/duplicates', type: 'GET' },
                columns: [{ data: '_group_id', defaultContent: '0', render: function(d) { return '<span class="badge bg-secondary">G' + (parseInt(d)+1) + '</span>'; } }, { data: '_row_num', defaultContent: '-' }, {% for col in display_cols %}{ data: '{{ col }}', defaultContent: '-' {% if "Val" in col %}, render: function(d) { return fmtNum(d); } {% endif %} },{% endfor %}],
                pageLength: 50, order: [[0, 'asc']], scrollX: true
            });
        });

        function checkStatus() {
            $.get('/api/status', function(data) {
                $('#statusArea').show();
                $('#statusLabel').text(data.status).attr('class', 'status-badge status-' + data.status.toLowerCase().replace(' ', ''));
                $('#statusPercent').text(data.progress + '%');
                $('#progressBar').css('width', data.progress + '%');
                $('#statusMsg').text(data.message);
                
                if (data.status === 'In Progress') {
                    setTimeout(checkStatus, 1000);
                    $('#uploadBtn').prop('disabled', true);
                } else {
                    $('#uploadBtn').prop('disabled', false);
                    if (data.status === 'Completed' && data.progress === 100) {
                        setTimeout(function() { location.reload(); }, 2000);
                    }
                }
            });
        }

        $('#fileInput').on('change', function() {
            var formData = new FormData(); formData.append('file', this.files[0]);
            $('#statusArea').show();
            $('#statusMsg').text('Uploading file...');
            $.ajax({ url: '/api/upload-source', type: 'POST', data: formData, processData: false, contentType: false, success: function() {
                $.post('/api/run-analysis', function() {
                    checkStatus();
                });
            }, error: function() {
                $('#statusMsg').text('Upload failed.');
            }});
        });
    });
    </script>
</body>
</html>
"""

@app.route("/")
def index():
    _, _, dup_stats = load_duplicates(); _, crn_stats = load_crn_ratios()
    return render_template_string(HTML_TEMPLATE, display_cols=DISPLAY_COLS_PREFERRED, dup_stats=dup_stats, has_dup_data=True, crn_stats=crn_stats, has_crn_data=True)

@app.route("/api/duplicates")
def api_duplicates():
    rows, _, _ = load_duplicates(); start = int(flask_request.args.get("start", 0)); length = int(flask_request.args.get("length", 100)); search = flask_request.args.get("search[value]", "").strip().lower()
    filtered = [r for r in rows if any(search in str(v).lower() for v in r.values() if v)] if search else rows
    page = filtered[start:start + length]; result = []
    for i, row in enumerate(page):
        r = {k: (v if v else "-") for k, v in row.items() if not k.startswith("_")}
        r["_group_id"] = row.get("_group_id", 0); r["_row_num"] = start + i + 1; result.append(r)
    return jsonify({"draw": int(flask_request.args.get("draw", 1)), "recordsTotal": len(rows), "recordsFiltered": len(filtered), "data": result})

@app.route("/api/ratios")
def api_ratios():
    data, _ = load_crn_ratios(); r_type = flask_request.args.get("type", "crn"); start = int(flask_request.args.get("start", 0)); length = int(flask_request.args.get("length", 50)); search = flask_request.args.get("search[value]", "").strip().lower()
    sort_key = f"{r_type}_score"; filtered = [r for r in data if search in r.get("gstin", "").lower() or search in r.get("name", "").lower()] if search else data
    filtered = sorted(filtered, key=lambda x: x.get(sort_key, 0), reverse=True)
    page = filtered[start:start + length]; [p.update({"_row_num": start + i + 1}) for i, p in enumerate(page)]
    return jsonify({"draw": int(flask_request.args.get("draw", 1)), "recordsTotal": len(data), "recordsFiltered": len(filtered), "data": page})

@app.route("/api/upload-source", methods=["POST"])
def api_upload_source():
    flask_request.files['file'].save(SOURCE_CSV_PATH); return jsonify({"success": True})

@app.route("/api/run-analysis", methods=["POST"])
def api_run_analysis():
    threading.Thread(target=process_raw_csv, args=(SOURCE_CSV_PATH,)).start()
    return jsonify({"success": True})

@app.route("/api/status")
def api_status():
    return jsonify(_status)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
