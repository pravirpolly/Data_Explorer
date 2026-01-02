SOP:
* Create a folder ".streamlit" in the same location as app.py.
* Copy the "config.toml" to ".streamlit" folder.
"""
[theme]
primaryColor = "#FF4B4B"
backgroundColor = "#280f0fff"
secondaryBackgroundColor = "#41424aff"
textColor = "#d3d7ebff"
font = "sans serif"

[global]
developmentMode = false

[server]
maxUploadSize = 50000
headless = true
port = 8501
address = "127.0.0.1"
"""
* Open python environment and execute "streamlit run app.py"

Creation of Bash executable
* Move .streamlit, app.py, run.sh to the environment folder
* Open bash terminal bash terminal in the same folder and execute "chmod +x run.sh"
* Open the run.sh file by double clicking in folder.
"""
#!/usr/bin/env bash
set -e

# Move to project directory
cd "$(dirname "$0")"

# Activate virtualenv (adjust path if needed)
source Scripts/activate

# Run Streamlit
streamlit run app.py \
  --server.address=127.0.0.1 \
  --server.port=8501 \
  --server.headless=true \
  --browser.gatherUsageStats=false
"""


# app.py
# Data Explorer v2.4 — DuckDB backend + LXML XML + SQL Pivot + Progress + DESCRIBE (with data types)
#
# Adds:
# - Robust XML tag handling (_q_localname) to avoid cython_function_or_method QName.localname issues
# - "Describe" section: DuckDB DESCRIBE with column types + null %, distinct count (sampled/limited) + min/max for numerics
# - Progress bars for file read/load + pivot computations + describe computations
#
# Notes:
# - For large tables, describe stats can be expensive; this app computes distinct counts with a cap.
# - Upload limit: set .streamlit/config.toml with e.g. maxUploadSize = 5000 (MB)

import io
import os
import re
import tempfile
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd
import requests
import streamlit as st
from lxml import etree

st.set_page_config(page_title="Data Explorer v2.4 (DuckDB + LXML + Pivot + Describe)", layout="wide")


# -----------------------------
# DuckDB connection
# -----------------------------
@st.cache_resource
def get_con():
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")
    return con


con = get_con()

# -----------------------------
# Utilities
# -----------------------------
def safe_table_name(name: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", name.strip())
    if not name:
        name = "uploaded"
    if name[0].isdigit():
        name = f"t_{name}"
    return name.lower()


def quote_ident(col: str) -> str:
    return '"' + col.replace('"', '""') + '"'


def write_upload_to_temp(uploaded_file) -> str:
    suffix = os.path.splitext(uploaded_file.name)[1].lower()
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return path


def download_url_to_temp(
    url: str,
    progress: st.delta_generator.DeltaGenerator,
    status: st.delta_generator.DeltaGenerator
) -> Tuple[str, str]:
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()

    filename = "downloaded_file"
    cd = r.headers.get("content-disposition", "")
    if "filename=" in cd:
        filename = cd.split("filename=")[-1].strip().strip('"').strip("'")
    if filename == "downloaded_file":
        filename = os.path.basename(url.split("?")[0]) or filename

    suffix = os.path.splitext(filename)[1].lower()
    fd, path = tempfile.mkstemp(suffix=suffix if suffix else ".bin")
    os.close(fd)

    total = r.headers.get("content-length")
    total_bytes = int(total) if total and total.isdigit() else None

    downloaded = 0
    chunk_size = 8 * 1024 * 1024

    progress.progress(0)
    status.info("Downloading file...")

    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if not chunk:
                continue
            f.write(chunk)
            downloaded += len(chunk)

            if total_bytes:
                pct = min(int(downloaded * 100 / total_bytes), 100)
                progress.progress(pct)
                status.info(f"Downloading... {pct}% ({downloaded/1024/1024:.1f} MB / {total_bytes/1024/1024:.1f} MB)")
            else:
                pct = min(int((downloaded / (200 * 1024 * 1024)) * 100), 90)
                progress.progress(pct)
                status.info(f"Downloading... {downloaded/1024/1024:.1f} MB (total size unknown)")

    progress.progress(100)
    status.success("Download complete.")
    return path, filename


def get_excel_sheets(file_path: str, ext: str) -> List[str]:
    if ext == ".xlsx":
        xls = pd.ExcelFile(file_path, engine="openpyxl")
    else:
        xls = pd.ExcelFile(file_path, engine="xlrd")
    return list(xls.sheet_names)


def _q_localname(el) -> str:
    """
    lxml QName.localname is usually a string, but in some builds it can be a callable cython method.
    Normalize to string.
    """
    qn = etree.QName(el)
    ln = getattr(qn, "localname", None)
    if callable(ln):
        ln = ln()
    return str(ln) if ln is not None else str(el.tag)


def detect_xml_record_xpath(xml_path: str) -> str:
    parser = etree.XMLParser(recover=True, huge_tree=True)
    tree = etree.parse(xml_path, parser)
    root = tree.getroot()

    counts: Dict[str, int] = {}
    for el in root.iter():
        if el is root:
            continue
        tag = _q_localname(el)
        counts[tag] = counts.get(tag, 0) + 1

    if not counts:
        return ".//*"

    record_tag = max(counts.items(), key=lambda kv: kv[1])[0]
    return f".//{record_tag}"


def xml_nodes_to_df(xml_path: str, record_xpath: str) -> pd.DataFrame:
    parser = etree.XMLParser(recover=True, huge_tree=True)
    tree = etree.parse(xml_path, parser)
    root = tree.getroot()

    nodes = root.xpath(record_xpath)
    if not isinstance(nodes, list):
        nodes = [nodes]

    rows = []
    for node in nodes:
        if not isinstance(node, etree._Element):
            continue

        row = {}

        for k, v in node.attrib.items():
            row[f"@{k}"] = v

        for child in list(node):
            tag = _q_localname(child)

            text = (child.text or "").strip()
            if text:
                row[tag] = text

            for ak, av in child.attrib.items():
                row[f"{tag}.@{ak}"] = av

        if not row and ((node.text or "").strip() != ""):
            row["value"] = (node.text or "").strip()

        rows.append(row)

    return pd.DataFrame(rows)


def get_columns(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    return con.execute(f"DESCRIBE {table}").df()


def get_numeric_columns_from_describe(describe_df: pd.DataFrame) -> List[str]:
    numeric = []
    for _, r in describe_df.iterrows():
        t = str(r["column_type"]).upper()
        if any(k in t for k in ["INT", "DOUBLE", "DECIMAL", "REAL", "FLOAT", "HUGEINT", "UBIGINT", "SMALLINT", "TINYINT"]):
            numeric.append(r["column_name"])
    return numeric


def build_where_clause(filters: Dict[str, Dict]) -> Tuple[str, List]:
    clauses = []
    params: List = []
    for col, spec in filters.items():
        mode = spec.get("mode")
        if mode == "in":
            vals = spec.get("values", [])
            if vals:
                placeholders = ", ".join(["?"] * len(vals))
                clauses.append(f"{quote_ident(col)} IN ({placeholders})")
                params.extend(vals)
        elif mode == "contains":
            text = (spec.get("text") or "").strip()
            if text:
                clauses.append(f"LOWER(CAST({quote_ident(col)} AS VARCHAR)) LIKE LOWER(?)")
                params.append(f"%{text}%")
    if not clauses:
        return "", []
    return "WHERE " + " AND ".join(clauses), params


def load_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    file_path: str,
    file_name: str,
    progress: st.delta_generator.DeltaGenerator,
    status: st.delta_generator.DeltaGenerator,
    *,
    excel_sheet: Optional[str] = None,
    xml_record_xpath: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    ext = os.path.splitext(file_name)[1].lower()
    base = os.path.splitext(file_name)[0]
    table = safe_table_name(base)

    try:
        progress.progress(5)
        status.info("Preparing DuckDB table...")
        con.execute(f"DROP TABLE IF EXISTS {table}")

        progress.progress(15)
        status.info("Loading data into DuckDB...")

        if ext == ".csv":
            con.execute(f"""
                CREATE TABLE {table} AS
                SELECT * FROM read_csv(
                    '{file_path}',
                    auto_detect=true,
                    header=true,
                    sample_size=-1,
                    ignore_errors=true
                );
            """)
        elif ext == ".parquet":
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{file_path}');")
        elif ext == ".json":
            con.execute(f"""
                CREATE TABLE {table} AS
                SELECT * FROM read_json(
                    '{file_path}',
                    auto_detect=true,
                    sample_size=-1
                );
            """)
        elif ext in [".xlsx", ".xls"]:
            status.info("Reading Excel via pandas (sheet selection) ...")
            progress.progress(20)
            if excel_sheet is None:
                excel_sheet = 0
            if ext == ".xlsx":
                df = pd.read_excel(file_path, sheet_name=excel_sheet, engine="openpyxl")
            else:
                df = pd.read_excel(file_path, sheet_name=excel_sheet, engine="xlrd")
            progress.progress(50)
            status.info("Registering dataframe into DuckDB...")
            con.register("tmp_df", df)
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM tmp_df;")
            con.unregister("tmp_df")
        elif ext == ".txt":
            con.execute(f"""
                CREATE TABLE {table} AS
                SELECT * FROM read_csv(
                    '{file_path}',
                    auto_detect=true,
                    delim='\t',
                    sample_size=-1,
                    ignore_errors=true
                );
            """)
        elif ext == ".xml":
            status.info("Parsing XML with lxml...")
            progress.progress(20)
            if not xml_record_xpath:
                xml_record_xpath = detect_xml_record_xpath(file_path)
            df = xml_nodes_to_df(file_path, xml_record_xpath)
            progress.progress(60)
            status.info("Registering XML dataframe into DuckDB...")
            con.register("tmp_df", df)
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM tmp_df;")
            con.unregister("tmp_df")
        else:
            return None, f"Unsupported file type: {ext}"

        progress.progress(90)
        status.info("Finalizing...")
        con.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchall()

        progress.progress(100)
        status.success("Load complete.")
        return table, None

    except Exception as e:
        return None, str(e)


def build_pivot_query(
    con: duckdb.DuckDBPyConnection,
    table: str,
    row_cols: List[str],
    col_cols: List[str],
    measures: List[Tuple[str, str]],
    where_sql: str,
    where_params: List,
    progress: st.delta_generator.DeltaGenerator,
    status: st.delta_generator.DeltaGenerator,
    *,
    max_pivot_cols: int
) -> Tuple[str, List, int]:
    row_cols = row_cols or []

    progress.progress(5)
    status.info("Preparing pivot configuration...")

    if col_cols:
        parts = [f"COALESCE(CAST({quote_ident(c)} AS VARCHAR), '')" for c in col_cols]
        col_key_expr = parts[0] if len(parts) == 1 else " || ' | ' || ".join(parts)
    else:
        col_key_expr = None

    progress.progress(20)
    status.info("Discovering pivot column keys (distinct)...")

    col_keys: List[str] = []
    total_keys = 0
    if col_key_expr is not None:
        key_sql = f"SELECT DISTINCT {col_key_expr} AS _k FROM {table} {where_sql} ORDER BY _k"
        keys_df = con.execute(key_sql, where_params).df()
        col_keys = keys_df["_k"].astype(str).tolist()
        total_keys = len(col_keys)
        if len(col_keys) > max_pivot_cols:
            col_keys = col_keys[:max_pivot_cols]

    def agg_expr(agg: str, expr: str) -> str:
        a = agg.lower()
        if a == "count_distinct":
            return f"COUNT(DISTINCT {expr})"
        if a == "count":
            return f"COUNT({expr})"
        if a in ("avg", "mean"):
            return f"AVG({expr})"
        if a == "sum":
            return f"SUM({expr})"
        if a == "min":
            return f"MIN({expr})"
        if a == "max":
            return f"MAX({expr})"
        return f"SUM({expr})"

    progress.progress(45)
    status.info("Generating pivot SQL...")

    select_parts: List[str] = []
    group_by_parts: List[str] = []

    for c in row_cols:
        select_parts.append(quote_ident(c))
        group_by_parts.append(quote_ident(c))

    params: List = list(where_params)

    if col_key_expr is None:
        for agg, vcol in measures:
            base_expr = "1" if vcol == "__rowcount__" else quote_ident(vcol)
            alias = f"{agg.lower()}_{'rows' if vcol=='__rowcount__' else vcol}"
            select_parts.append(f"{agg_expr(agg, base_expr)} AS {quote_ident(alias)}")

        sql = f"SELECT {', '.join(select_parts)} FROM {table} {where_sql}"
        if group_by_parts:
            sql += f" GROUP BY {', '.join(group_by_parts)} ORDER BY {', '.join(group_by_parts)}"

        progress.progress(60)
        status.info("Pivot SQL ready.")
        return sql, params, total_keys

    key_params: List[str] = []
    for key in col_keys:
        for agg, vcol in measures:
            base_expr = "1" if vcol == "__rowcount__" else quote_ident(vcol)
            case_expr = f"CASE WHEN ({col_key_expr}) = ? THEN {base_expr} ELSE NULL END"
            key_params.append(str(key))

            alias = f"{key}__{agg.lower()}_{'rows' if vcol=='__rowcount__' else vcol}"
            alias = alias[:240]
            select_parts.append(f"{agg_expr(agg, case_expr)} AS {quote_ident(alias)}")

    params = params + key_params

    sql = f"SELECT {', '.join(select_parts)} FROM {table} {where_sql}"
    if group_by_parts:
        sql += f" GROUP BY {', '.join(group_by_parts)} ORDER BY {', '.join(group_by_parts)}"

    progress.progress(60)
    status.info("Pivot SQL ready.")
    return sql, params, total_keys


# -----------------------------
# NEW: Describe / profiling
# -----------------------------
def describe_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    describe_df: pd.DataFrame,
    progress: st.delta_generator.DeltaGenerator,
    status: st.delta_generator.DeltaGenerator,
    *,
    distinct_cap: int = 50000
) -> pd.DataFrame:
    """
    Builds a describe-like dataframe with:
      - column_name, column_type (DuckDB)
      - null_count, null_pct
      - approx_distinct (exact up to distinct_cap, else returns ">cap" marker)
      - min/max for numeric types
    """
    status.info("Computing table row count...")
    progress.progress(5)
    total_rows = con.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()[0]
    progress.progress(15)

    rows = []
    numeric_cols = set(get_numeric_columns_from_describe(describe_df))
    cols = describe_df["column_name"].tolist()

    status.info("Computing column statistics...")
    for i, col in enumerate(cols):
        # progress: 15..95
        pct = 15 + int((i / max(len(cols), 1)) * 80)
        progress.progress(min(pct, 95))

        col_type = describe_df.loc[describe_df["column_name"] == col, "column_type"].iloc[0]

        # Null count
        null_count = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {quote_ident(col)} IS NULL"
        ).fetchone()[0]
        null_pct = (null_count / total_rows * 100) if total_rows else 0.0

        # Distinct count: exact but capped via "LIMIT cap+1" on distinct list to avoid heavy counts
        # This avoids a full COUNT(DISTINCT) on extremely wide/high-card columns.
        # Strategy: materialize distinct up to cap+1, then decide.
        distinct_sql = f"""
            SELECT COUNT(*) AS c
            FROM (SELECT DISTINCT {quote_ident(col)} FROM {table} LIMIT {distinct_cap + 1})
        """
        dc = con.execute(distinct_sql).fetchone()[0]
        distinct_display = f">{distinct_cap}" if dc > distinct_cap else str(dc)

        # Min/max for numeric
        min_val = None
        max_val = None
        if col in numeric_cols:
            mm = con.execute(
                f"SELECT MIN({quote_ident(col)}), MAX({quote_ident(col)}) FROM {table}"
            ).fetchone()
            min_val, max_val = mm[0], mm[1]

        rows.append(
            {
                "column_name": col,
                "duckdb_type": col_type,
                "rows": total_rows,
                "null_count": null_count,
                "null_pct": round(null_pct, 2),
                "distinct_count": distinct_display,
                "min": min_val,
                "max": max_val,
            }
        )

    progress.progress(100)
    status.success("Describe complete.")
    return pd.DataFrame(rows)


# -----------------------------
# UI
# -----------------------------
st.title("Data Explorer v2.4 — Add DESCRIBE (data types)")

with st.sidebar:
    st.header("Ingestion mode")
    mode = st.radio("Choose input", options=["Upload file", "Load from URL", "Load from local path"], index=0)

    st.divider()
    st.header("Preview / Performance")
    preview_limit = st.number_input("Preview row limit", min_value=50, max_value=20000, value=1000, step=50)
    max_pivot_cols = st.number_input("Max pivot columns (distinct column keys)", min_value=10, max_value=500, value=60, step=10)

    st.divider()
    st.header("Describe settings")
    distinct_cap = st.number_input(
        "Distinct cap (per column)",
        min_value=1000,
        max_value=1_000_000,
        value=50_000,
        step=1000,
        help="If distinct exceeds cap, app shows >cap. Larger values can be expensive."
    )

    st.divider()
    st.header("SQL (Optional)")
    sql_text = st.text_area("SQL", value="", height=140, placeholder="SELECT * FROM my_table LIMIT 100;")

uploaded = None
file_path = None
file_name = None
ext = None

excel_sheet = None
xml_record_xpath = None

progress_container = st.container()
pb = progress_container.progress(0)
status = progress_container.empty()

# Ingestion
if mode == "Upload file":
    uploaded = st.file_uploader(
        "Upload: xlsx, xls, csv, json, parquet, txt, xml",
        type=["xlsx", "xls", "csv", "json", "parquet", "txt", "xml"],
        accept_multiple_files=False
    )
    if uploaded is not None:
        pb.progress(5)
        status.info("Receiving upload...")
        file_path = write_upload_to_temp(uploaded)
        file_name = uploaded.name
        pb.progress(10)
        status.info("Upload received; preparing to load...")

elif mode == "Load from URL":
    url = st.text_input("File URL (direct download link)")
    if st.button("Download and load"):
        if not url.strip():
            st.error("Please provide a URL.")
        else:
            try:
                file_path, file_name = download_url_to_temp(url.strip(), pb, status)
                pb.progress(0)
            except Exception as e:
                st.error(f"Download failed: {e}")

elif mode == "Load from local path":
    p = st.text_input("Local file path on server/machine")
    if st.button("Load path"):
        if not p.strip():
            st.error("Please provide a path.")
        elif not os.path.exists(p.strip()):
            st.error("File not found at that path.")
        else:
            file_path = p.strip()
            file_name = os.path.basename(file_path)
            pb.progress(10)
            status.info("Path accepted; preparing to load...")

# Load
table = None
table_loaded = False

if file_path and file_name:
    ext = os.path.splitext(file_name)[1].lower()

    with st.sidebar:
        st.divider()
        st.header("File options")

        if ext in [".xlsx", ".xls"]:
            try:
                sheets = get_excel_sheets(file_path, ext)
                excel_sheet = st.selectbox("Excel sheet", options=sheets, index=0)
            except Exception as e:
                st.error(f"Could not read Excel sheets: {e}")

        if ext == ".xml":
            suggested = detect_xml_record_xpath(file_path)
            st.subheader("XML parsing (LXML)")
            xml_record_xpath = st.text_input("Record XPath", value=suggested)

    pb.progress(0)
    status.info("Starting load...")
    table, err = load_to_duckdb(
        con,
        file_path,
        file_name,
        pb,
        status,
        excel_sheet=excel_sheet,
        xml_record_xpath=xml_record_xpath,
    )

    if err:
        status.empty()
        pb.empty()
        st.error(f"Load failed: {err}")
    else:
        table_loaded = True
        st.success(f"Loaded into DuckDB table: `{table}`")

if not table_loaded or not table:
    st.info("Provide an input file (upload / URL / local path) to begin.")
    st.stop()

# Schema
pb.progress(0)
status.info("Reading schema...")
describe_df = get_columns(con, table)  # DuckDB DESCRIBE output
pb.progress(30)

# Preview
status.info("Running preview query...")
preview_df = con.execute(f"SELECT * FROM {table} LIMIT {int(preview_limit)}").df()
pb.progress(100)
status.success("Ready.")

st.subheader("Preview (Scrollable)")
st.dataframe(preview_df, use_container_width=True, height=420)
st.caption(f"Table: {table} | Columns: {len(describe_df)} | Preview rows: {len(preview_df)}")

# -----------------------------
# NEW: Describe section
# -----------------------------
st.divider()
st.subheader("Describe (DuckDB data types + basic profiling)")

show_describe = st.button("Compute Describe", type="secondary")
if show_describe:
    d_pb = st.progress(0)
    d_status = st.empty()
    try:
        profile_df = describe_table(
            con, table, describe_df, d_pb, d_status, distinct_cap=int(distinct_cap)
        )
        st.dataframe(profile_df, use_container_width=True, height=420)

        # Download
        out = io.BytesIO()
        profile_df.to_excel(out, index=False, engine="openpyxl")
        st.download_button(
            "Download Describe as Excel",
            data=out.getvalue(),
            file_name="describe.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        with st.expander("Raw DuckDB DESCRIBE output", expanded=False):
            st.dataframe(describe_df, use_container_width=True, height=220)

    except Exception as e:
        d_pb.empty()
        d_status.empty()
        st.error(f"Describe failed: {e}")

# Optional SQL Runner
if sql_text.strip():
    st.divider()
    st.subheader("SQL Result (Scrollable)")
    sql_pb = st.progress(0)
    sql_status = st.empty()
    try:
        sql_status.info("Executing SQL...")
        sql_pb.progress(40)
        qdf = con.execute(sql_text).df()
        sql_pb.progress(100)
        sql_status.success("SQL complete.")
        st.dataframe(qdf, use_container_width=True, height=420)
    except Exception as e:
        sql_pb.empty()
        sql_status.empty()
        st.error(f"SQL error: {e}")

# Pivot Builder
st.divider()
st.subheader("Pivot Builder (DuckDB SQL)")

cols = describe_df["column_name"].tolist()
numeric_cols = get_numeric_columns_from_describe(describe_df)

with st.expander("Filters (DuckDB WHERE)", expanded=False):
    filter_cols = st.multiselect("Filter columns", options=cols, default=[])
    max_distinct_filter_values = st.number_input(
        "Max distinct values to enumerate for filter UI (per column)",
        min_value=50,
        max_value=100_000,
        value=10_000,
        step=500
    )

    filters: Dict[str, Dict] = {}
    f_pb = st.progress(0) if filter_cols else None
    f_status = st.empty() if filter_cols else None

    for i, fc in enumerate(filter_cols):
        if f_status and f_pb:
            f_status.info(f"Computing filter options for: {fc}")
            f_pb.progress(int((i / max(len(filter_cols), 1)) * 70) + 5)

        try:
            dcount = con.execute(f"SELECT COUNT(DISTINCT {quote_ident(fc)}) FROM {table}").fetchone()[0]
        except Exception:
            dcount = None

        if (dcount is not None) and (dcount <= int(max_distinct_filter_values)):
            dvals = con.execute(
                f"SELECT DISTINCT CAST({quote_ident(fc)} AS VARCHAR) AS v FROM {table} ORDER BY v"
            ).df()["v"].tolist()
            keep = st.multiselect(f"Keep values for {fc}", options=dvals, default=dvals, key=f"keep_{fc}")
            filters[fc] = {"mode": "in", "values": keep}
        else:
            txt = st.text_input(f"{fc} contains (case-insensitive)", value="", key=f"contains_{fc}")
            filters[fc] = {"mode": "contains", "text": txt}

    if f_status and f_pb:
        f_pb.progress(100)
        f_status.success("Filter setup complete.")

    where_sql, where_params = build_where_clause(filters)

left, right = st.columns([1, 1])
with left:
    row_cols = st.multiselect("Rows (Group by)", options=cols, default=[])
    col_cols = st.multiselect("Columns (Pivot across)", options=cols, default=[])

with right:
    available_value_cols = ["__rowcount__"] + (numeric_cols if numeric_cols else cols)
    value_cols = st.multiselect("Value fields", options=available_value_cols, default=["__rowcount__"])
    agg_options = ["sum", "avg", "min", "max", "count", "count_distinct"]

    measures: List[Tuple[str, str]] = []
    for vc in value_cols:
        default_agg = "count" if vc == "__rowcount__" else "sum"
        idx = agg_options.index(default_agg)
        agg = st.selectbox(f"Aggregation for {vc}", options=agg_options, index=idx, key=f"agg_{vc}")
        measures.append((agg, vc))

if st.button("Create Pivot", type="primary"):
    pivot_pb = st.progress(0)
    pivot_status = st.empty()

    try:
        pivot_sql, pivot_params, total_keys = build_pivot_query(
            con,
            table,
            row_cols=row_cols,
            col_cols=col_cols,
            measures=measures,
            where_sql=where_sql,
            where_params=where_params,
            progress=pivot_pb,
            status=pivot_status,
            max_pivot_cols=int(max_pivot_cols),
        )

        if col_cols and total_keys > int(max_pivot_cols):
            st.warning(f"Pivot column keys: {total_keys}. Limited to first {int(max_pivot_cols)}.")

        pivot_status.info("Executing pivot query...")
        pivot_pb.progress(75)
        pivot_df = con.execute(pivot_sql, pivot_params).df()
        pivot_pb.progress(95)

        pivot_status.info("Preparing downloads...")
        out_xlsx = io.BytesIO()
        pivot_df.to_excel(out_xlsx, index=False, engine="openpyxl")
        pivot_pb.progress(100)
        pivot_status.success("Pivot complete.")

        st.markdown("**Pivot Result (Scrollable)**")
        st.dataframe(pivot_df, use_container_width=True, height=420)
        st.caption(f"Pivot rows: {pivot_df.shape[0]} | Pivot columns: {pivot_df.shape[1]}")

        st.download_button(
            "Download Pivot as Excel",
            data=out_xlsx.getvalue(),
            file_name="pivot_v2_4.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        out_csv = pivot_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download Pivot as CSV",
            data=out_csv,
            file_name="pivot_v2_4.csv",
            mime="text/csv",
        )

        with st.expander("Show generated Pivot SQL", expanded=False):
            st.code(pivot_sql.strip(), language="sql")

    except Exception as e:
        pivot_pb.empty()
        pivot_status.empty()
        st.error(f"Pivot failed: {e}")


