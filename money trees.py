import json
from datetime import date, datetime
from pathlib import Path
import base64
from io import BytesIO
from urllib.parse import quote, unquote
from html import escape

import gspread
from google.oauth2.service_account import Credentials

import streamlit as st
import plotly.express as px
import pandas as pd
from PIL import Image
import threading
import copy


st.set_page_config(layout="wide")

ASSETS_DIR = Path(__file__).with_name("assets")

GOOGLE_CREDENTIALS_PATH = Path(__file__).with_name("google_credentials.json")
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
_WORKSHEET = None


def _get_worksheet():
    global _WORKSHEET
    if _WORKSHEET is not None:
        return _WORKSHEET

    # שינוי קריטי: קריאת הנתונים מהסודות של Streamlit במקום מהקובץ
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=SCOPE,
        )
        gc = gspread.authorize(credentials)

        spreadsheet = gc.open("ExpenseTrackerDB")
        _WORKSHEET = spreadsheet.sheet1
        return _WORKSHEET
    except Exception as e:
        st.error(f"שגיאה בחיבור לגוגל: {e}")
        raise e

def load_data() -> dict:
    worksheet = _get_worksheet()
    try:
        # קורא את כל העמודה הראשונה (A) במכה אחת
        col_a_values = worksheet.col_values(1)
        # מחבר את כל חלקי הטקסט חזרה למחרוזת אחת ארוכה
        raw_value = "".join(col_a_values)
    except Exception:
        raw_value = ""

    if not raw_value.strip():
        return {"last_active_user": "", "users": {}}
    try:
        data = json.loads(raw_value)
        if not isinstance(data, dict):
            return {"last_active_user": "", "users": {}}
        data.setdefault("last_active_user", "")
        data.setdefault("users", {})
        return data
    except json.JSONDecodeError:
        return {"last_active_user": "", "users": {}}


def _background_save(data_snapshot: dict) -> None:
    try:
        worksheet = _get_worksheet()
        json_string = json.dumps(data_snapshot, ensure_ascii=False)
        
        chunk_size = 40000
        chunks = [json_string[i:i + chunk_size] for i in range(0, len(json_string), chunk_size)]
        
        values_to_update = [[chunk] for chunk in chunks]
        
        worksheet.clear()
        worksheet.update(values=values_to_update, range_name=f"A1:A{len(chunks)}")
    except Exception as e:
        print(f"Background save failed: {e}")

def _background_save(data_snapshot: dict) -> None:
    try:
        worksheet = _get_worksheet()
        json_string = json.dumps(data_snapshot, ensure_ascii=False)
        
        chunk_size = 40000
        chunks = [json_string[i:i + chunk_size] for i in range(0, len(json_string), chunk_size)]
        
        values_to_update = [[chunk] for chunk in chunks]
        
        worksheet.clear()
        worksheet.update(values=values_to_update, range_name=f"A1:A{len(chunks)}")
    except Exception as e:
        print(f"Background save failed: {e}")

def save_data(data: dict) -> None:
    # 1. שומרים תמונת מצב מדויקת של הנתונים ברגע הלחיצה
    data_snapshot = copy.deepcopy(data)
    
    # 2. פותחים "נתיב צדדי" (Thread) ששומר לגוגל, ונותנים לאפליקציה המרכזית להמשיך מיד
    thread = threading.Thread(target=_background_save, args=(data_snapshot,))
    thread.start()


def file_to_data_uri(file_path: Path) -> str:
    file_bytes = file_path.read_bytes()
    mime_type = "image/png" if file_path.suffix.lower() == ".png" else "image/jpeg"
    encoded_bytes = base64.b64encode(file_bytes).decode("utf-8")
    return f"data:{mime_type};base64,{encoded_bytes}"


def get_tinted_color_from_image_bytes(image_bytes: bytes) -> tuple[int, int, int]:
    image = Image.open(BytesIO(image_bytes)).convert("RGB")
    image.thumbnail((120, 120))
    pixels = list(image.getdata())
    if not pixels:
        return (245, 245, 245)
    avg_r = int(sum(pixel[0] for pixel in pixels) / len(pixels))
    avg_g = int(sum(pixel[1] for pixel in pixels) / len(pixels))
    avg_b = int(sum(pixel[2] for pixel in pixels) / len(pixels))
    mixed_r = int(avg_r * 0.25 + 255 * 0.75)
    mixed_g = int(avg_g * 0.25 + 255 * 0.75)
    mixed_b = int(avg_b * 0.25 + 255 * 0.75)
    return (mixed_r, mixed_g, mixed_b)


def get_tinted_color_from_data_uri(data_uri: str) -> tuple[int, int, int]:
    if not data_uri.startswith("data:image"):
        return (245, 245, 245)
    try:
        encoded_part = data_uri.split(",", 1)[1]
        image_bytes = base64.b64decode(encoded_part)
        return get_tinted_color_from_image_bytes(image_bytes)
    except (IndexError, ValueError):
        return (245, 245, 245)


def ensure_user_data(app_data: dict, username: str) -> dict:
    users = app_data.setdefault("users", {})
    if username not in users:
        users[username] = {
            "settings": {"currency": "$", "background_image": "", "background_tint": [245, 245, 245]},
            "month_settings": {},
            "monthly_expenses": {},
            "standing_orders": [],
        }
    user_data = users[username]
    user_data.setdefault("settings", {})
    user_data["settings"].setdefault("currency", "$")
    user_data["settings"].setdefault("background_image", "")
    user_data["settings"].setdefault("background_tint", [245, 245, 245])
    user_data.setdefault("month_settings", {})
    user_data.setdefault("monthly_expenses", {})
    user_data.setdefault("standing_orders", [])
    return user_data


def is_standing_order_active(order: dict, target_year: int, target_month: int) -> bool:
    start_date = datetime.fromisoformat(order["start_date"]).date()
    end_date = datetime.fromisoformat(order["end_date"]).date()
    target_period = target_year * 12 + target_month
    start_period = start_date.year * 12 + start_date.month
    end_period = end_date.year * 12 + end_date.month
    if target_period < start_period or target_period > end_period:
        
        return False
    if order["frequency"] == "Monthly":
        return True
    return target_month == start_date.month


hebrew_category_map = {
    "Expenses": "הוצאות",
    "Remaining Income": "הכנסה נותרת",
    "Remaining Budget": "תקציב נותר",
    "Deficit (Over Budget)": "חריגה מהתקציב",
}

english_months = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
year_options = list(range(2025, 2101))


# טעינת נתונים חכמה - פונה לגוגל רק פעם אחת
if "app_data" not in st.session_state:
    st.session_state["app_data"] = load_data()

app_data = st.session_state["app_data"]

st.sidebar.header("Global Settings")
available_users = list(app_data.get("users", {}).keys())
new_user_name = st.sidebar.text_input("New User Name")
if st.sidebar.button("Add User"):
    clean_name = new_user_name.strip()
    if clean_name and clean_name not in available_users:
        ensure_user_data(app_data, clean_name)
        app_data["last_active_user"] = clean_name
        save_data(app_data)
        st.session_state["selected_user"] = clean_name
        st.rerun()

if not available_users:
    st.sidebar.info("Create your first profile to continue.")
    st.info("Please create your first user profile from the sidebar.")
    st.stop()

last_active_user = app_data.get("last_active_user", "")
if "selected_user" not in st.session_state or st.session_state["selected_user"] not in available_users:
    if last_active_user in available_users:
        st.session_state["selected_user"] = last_active_user
    else:
        st.session_state["selected_user"] = available_users[0]

selected_user = st.sidebar.selectbox(
    "Select User",
    options=available_users,
    index=available_users.index(st.session_state["selected_user"]),
)
st.session_state["selected_user"] = selected_user
if app_data.get("last_active_user") != selected_user:
    app_data["last_active_user"] = selected_user
    save_data(app_data)

if st.sidebar.button("Delete Current User"):
    app_data["users"].pop(selected_user, None)
    remaining_users = list(app_data["users"].keys())
    app_data["last_active_user"] = remaining_users[0] if remaining_users else ""
    st.session_state.pop("selected_user", None)
    save_data(app_data)
    st.rerun()

user_data = ensure_user_data(app_data, selected_user)
user_settings = user_data["settings"]

currency_options = ["₪", "$", "€", "£"]
currency_default = user_settings.get("currency", "$")
currency = st.sidebar.selectbox(
    "Currency",
    options=currency_options,
    index=currency_options.index(currency_default) if currency_default in currency_options else 1,
)
if user_settings.get("currency") != currency:
    user_settings["currency"] = currency
    save_data(app_data)

with st.sidebar.expander("Background Settings", expanded=False):
    st.markdown("**Preset Gallery**")
    preset_files = []
    if ASSETS_DIR.exists() and ASSETS_DIR.is_dir():
        preset_files = sorted(
            [
                file_path
                for file_path in ASSETS_DIR.iterdir()
                if file_path.is_file() and file_path.suffix.lower() in [".png", ".jpg", ".jpeg"]
            ]
        )

    selected_preset_from_query = st.query_params.get("preset_bg")
    if selected_preset_from_query:
        selected_preset_name = unquote(selected_preset_from_query)
        selected_file = ASSETS_DIR / selected_preset_name
        if selected_file.exists() and selected_file.is_file():
            selected_background = file_to_data_uri(selected_file)
            user_settings["background_image"] = selected_background
            user_settings["background_tint"] = list(
                get_tinted_color_from_image_bytes(selected_file.read_bytes())
            )
            save_data(app_data)
        st.query_params.clear()
        st.rerun()

    gallery_html = "<div style='display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; margin-bottom: 10px;'>"
    for file_path in preset_files:
        thumbnail_data_uri = file_to_data_uri(file_path)
        encoded_name = quote(file_path.name)
        gallery_html += (
            "<a href='?preset_bg="
            f"{encoded_name}"
            "' style='display:block; width:100px; height:100px; border-radius:10px; overflow:hidden; border:2px solid rgba(0,0,0,0.15);'>"
            f"<img src='{thumbnail_data_uri}' alt='{escape(file_path.name)}' style='width:100%; height:100%; object-fit:cover;'/>"
            "</a>"
        )
    gallery_html += "</div>"
    if preset_files:
        st.markdown(gallery_html, unsafe_allow_html=True)
    else:
        st.caption("No preset images found in the assets folder.")

    background_image_url = st.text_input("Image URL")
    background_image_file = st.file_uploader("Upload Local Image", type=["png", "jpg", "jpeg"])

    background_image = ""
    if background_image_file is not None:
        file_bytes = background_image_file.read()
        file_extension = background_image_file.type or "image/png"
        image_base64 = base64.b64encode(file_bytes).decode("utf-8")
        background_image = f"data:{file_extension};base64,{image_base64}"
    elif background_image_url.strip():
        background_image = background_image_url.strip()
    else:
        background_image = user_settings.get("background_image", "")

    if st.button("Apply Background"):
        user_settings["background_image"] = background_image
        if background_image_file is not None:
            user_settings["background_tint"] = list(get_tinted_color_from_image_bytes(file_bytes))
        elif background_image.startswith("data:image"):
            user_settings["background_tint"] = list(get_tinted_color_from_data_uri(background_image))
        else:
            user_settings["background_tint"] = [245, 245, 245]
        save_data(app_data)
        st.rerun()
    if st.button("Clear Background"):
        user_settings["background_image"] = ""
        user_settings["background_tint"] = [245, 245, 245]
        save_data(app_data)
        st.rerun()

if user_settings.get("background_image") and not user_settings.get("background_tint"):
    user_settings["background_tint"] = list(
        get_tinted_color_from_data_uri(user_settings.get("background_image", ""))
    )
    save_data(app_data)

if user_settings.get("background_image"):
    st.markdown(
        f"""
        <style>
        .stApp, [data-testid="stAppViewContainer"] {{
            background-image: url("{user_settings['background_image']}");
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
            filter: none !important;
            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
            image-rendering: high-quality;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

tint_r, tint_g, tint_b = user_settings.get("background_tint", [245, 245, 245])
st.markdown(
    f"""
    <style>
    .block-container {{
        background-color: rgba({tint_r}, {tint_g}, {tint_b}, 0.85);
        padding: 1.5rem 2rem;
        border-radius: 15px;
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.18);
    }}
    .block-container, .block-container * {{
        color: #1f1f1f !important;
    }}
    .center-grid {{
        text-align: center;
        display: flex;
        justify-content: center;
        align-items: center;
        min-height: 38px;
        width: 100%;
    }}
    div[data-testid="stButton"] button[kind="primary"] {{
        min-height: 1.8rem;
        padding: 0.1rem 0.45rem;
        font-size: 0.85rem;
        line-height: 1;
        background: transparent;
        color: #8b0000;
        border: 1px solid #bdbdbd;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

selected_year = st.selectbox("Select Year", options=year_options, index=1)
selected_month = st.radio(
    label="Select Month",
    options=english_months,
    horizontal=True,
    label_visibility="collapsed",
)
st.markdown(
    f"<h1 style='text-align: center; margin-bottom: 1rem;'>Expense Tracker - {selected_month} {selected_year}</h1>",
    unsafe_allow_html=True,
)

month_state_key = f"{selected_year}-{selected_month}"
month_index = english_months.index(selected_month) + 1
user_data["monthly_expenses"].setdefault(month_state_key, [])
expenses_for_month = user_data["monthly_expenses"][month_state_key]

current_settings = user_data["month_settings"].get(month_state_key)
is_confirmed = current_settings is not None
default_status = "Working Month" if not is_confirmed else current_settings["employment_status"]
default_funds = 0.0 if not is_confirmed else float(current_settings["available_funds"])

settings_container = st.container() if not is_confirmed else st.expander("⚙️ Edit Month Settings", expanded=False)
with settings_container:
    employment_status = st.radio(
        "Employment Status",
        options=["Working Month", "Not Working Month"],
        horizontal=True,
        index=0 if default_status == "Working Month" else 1,
        key=f"employment_status_{selected_user}_{month_state_key}",
    )
    funds_label = "Total Income" if employment_status == "Working Month" else "Monthly Budget"
    available_funds = st.number_input(
        f"{funds_label} ({currency})",
        min_value=0.0,
        step=1.0,
        value=float(default_funds),
        key=f"funds_input_{selected_user}_{month_state_key}",
    )
    if st.button("Confirm", key=f"confirm_{selected_user}_{month_state_key}"):
        user_data["month_settings"][month_state_key] = {
            "employment_status": employment_status,
            "available_funds": float(available_funds),
        }
        save_data(app_data)
        st.rerun()

current_settings = user_data["month_settings"].get(month_state_key)
if current_settings:
    effective_status = current_settings["employment_status"]
    base_funds = float(current_settings["available_funds"])
    st.markdown(f"### Base Available Funds: {currency}{base_funds:,.2f}")
else:
    effective_status = employment_status
    base_funds = float(available_funds)
    st.markdown("### Base Available Funds: Not confirmed yet")

active_standing_orders = [
    order
    for order in user_data["standing_orders"]
    if is_standing_order_active(order, selected_year, month_index)
]
standing_orders_total = sum(float(order["amount"]) for order in active_standing_orders)

expenses_tab, standing_orders_tab = st.tabs(["Expenses", "Standing Orders"])

with expenses_tab:
    left_col, right_col = st.columns([1.15, 1], gap="large")
    with left_col:
        st.subheader("Add Transaction")
        transaction_name = st.text_input("Transaction Name", key=f"expense_name_{selected_user}_{month_state_key}")
        transaction_type = st.selectbox(
            "Transaction Type",
            options=["Expense", "Income"],
            key=f"transaction_type_{selected_user}_{month_state_key}",
        )
        transaction_amount = st.number_input(
            f"Amount ({currency})",
            min_value=0.0,
            step=1.0,
            value=0.0,
            key=f"expense_amount_{selected_user}_{month_state_key}",
        )
        if st.button("Add Transaction", key=f"add_expense_{selected_user}_{month_state_key}"):
            if transaction_name.strip() and transaction_amount > 0:
                expenses_for_month.append(
                    {
                        "name": transaction_name.strip(),
                        "amount": float(transaction_amount),
                        "Type": transaction_type,
                    }
                )
                save_data(app_data)
                st.rerun()
            else:
                st.warning("Please enter a valid transaction name and amount greater than 0.")

        st.subheader("Transaction List")
        if expenses_for_month:
            header_col1, header_col2, header_col3, header_col4 = st.columns([5, 2, 2, 1])
            header_col1.markdown("<div class='center-grid'><strong>Name</strong></div>", unsafe_allow_html=True)
            header_col2.markdown(
                f"<div class='center-grid'><strong>Amount ({currency})</strong></div>",
                unsafe_allow_html=True,
            )
            header_col3.markdown("<div class='center-grid'><strong>Type</strong></div>", unsafe_allow_html=True)
            header_col4.markdown("<div class='center-grid'><strong>Action</strong></div>", unsafe_allow_html=True)
            with st.container(height=350):
                for idx, expense in enumerate(expenses_for_month):
                    item_type = expense.get("Type", "Expense")
                    row_1, row_2, row_3, row_4 = st.columns([5, 2, 2, 1])
                    row_1.markdown(
                        f"<div class='center-grid'>{expense['name']}</div>",
                        unsafe_allow_html=True,
                    )
                    if item_type == "Income":
                        amount_html = f"<span style='color: #22c55e; font-weight: bold;'>+{expense['amount']:.1f}</span>"
                    else:
                        amount_html = f"<span style='color: #ef4444; font-weight: bold;'>{expense['amount']:.1f}</span>"
                    row_2.markdown(
                        f"<div class='center-grid'>{amount_html}</div>",
                        unsafe_allow_html=True,
                    )
                    row_3.markdown(f"<div class='center-grid'>{item_type}</div>", unsafe_allow_html=True)
                    if row_4.button(
                        "❌",
                        key=f"delete_expense_{selected_user}_{month_state_key}_{idx}",
                        type="primary",
                    ):
                        expenses_for_month.pop(idx)
                        save_data(app_data)
                        st.rerun()
        else:
            st.info("No expenses added for this month yet.")

        if active_standing_orders:
            st.subheader("Active Standing Orders This Month")
            s_header1, s_header2, s_header3, s_header4 = st.columns([5, 2, 2, 1])
            s_header1.markdown("<div class='center-grid'><strong>Standing Order</strong></div>", unsafe_allow_html=True)
            s_header2.markdown(
                f"<div class='center-grid'><strong>Amount ({currency})</strong></div>",
                unsafe_allow_html=True,
            )
            s_header3.markdown("<div class='center-grid'><strong>Frequency</strong></div>", unsafe_allow_html=True)
            s_header4.markdown("<div class='center-grid'><strong>Action</strong></div>", unsafe_allow_html=True)
            for active_idx, order in enumerate(active_standing_orders):
                s_row1, s_row2, s_row3, s_row4 = st.columns([5, 2, 2, 1])
                s_row1.markdown(f"<div class='center-grid'>{order['name']}</div>", unsafe_allow_html=True)
                s_row2.markdown(
                    f"<div class='center-grid'><span style='color:#c62828; font-weight:600;'>{float(order['amount']):.1f}</span></div>",
                    unsafe_allow_html=True,
                )
                s_row3.markdown(f"<div class='center-grid'>{order['frequency']}</div>", unsafe_allow_html=True)
                delete_key = f"active_order_{order['name']}_{order['start_date']}_{order['end_date']}_{active_idx}"
                if s_row4.button("❌", key=f"delete_active_standing_{selected_user}_{delete_key}", type="primary"):
                    for all_idx, all_order in enumerate(user_data["standing_orders"]):
                        if (
                            all_order["name"] == order["name"]
                            and all_order["amount"] == order["amount"]
                            and all_order["frequency"] == order["frequency"]
                            and all_order["start_date"] == order["start_date"]
                            and all_order["end_date"] == order["end_date"]
                        ):
                            user_data["standing_orders"].pop(all_idx)
                            save_data(app_data)
                            st.rerun()
                            break

    with right_col:
        extra_income = sum(
            float(item["amount"])
            for item in expenses_for_month
            if item.get("Type", "Expense") == "Income"
        )
        manual_expenses_total = sum(
            float(item["amount"])
            for item in expenses_for_month
            if item.get("Type", "Expense") == "Expense"
        )
        effective_available_funds = base_funds + extra_income
        total_expenses = manual_expenses_total + standing_orders_total
        remaining_funds = effective_available_funds - total_expenses

        st.subheader("Monthly Overview")
        st.markdown(f"**Extra Income:** +{currency}{extra_income:,.2f}")
        st.markdown(f"**Effective Available Funds:** {currency}{effective_available_funds:,.2f}")
        st.markdown(f"**Total Expenses:** {currency}{total_expenses:,.2f}")
        st.markdown(f"**Remaining Funds:** {currency}{remaining_funds:,.2f}")

        if total_expenses > effective_available_funds:
            deficit = total_expenses - effective_available_funds
            budget_chart_rows = [
                {"Category": "Expenses", "Value": total_expenses},
                {"Category": "Deficit (Over Budget)", "Value": deficit},
            ]
            budget_colors = {"Expenses": "#ff8c00", "Deficit (Over Budget)": "#ffb347"}
        else:
            remainder_category = "Remaining Income" if effective_status == "Working Month" else "Remaining Budget"
            remainder_color = "#2e8b57" if effective_status == "Working Month" else "#808080"
            budget_chart_rows = [
                {"Category": "Expenses", "Value": total_expenses},
                {"Category": remainder_category, "Value": remaining_funds},
            ]
            budget_colors = {"Expenses": "#d62728", remainder_category: remainder_color}

        budget_df_chart = pd.DataFrame(budget_chart_rows)
        budget_df_chart["Hebrew_Category"] = budget_df_chart["Category"].map(hebrew_category_map)
        budget_pie_chart = px.pie(
            budget_df_chart,
            names="Category",
            values="Value",
            color="Category",
            color_discrete_map=budget_colors,
            hole=0.35,
        )
        budget_pie_chart.update_traces(
            customdata=budget_df_chart["Hebrew_Category"],
            textposition="inside",
            textinfo="percent+label",
            hovertemplate="Category: %{customdata}<br>Value: %{value:.1f} " + currency + "<extra></extra>",
        )
        budget_pie_chart.update_layout(
            margin=dict(t=20, b=20, l=20, r=20),
            showlegend=True,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(budget_pie_chart, use_container_width=True)

    # Dedicated bottom row for monthly breakdown charts.
    col_exp, col_inc = st.columns(2)

    with col_exp:
        st.markdown("<h3 style='text-align: center;'>Expenses Breakdown</h3>", unsafe_allow_html=True)
        breakdown_rows = []
        for item in expenses_for_month:
            if item.get("Type", "Expense") == "Expense":
                breakdown_rows.append(
                    {"Category": item["name"], "Value": float(item["amount"]), "Source": "Manual"}
                )
        for order in active_standing_orders:
            breakdown_rows.append(
                {"Category": order["name"], "Value": float(order["amount"]), "Source": "Standing Order"}
            )
        if breakdown_rows:
            breakdown_df = pd.DataFrame(breakdown_rows)
            breakdown_df["Hebrew_Category"] = breakdown_df["Category"]
            breakdown_color_map = {
                row["Category"]: "#7b2cbf"
                for _, row in breakdown_df[breakdown_df["Source"] == "Standing Order"].iterrows()
            }
            breakdown_chart = px.pie(
                breakdown_df,
                names="Category",
                values="Value",
                color="Category",
                color_discrete_map=breakdown_color_map,
                hole=0.25,
            )
            breakdown_chart.update_traces(
                customdata=breakdown_df["Hebrew_Category"],
                textposition="inside",
                textinfo="percent+label",
                hovertemplate="Category: %{customdata}<br>Value: %{value:.1f} "
                + currency
                + "<extra></extra>",
            )
            breakdown_chart.update_layout(
                height=400,
                margin=dict(t=20, b=20, l=10, r=10),
                showlegend=True,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(breakdown_chart, use_container_width=True)
        else:
            st.info("No expenses to display in the breakdown chart.")

    with col_inc:
        st.markdown("<h3 style='text-align: center;'>Income Breakdown</h3>", unsafe_allow_html=True)
        income_rows = [
            {"Category": item["name"], "Value": float(item["amount"])}
            for item in expenses_for_month
            if item.get("Type", "Expense") == "Income"
        ]
        if income_rows:
            income_df = pd.DataFrame(income_rows)
            income_df["Hebrew_Category"] = income_df["Category"]
            income_color_map = {}
            for income_category in income_df["Category"].unique():
                if income_category.strip().lower() in ["salary", "משכורת"]:
                    income_color_map[income_category] = "#2e8b57"
            income_chart = px.pie(
                income_df,
                names="Category",
                values="Value",
                color="Category",
                color_discrete_map=income_color_map,
                hole=0.25,
            )
            income_chart.update_traces(
                customdata=income_df["Hebrew_Category"],
                textposition="inside",
                textinfo="percent+label",
                hovertemplate="Category: %{customdata}<br>Value: %{value:.1f} "
                + currency
                + "<extra></extra>",
            )
            income_chart.update_layout(
                height=400,
                margin=dict(t=20, b=20, l=10, r=10),
                showlegend=True,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(income_chart, use_container_width=True)
        else:
            st.info("No income transactions to display in the breakdown chart.")

with standing_orders_tab:
    st.subheader("Define Standing Orders")
    order_name = st.text_input("Standing Order Name", key=f"standing_name_{selected_user}")
    order_amount = st.number_input(
        f"Amount ({currency})",
        min_value=0.0,
        step=1.0,
        value=0.0,
        key=f"standing_amount_{selected_user}",
    )
    order_frequency = st.selectbox(
        "Frequency",
        options=["Monthly", "Yearly"],
        key=f"standing_frequency_{selected_user}",
    )
    order_start_date = st.date_input(
        "Start Date",
        value=date(selected_year, month_index, 1),
        key=f"standing_start_{selected_user}",
    )
    order_end_date = st.date_input(
        "End Date",
        value=date(selected_year, 12, 31),
        key=f"standing_end_{selected_user}",
    )
    if st.button("Add Standing Order", key=f"add_standing_{selected_user}"):
        if not order_name.strip():
            st.warning("Please enter a standing order name.")
        elif order_amount <= 0:
            st.warning("Please enter an amount greater than 0.")
        elif order_end_date < order_start_date:
            st.warning("End Date must be on or after Start Date.")
        else:
            user_data["standing_orders"].append(
                {
                    "name": order_name.strip(),
                    "amount": float(order_amount),
                    "frequency": order_frequency,
                    "start_date": order_start_date.isoformat(),
                    "end_date": order_end_date.isoformat(),
                }
            )
            save_data(app_data)
            st.rerun()

    st.subheader("All Standing Orders")
    if user_data["standing_orders"]:
        for idx, order in enumerate(user_data["standing_orders"]):
            col_a, col_b, col_c, col_d, col_e, col_f = st.columns([3, 2, 2, 2, 2, 1])
            col_a.markdown(f"<div class='center-grid'>{order['name']}</div>", unsafe_allow_html=True)
            col_b.markdown(
                f"<div class='center-grid'><span style='color:#c62828; font-weight:600;'>{currency}{float(order['amount']):.1f}</span></div>",
                unsafe_allow_html=True,
            )
            col_c.markdown(f"<div class='center-grid'>Expense</div>", unsafe_allow_html=True)
            col_d.markdown(f"<div class='center-grid'>{order['start_date']}</div>", unsafe_allow_html=True)
            col_e.markdown(f"<div class='center-grid'>{order['end_date']}</div>", unsafe_allow_html=True)
            if col_f.button("❌", key=f"delete_standing_{selected_user}_{idx}", type="primary"):
                user_data["standing_orders"].pop(idx)
                save_data(app_data)
                st.rerun()
    else:
        st.info("No standing orders defined yet.")
