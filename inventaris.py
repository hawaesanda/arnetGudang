import os, re, imghdr, pickle, logging, gspread
from datetime import datetime
from collections import defaultdict
from typing import Optional, Dict, Any, List, Tuple
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

# =========================
# KONFIGURASI
# =========================
API_ID = 21604277 
API_HASH = "5320799e4addb26b8117972b3c959440"
BOT_TOKEN = "8340869371:AAFbTsuAxc3QRBR0-cRyLCDsa9p23vW7mTg"

SPREADSHEET_ID = "1nD_g4_5AZ8yG5-7Z-LOfQ9rOhzXfDJgLzGS7wvXg1n8"
GOOGLE_DRIVE_PARENT_FOLDER_ID = "1aTEuqTK4FsezLHc_W83N08HI-7VtlvEb"
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.pickle"

# =========================
# "BUKU RESEP" PERANGKAT
# =========================
DEVICE_CONFIG: Dict[str, Dict[str, Any]] = {
    "SFP": {
        "worksheet_name": "SFP",
        "drive_folder_ids": {
            "SFP":  "1uLYT7rw3GcdEJ4Qkad3zIk_E9Xjxvgqh",
            "SFP+": "1o0tWNJsMMnHmRU94veIlISLBN5Zfui0y",
            "XFP":  "1rEznUq-AgyZDkscLkpuzv3YcQ_IS1sqs",
            "XFP+": "1rq4upY2HoElBwxm17LoQczcWqfasWedH",
        },
        "display_group_by": ["BW (SFP)", "Jarak (SFP)"],
        "questions": [
            {"key": "Detail Perangkat", "prompt": "Pilih Detail Perangkat", "type": "buttons",
             "options": ["SFP", "SFP+", "XFP", "XFP+"]},
            {"key": "BW (SFP)", "prompt": "Pilih Bandwidth (BW)", "type": "buttons",
             "options": ["1G", "10G", "100G"]},
            {"key": "Jarak (SFP)", "prompt": "Pilih Jarak", "type": "buttons",
             "options": ["10 km", "40 km", "80 km"]},
            {"key": "SN", "prompt": "Ketik Serial Number (SN)", "type": "text", "required": True},
            {"key": "Keterangan", "prompt": "Masukkan Keterangan (lokasi/kondisi barang)", "type": "text"},
            {"key": "Link Foto", "prompt": "Kirim foto perangkat", "type": "photo"},
        ]
    },
    "Patch Cord": {
        "worksheet_name": "Patch Cord",
        "drive_folder_ids": {
            "Simplex": "14U_zpXKlqIsm155K5hbedYOITVDh5K0V",
            "Duplex":  "1I8Um2KXF3hTPeilOzc4o_7BnKFFWhmlI",
        },
        "display_group_by": ["Konektor 1", "Konektor 2", "Ukuran (PC)"],
        "questions": [
            {"key": "Detail Perangkat", "prompt": "Pilih Detail (Tipe Kabel)", "type": "buttons",
             "options": ["Simplex", "Duplex"]},
            {"key": "Konektor 1", "prompt": "Pilih Jenis Konektor Pertama", "type": "buttons",
             "options": ["SC-UPC", "SC-APC", "FC-UPC", "FC-APC", "LC-UPC", "LC-APC"]},
            {"key": "Konektor 2", "prompt": "Pilih Jenis Konektor Kedua", "type": "buttons",
             "options": ["SC-UPC", "SC-APC", "FC-UPC", "FC-APC", "LC-UPC", "LC-APC"]},
            {"key": "Ukuran (PC)", "prompt": "Pilih Ukuran", "type": "buttons",
             "options": ["1m", "3m", "5m", "10m", "15m", "20m", "50m"]},
            {"key": "Jumlah", "prompt": "Masukkan Jumlah unit (angka saja)", "type": "text", "required": True},
            {"key": "Keterangan", "prompt": "Masukkan Keterangan (lokasi/kondisi barang)", "type": "text"},
            {"key": "Link Foto", "prompt": "Kirim foto perangkat", "type": "photo"},
        ]
    },
}

# =========================
# LOGGING & GOOGLE AUTH
# =========================
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger("gudang")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def get_oauth2_credentials():
    creds: Optional[Credentials] = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.error(f"Gagal refresh token: {e}"); creds = None
        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "wb") as f:
            pickle.dump(creds, f)
    return creds

# =========================
# INISIALISASI
# =========================
try:
    creds = get_oauth2_credentials()
    gspread_client = gspread.authorize(creds)
    drive_service = build("drive", "v3", credentials=creds)
    ss = gspread_client.open_by_key(SPREADSHEET_ID)
    logger.info("Berhasil terhubung ke Google Sheets & Drive.")
except Exception:
    logger.exception("Gagal terhubung ke Google API"); raise

# =========================
# TELEGRAM
# =========================
from pyrogram import Client, filters
from pyrogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
)

app = Client("bot-gudang", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_states: dict[int, list[str]] = defaultdict(list)
user_data: dict[int, dict] = defaultdict(dict)

# =========================
# UI LABELS
# =========================
BTN_INPUT   = "Input Data Baru"
BTN_DISPLAY = "Tampilkan Rekap Stok"
BTN_DELETE  = "Hapus Data"
BTN_EDIT    = "Ubah Data"
BTN_LOG     = "Riwayat Perubahan"
BTN_PEMAKAIAN = "Pemakaian"
BTN_CANCEL  = "Batal"
BTN_BACK    = "Kembali"

BTN_PEMAKAIAN_LOG   = "Log Pemakaian"
BTN_PEMAKAIAN_AMBIL = "Ambil Barang"

OPT_EDIT_KET = "Ubah Keterangan"
OPT_EDIT_QTY = "Ubah Jumlah (Patch Cord)"

LABEL_CONFIRM_SAVE   = "Simpan"
LABEL_CONFIRM_DELETE = "Hapus"
LABEL_CONFIRM_UPDATE = "Ubah"
LABEL_CONFIRM_TAKE   = "Ambil"

BTN_YES_ADD = "Iya, tambah jumlah"
BTN_NO_CANCEL_INPUT = "Tidak, batalkan input"

# Keyboards
MAIN_MENU_KEYBOARD = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_INPUT)],
     [KeyboardButton(BTN_DISPLAY)],
     [KeyboardButton(BTN_DELETE), KeyboardButton(BTN_EDIT)],
     [KeyboardButton(BTN_PEMAKAIAN)],
     [KeyboardButton(BTN_LOG)]],
    resize_keyboard=True
)
PEMAKAIAN_KEYBOARD = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_PEMAKAIAN_AMBIL)],
     [KeyboardButton(BTN_PEMAKAIAN_LOG)],
     [KeyboardButton(BTN_BACK)]],
    resize_keyboard=True
)
EDIT_SUBMENU_KEYBOARD = ReplyKeyboardMarkup(
    [[KeyboardButton(OPT_EDIT_KET)],
     [KeyboardButton(OPT_EDIT_QTY)],
     [KeyboardButton(BTN_BACK)]],
    resize_keyboard=True
)
NAVIGATION_KEYBOARD = ReplyKeyboardMarkup([[KeyboardButton(BTN_BACK), KeyboardButton(BTN_CANCEL)]], resize_keyboard=True)
CONFIRMATION_KEYBOARD   = ReplyKeyboardMarkup([[KeyboardButton(LABEL_CONFIRM_SAVE),   KeyboardButton(BTN_CANCEL)]], resize_keyboard=True)
DELETE_CONFIRM_KEYBOARD = ReplyKeyboardMarkup([[KeyboardButton(LABEL_CONFIRM_DELETE), KeyboardButton(BTN_CANCEL)]], resize_keyboard=True)
EDIT_CONFIRM_KEYBOARD   = ReplyKeyboardMarkup([[KeyboardButton(LABEL_CONFIRM_UPDATE), KeyboardButton(BTN_CANCEL)]], resize_keyboard=True)
TAKE_CONFIRM_KEYBOARD   = ReplyKeyboardMarkup([[KeyboardButton(LABEL_CONFIRM_TAKE),   KeyboardButton(BTN_CANCEL)]], resize_keyboard=True)
DUPLICATE_CONFIRM_KEYBOARD = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_YES_ADD)], [KeyboardButton(BTN_NO_CANCEL_INPUT)]],
    resize_keyboard=True
)
CANCEL_ONLY_KEYBOARD = ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True)

# =========================
# HELPERS
# =========================
async def clear_user_session(user_id: int):
    user_states.pop(user_id, None); user_data.pop(user_id, None)

def is_non_text_message(msg: Message) -> bool:
    return any([
        getattr(msg, "photo", None), getattr(msg, "document", None),
        getattr(msg, "sticker", None), getattr(msg, "video", None),
        getattr(msg, "animation", None), getattr(msg, "voice", None),
        getattr(msg, "audio", None), getattr(msg, "video_note", None)
    ])

def invalid_choice(text: str, options: List[str]) -> bool:
    return text not in options

def reply_invalid_choice(message: Message):
    return message.reply_text("Pilihan tidak valid.")

def ensure_headers(ws: gspread.Worksheet, required: List[str]) -> List[str]:
    headers = ws.row_values(1)
    missing = [h for h in required if h not in headers]
    if missing:
        raise RuntimeError(f"Kolom wajib hilang di sheet '{ws.title}': {', '.join(missing)}")
    return headers

def next_no(ws: gspread.Worksheet) -> int:
    col_a = ws.col_values(1)
    if len(col_a) <= 1:
        return 1
    last = col_a[-1]
    try:
        return int(last) + 1
    except Exception:
        return len(col_a) - 1

def upload_photo_to_drive(file_data: bytes, file_name: str, jenis_perangkat: str, detail_perangkat: str) -> Optional[str]:
    try:
        folder_ids = DEVICE_CONFIG.get(jenis_perangkat, {}).get("drive_folder_ids", {})
        target_folder_id = folder_ids.get(detail_perangkat, GOOGLE_DRIVE_PARENT_FOLDER_ID)
        kind = imghdr.what(None, h=file_data) or "jpeg"
        media = MediaInMemoryUpload(file_data, mimetype=f"image/{kind}", resumable=False)
        file = drive_service.files().create(
            body={"name": file_name, "parents": [target_folder_id]},
            media_body=media, fields="id", supportsAllDrives=True
        ).execute()
        drive_service.permissions().create(fileId=file['id'], body={"role": "reader", "type": "anyone"}, supportsAllDrives=True).execute()
        return f"https://drive.google.com/file/d/{file['id']}/view"
    except Exception:
        logger.exception("Upload ke Drive gagal.")
        return None

def extract_drive_id_from_url(url: str) -> Optional[str]:
    if not isinstance(url, str): return None
    match = re.search(r"/file/d/([^/]+)", url)
    return match.group(1) if match else None

def delete_photo_from_drive(file_id: str):
    if not file_id:
        return
    try:
        drive_service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
        logger.info(f"Berhasil menghapus file Drive dengan ID: {file_id}")
    except Exception as e:
        logger.error(f"Gagal menghapus file Drive ID {file_id}: {e}")

def find_sn_in_all_sheets(sn_to_find: str):
    for config in DEVICE_CONFIG.values():
        try:
            ws = ss.worksheet(config["worksheet_name"])
            headers = ws.row_values(1)
            if "SN" not in headers: continue
            cell = ws.find(sn_to_find, in_column=headers.index("SN") + 1)
            if cell:
                return ws, cell.row, dict(zip(headers, ws.row_values(cell.row)))
        except (gspread.exceptions.WorksheetNotFound, ValueError):
            continue
    return None, None, None

def _pc_row_match(r: Dict[str, Any], detail: str, k1: str, k2: str, uk: str) -> bool:
    if r.get("Detail Perangkat") != detail: return False
    if r.get("Ukuran (PC)") != uk: return False
    a1, a2 = r.get("Konektor 1"), r.get("Konektor 2")
    return (a1 == k1 and a2 == k2) or (a1 == k2 and a2 == k1)

def find_patchcord_row(detail: str, k1: str, k2: str, ukuran: str) -> Tuple[Optional[gspread.Worksheet], Optional[int], Optional[Dict[str, Any]]]:
    try:
        ws = ss.worksheet("Patch Cord")
        for i, r in enumerate(ws.get_all_records()):
            if _pc_row_match(r, detail, k1, k2, ukuran):
                return ws, i + 2, r
    except gspread.exceptions.WorksheetNotFound:
        pass
    return None, None, None

def join_detail_sfp_no_ket(row: Dict[str, Any]) -> str:
    d   = row.get("Detail Perangkat","-")
    bw  = row.get("BW (SFP)","-")
    jrk = row.get("Jarak (SFP)","-")
    sn  = row.get("SN","-")
    return f"{d} | BW {bw} | Jarak {jrk} | SN {sn}"

def join_detail_pc_no_ket(detail: str, k1: str, k2: str, ukuran: str) -> str:
    return f"{detail} | {k1} -> {k2} | {ukuran}"

def build_summary_text(ws_name: str, data: Dict[str, Any]) -> str:
    if ws_name == "Patch Cord":
        qty = str(data.get('Jumlah','')).strip()
        ket = str(data.get('Keterangan', '')).strip()
        parts = [
            f"{data.get('Detail Perangkat','-')}",
            f"{data.get('Konektor 1','-')} -> {data.get('Konektor 2','-')}",
            f"{data.get('Ukuran (PC)','-')}",
        ]
        if qty: parts.append(f"Jumlah: {qty}")
        if ket: parts.append(f"Ket: {ket}")
        return " | ".join(parts)
    
    parts = [
        f"{data.get('Detail Perangkat','-')}",
        f"BW {data.get('BW (SFP)','-')}",
        f"Jarak {data.get('Jarak (SFP)','-')}",
        f"SN {data.get('SN','-')}",
    ]
    ket = str(data.get('Keterangan', '')).strip()
    if ket: parts.append(f"Ket: {ket}")
    return " | ".join(parts)

def bullets_from_detail(_ws_name: str, detail: str) -> str:
    toks = [t.strip() for t in (detail or "").split("|") if t.strip()]
    return "\n".join([f"- {t.strip()}" for t in toks])

def renumber_worksheet(ws: gspread.Worksheet):
    vals = ws.get_all_values()
    if len(vals) < 2: return
    if vals[0][0] != "No": return
    ws.update(values=[[i + 1] for i in range(len(vals) - 1)],
              range_name=f"A2:A{len(vals)}",
              value_input_option="USER_ENTERED")

def get_or_create_log_ws() -> gspread.Worksheet:
    try:
        return ss.worksheet("Log")
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title="Log", rows=1000, cols=7)
        ws.update("A1:G1", [[
            "Waktu", "User ID", "Username", "Action", "Worksheet", "Detail", "Keterangan"
        ]])
        return ws


def append_log(action: str, worksheet_name: str, detail_no_ket: str,
               user_id: int, username: Optional[str], ket: str):
    ws = get_or_create_log_ws()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.append_row(
        [ts, str(user_id), username or "", action, worksheet_name, detail_no_ket, ket],
        value_input_option="USER_ENTERED"
    )

def get_or_create_pemakaian_ws() -> gspread.Worksheet:
    try:
        return ss.worksheet("Pemakaian")
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title="Pemakaian", rows=1000, cols=8)
        ws.update("A1:H1", [[
            "Waktu", "User ID", "Username", "Jenis Perangkat", "Detail",
            "Jumlah Ambil", "Keterangan (Barang)", "Keterangan Pemakaian"
        ]])
        return ws


def append_pemakaian(jenis:str, detail_no_ket:str, qty:str,
                     ket_barang:str, ket_pemakaian:str,
                     user_id:int, username:Optional[str]):
    ws = get_or_create_pemakaian_ws()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.append_row([ts, str(user_id), username or "", jenis, detail_no_ket, qty, ket_barang, ket_pemakaian],
                  value_input_option="USER_ENTERED")

def get_device_selection_keyboard(purpose: str):
    buttons = [KeyboardButton(d) for d in DEVICE_CONFIG.keys()]
    rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    inline = [InlineKeyboardButton(d, callback_data=f"display_{d}") for d in DEVICE_CONFIG.keys()]
    ik = [inline[i:i+2] for i in range(0, len(inline), 2)]
    ik.append([InlineKeyboardButton("Tutup Menu Rekap", callback_data="display_close")])
    return InlineKeyboardMarkup(ik)

def get_dynamic_keyboard(options: List[str]):
    btns = [KeyboardButton(o) for o in options]
    layout = [btns[i:i+2] for i in range(0, len(btns), 2)]
    layout.append([KeyboardButton(BTN_BACK), KeyboardButton(BTN_CANCEL)])
    return ReplyKeyboardMarkup(layout, resize_keyboard=True)

def konektor_keyboard(which: int) -> ReplyKeyboardMarkup:
    idx = 1 if which == 1 else 2
    return get_dynamic_keyboard(DEVICE_CONFIG["Patch Cord"]["questions"][idx]["options"])

# =========================
# UTILITAS FLOW PATCH CORD
# =========================
PC_PROMPTS = {
    "detail": ("Pilih Detail (Simplex/Duplex):", DEVICE_CONFIG["Patch Cord"]["questions"][0]["options"]),
    "k1": ("Pilih Konektor 1:", DEVICE_CONFIG["Patch Cord"]["questions"][1]["options"]),
    "k2": ("Pilih Konektor 2:", DEVICE_CONFIG["Patch Cord"]["questions"][2]["options"]),
    "uk": ("Pilih Ukuran:", DEVICE_CONFIG["Patch Cord"]["questions"][3]["options"]),
}

async def pc_prompt(message: Message, step: str):
    prompt, opts = PC_PROMPTS[step]
    kb = get_dynamic_keyboard(opts) if step in {"detail","uk"} else (konektor_keyboard(1) if step=="k1" else konektor_keyboard(2))
    await message.reply_text(prompt, reply_markup=kb)

def pc_store(user_id: int, step: str, value: str):
    pc = user_data[user_id].setdefault("pc", {})
    pc[step] = value

def pc_values(user_id: int):
    pc = user_data[user_id].get("pc", {})
    return pc.get("detail"), pc.get("k1"), pc.get("k2"), pc.get("uk")

async def pc_find_and_prepare(message: Message, mode: str):
    d, k1, k2, uk = pc_values(message.from_user.id)
    await message.reply_text(f"Mencari Patch Cord: {d} | {k1} -> {k2} | {uk}...", reply_markup=ReplyKeyboardRemove())
    ws, row_num, row_data = find_patchcord_row(d, k1, k2, uk)
    if not row_num:
        await message.reply_text("Kombinasi tidak ditemukan.", reply_markup=NAVIGATION_KEYBOARD); return False
    summary = build_summary_text(ws.title, row_data)
    user_data[message.from_user.id].update({'worksheet_to_edit': ws, 'row_to_edit': row_num, 'item_summary': summary, 'item_rowdata': row_data})
    if mode == "delete":
        user_data[message.from_user.id]['row_to_delete'] = row_num
        bullets = bullets_from_detail(ws.title, summary)
        user_states[message.from_user.id].append("awaiting_delete_confirmation")
        await message.reply_text(f"Konfirmasi Hapus - {ws.title}\n\n{bullets}\n\nYakin hapus?", reply_markup=DELETE_CONFIRM_KEYBOARD)
    elif mode == "edit_ket":
        user_data[message.from_user.id]['old_ket'] = row_data.get('Keterangan','')
        user_states[message.from_user.id].append("awaiting_new_ket")
        await message.reply_text(f"Keterangan sekarang: {row_data.get('Keterangan','(kosong)')}\nKirim keterangan baru:", reply_markup=NAVIGATION_KEYBOARD)
    else:  # edit_qty
        user_data[message.from_user.id]['old_qty'] = row_data.get('Jumlah','0')
        user_states[message.from_user.id].append("awaiting_new_jumlah")
        await message.reply_text(f"Jumlah sekarang: {row_data.get('Jumlah','0')}\nKirim jumlah baru (angka):", reply_markup=CANCEL_ONLY_KEYBOARD)
    return True

# =========================
# CORE UI
# =========================
async def show_main_menu(message: Message):
    await clear_user_session(message.from_user.id)
    await message.reply_text("Silakan pilih salah satu menu di bawah ini:", reply_markup=MAIN_MENU_KEYBOARD)

async def ask_next_question(message: Message):
    user_id = message.from_user.id
    device_type = user_data[user_id].get("device_type")
    if not device_type: return await show_main_menu(message)
    qs = DEVICE_CONFIG[device_type]["questions"]
    i = user_data[user_id].get("question_index", 0)
    if i >= len(qs): return await process_input_confirmation(message)
    q = qs[i]
    user_states[user_id] = ["awaiting_answer"]
    kb = get_dynamic_keyboard(q["options"]) if q["type"] == "buttons" else NAVIGATION_KEYBOARD
    await message.reply_text(f"Langkah {i + 2}: {q['prompt']}", reply_markup=kb)

async def process_input_confirmation(message: Message):
    uid = message.from_user.id
    data = user_data[uid]; device_type = data.get("device_type")
    lines = [f"Konfirmasi Data Masuk - {device_type}", ""]
    for q in DEVICE_CONFIG[device_type]["questions"]:
        key, value = q["key"], data.get(q["key"], "(kosong)")
        if q["type"] == "photo": value = "(foto akan diunggah)"
        lines.append(f"- {key}: {value}")
    lines += ["", "Apakah Anda yakin ingin menyimpan data ini?"]
    user_states[uid].append("awaiting_input_confirmation")
    await message.reply_text("\n".join(lines), reply_markup=CONFIRMATION_KEYBOARD)

# =========================
# COMMANDS
# =========================
@app.on_message(filters.command(["start", "help"]) & filters.private)
async def start_command(client: Client, message: Message):
    await show_main_menu(message)

# =========================
# MAIN HANDLER
# =========================
@app.on_message(
    (filters.text | filters.photo | filters.document | filters.voice | filters.audio | filters.video | filters.animation | filters.sticker | filters.video_note)
    & filters.private
)
async def handle_messages(client: Client, message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    text = message.text or ""

    if text == BTN_CANCEL:
        await message.reply_text("Dibatalkan.", reply_markup=ReplyKeyboardRemove()); return await show_main_menu(message)
    if text == BTN_BACK:
        state = user_states.get(user_id, [None])[-1]
        
        if state in ["awaiting_device_to_edit", "awaiting_pc_selection_for_edit_qty", "awaiting_item_selection_for_edit_ket"]:
            user_states[user_id] = ["awaiting_edit_menu_choice"]
            return await message.reply_text("Pilih jenis perubahan:", reply_markup=EDIT_SUBMENU_KEYBOARD)

        if state in ["awaiting_consume_device_type", "awaiting_item_selection_for_consume", "awaiting_consume_sfp_type"]:
            user_states[user_id] = ["awaiting_pemakaian_menu"]
            return await message.reply_text("Pilih menu pemakaian:", reply_markup=PEMAKAIAN_KEYBOARD)

        if state == "awaiting_pemakaian_menu" or state == "awaiting_edit_menu_choice":
             return await show_main_menu(message)

        if user_data[user_id].get("question_index", 0) > 0:
            user_data[user_id]["question_index"] -= 1; return await ask_next_question(message)
            
        return await show_main_menu(message)

    state = user_states.get(user_id, [None])[-1]

    if not state:
        if text == BTN_INPUT:
            user_states[user_id].append("awaiting_device_selection")
            device_options = list(DEVICE_CONFIG.keys())
            return await message.reply_text("Langkah 1: Pilih Jenis Perangkat", reply_markup=get_dynamic_keyboard(device_options))
        if text == BTN_DISPLAY:
            return await message.reply_text("Pilih jenis perangkat untuk rekap:", reply_markup=get_device_selection_keyboard("display"))
        if text == BTN_DELETE:
            user_states[user_id].append("awaiting_device_to_delete")
            device_options = list(DEVICE_CONFIG.keys())
            return await message.reply_text("Pilih jenis perangkat yang akan dihapus datanya:", reply_markup=get_dynamic_keyboard(device_options))
        if text == BTN_EDIT:
            user_states[user_id].append("awaiting_edit_menu_choice")
            return await message.reply_text("Pilih jenis perubahan:", reply_markup=EDIT_SUBMENU_KEYBOARD)
        if text == BTN_LOG:
            try:
                wslog = get_or_create_log_ws(); rows = wslog.get_all_values()
                if len(rows) <= 1: return await message.reply_text("Belum ada log perubahan.", reply_markup=MAIN_MENU_KEYBOARD)
                last = rows[-10:] if len(rows) > 11 else rows[1:]
                blocks = ["Riwayat Perubahan (terbaru di bawah):", ""]
                for r in last:
                    waktu, uid, uname, action, wsn, detail, ket = (r + [""]*7)[:7]
                    blocks.append(f"[{waktu}] {action} - {wsn}")
                    blocks.append(bullets_from_detail(wsn, detail))
                    if ket: blocks.append(f"- Keterangan: {ket}")
                    blocks.append("")
                return await message.reply_text("\n".join(blocks), reply_markup=MAIN_MENU_KEYBOARD)
            except Exception:
                logger.exception("Gagal ambil log"); return await message.reply_text("Gagal memuat log.", reply_markup=MAIN_MENU_KEYBOARD)
        if text == BTN_PEMAKAIAN:
            user_states[user_id].append("awaiting_pemakaian_menu")
            return await message.reply_text("Pilih menu pemakaian:", reply_markup=PEMAKAIAN_KEYBOARD)
        return await show_main_menu(message)

    if state == "awaiting_device_selection":
        if text in DEVICE_CONFIG:
            user_data[user_id]["device_type"] = text; user_data[user_id]["question_index"] = 0
            return await ask_next_question(message)
        return await message.reply_text("Jenis perangkat tidak valid. Silakan pilih dari keyboard.")

    if state == "awaiting_answer":
        dev = user_data[user_id]["device_type"]
        i = user_data[user_id]["question_index"]
        q = DEVICE_CONFIG[dev]["questions"][i]
        ans = text

        if q["type"] == "photo":
            if message.photo or (message.document and str(message.document.mime_type).startswith("image/")):
                ans = message.id
            else:
                return await message.reply_text("Input tidak valid. Kirim foto.")
        else:
            if is_non_text_message(message): return await message.reply_text("Input harus berupa teks. Jangan kirim media.")
            if not text.strip() and q.get("required"): return await message.reply_text("Input ini wajib diisi.")
            if dev == "Patch Cord" and q["key"] == "Jumlah" and not re.fullmatch(r"\d+", text.strip()):
                return await message.reply_text("Jumlah harus angka. Contoh: 3")

        user_data[user_id][q["key"]] = ans

        if dev == "Patch Cord" and q["key"] == "Ukuran (PC)":
            d = user_data[user_id].get("Detail Perangkat")
            k1 = user_data[user_id].get("Konektor 1")
            k2 = user_data[user_id].get("Konektor 2")
            uk = ans

            ws, row_num, row_data = find_patchcord_row(d, k1, k2, uk)

            if row_num:
                user_data[user_id].update({
                    'duplicate_ws': ws,
                    'duplicate_row_num': row_num,
                    'duplicate_row_data': row_data,
                })
                user_data[user_id].pop("question_index", None)

                user_states[user_id].append("awaiting_add_or_cancel_duplicate")
                await message.reply_text(
                    "Barang ini sudah ada. Apakah Anda ingin menambah jumlah stok?",
                    reply_markup=DUPLICATE_CONFIRM_KEYBOARD
                )
                return

        user_data[user_id]["question_index"] += 1
        return await ask_next_question(message)

    if state == "awaiting_input_confirmation":
        if text == LABEL_CONFIRM_SAVE:
            await message.reply_text("Menyimpan data...", reply_markup=ReplyKeyboardRemove())
            try:
                dev = user_data[user_id]["device_type"]; cfg = DEVICE_CONFIG[dev]; ws = ss.worksheet(cfg["worksheet_name"])
                req_cols = ["No"] + [q["key"] for q in cfg["questions"]]
                headers = ensure_headers(ws, req_cols)

                photo_key = next((q['key'] for q in cfg['questions'] if q['type'] == 'photo'), None)
                photo_msg_id = user_data[user_id].get(photo_key)
                if not photo_msg_id:
                    await message.reply_text("Foto perangkat wajib. Data tidak disimpan.", reply_markup=ReplyKeyboardRemove())
                    return await show_main_menu(message)

                try:
                    photo_msg = await app.get_messages(user_id, photo_msg_id)
                    raw = (await app.download_media(photo_msg, in_memory=True)).getvalue()
                    detail = user_data[user_id].get("Detail Perangkat", "UNKNOWN")
                    safe_tail = datetime.now().strftime('%Y%m%d%H%M%S')
                    file_name = f"{dev}-{detail}-{safe_tail}.jpg"
                    link_to_save = upload_photo_to_drive(raw, file_name, dev, detail)
                    if not link_to_save:
                        await message.reply_text("Gagal mengunggah foto ke Drive. Data tidak disimpan. Silakan coba lagi.", reply_markup=ReplyKeyboardRemove())
                        return await show_main_menu(message)
                except Exception:
                    logger.exception("Gagal proses upload foto")
                    await message.reply_text("Terjadi kesalahan saat mengunggah foto. Data tidak disimpan. Silakan coba lagi.", reply_markup=ReplyKeyboardRemove())
                    return await show_main_menu(message)

                final_map = {h: (link_to_save if h == "Link Foto" else user_data[user_id].get(h, "N/A")) for h in headers if h != "No"}

                if dev == "Patch Cord":
                    d  = final_map.get("Detail Perangkat")
                    k1 = final_map.get("Konektor 1")
                    k2 = final_map.get("Konektor 2")
                    uk = final_map.get("Ukuran (PC)")
                    
                    nomor_baru = next_no(ws)
                    final_row = [nomor_baru if h == "No" else final_map.get(h, "") for h in headers]
                    ws.append_row(final_row, value_input_option='USER_ENTERED')
                    detail_no_ket = join_detail_pc_no_ket(d, k1, k2, uk)
                    append_log("INSERT", ws.title, detail_no_ket, user_id, username, ket=(final_map.get("Keterangan") or ""))
                    await message.reply_text("Data baru berhasil disimpan.")
                else:
                    nomor_baru = next_no(ws)
                    final_row = [nomor_baru if h == "No" else final_map.get(h, "") for h in headers]
                    ws.append_row(final_row, value_input_option='USER_ENTERED')
                    detail_no_ket = join_detail_sfp_no_ket(final_map)
                    append_log("INSERT", ws.title, detail_no_ket, user_id, username, ket=(final_map.get("Keterangan") or ""))
                    await message.reply_text("Data berhasil disimpan.")
            except Exception:
                logger.exception("Gagal menyimpan")
                await message.reply_text("Gagal menyimpan data.", reply_markup=ReplyKeyboardRemove())
            return await show_main_menu(message)
        await message.reply_text("Dibatalkan.", reply_markup=ReplyKeyboardRemove()); return await show_main_menu(message)

    if state == "awaiting_add_or_cancel_duplicate":
        if text == BTN_YES_ADD:
            user_states[user_id].append("awaiting_add_quantity_for_duplicate")
            await message.reply_text("Masukkan jumlah yang ingin DITAMBAH (angka saja):", reply_markup=CANCEL_ONLY_KEYBOARD)
            return
        elif text == BTN_NO_CANCEL_INPUT:
            await message.reply_text("Baik, input dibatalkan.", reply_markup=ReplyKeyboardRemove())
            return await show_main_menu(message)
        else:
            return await message.reply_text("Pilihan tidak valid. Silakan pilih 'Iya' atau 'Tidak'.", reply_markup=DUPLICATE_CONFIRM_KEYBOARD)

    if state == "awaiting_add_quantity_for_duplicate":
        if not re.fullmatch(r"\d+", text.strip()):
            return await message.reply_text("Input tidak valid. Jumlah harus berupa angka.")

        add_qty = int(text.strip())
        if add_qty <= 0:
            return await message.reply_text("Jumlah yang ditambahkan harus lebih dari 0.")

        await message.reply_text("Menambahkan jumlah...", reply_markup=ReplyKeyboardRemove())
        try:
            data = user_data[user_id]
            ws = data['duplicate_ws']
            row_num = data['duplicate_row_num']
            row_data = data['duplicate_row_data']

            qty_col_idx = ws.row_values(1).index("Jumlah") + 1
            old_qty = int(str(row_data.get("Jumlah", "0")).strip() or "0")
            new_qty = old_qty + add_qty
            ws.update_cell(row_num, qty_col_idx, str(new_qty))

            d = row_data.get("Detail Perangkat")
            k1 = row_data.get("Konektor 1")
            k2 = row_data.get("Konektor 2")
            uk = row_data.get("Ukuran (PC)")
            detail_no_ket = join_detail_pc_no_ket(d, k1, k2, uk)
            log_ket = f"Jumlah ditambahkan {add_qty} (dari {old_qty} menjadi {new_qty})"
            append_log("UPDATE", ws.title, detail_no_ket, user_id, username, ket=log_ket)

            await message.reply_text(f"Jumlah berhasil ditambahkan. Stok sekarang: {new_qty}")
        except Exception:
            logger.exception("Gagal menambahkan jumlah (early duplicate detection)")
            await message.reply_text("Terjadi kesalahan saat menambahkan jumlah.")
        finally:
            return await show_main_menu(message)

    if state == "awaiting_device_to_delete":
        if text == "SFP":
            user_states[user_id].append("awaiting_sn_to_delete")
            return await message.reply_text("Kirim SN SFP yang akan dihapus:", reply_markup=NAVIGATION_KEYBOARD)
        if text == "Patch Cord":
            user_states[user_id].append("awaiting_pc_detail_delete"); return await pc_prompt(message, "detail")
        return await message.reply_text("Jenis perangkat tidak valid.")

    if state == "awaiting_sn_to_delete":
        sn = text.strip()
        if not sn: return await message.reply_text("SN tidak boleh kosong.")
        await message.reply_text(f"Mencari SN: {sn}...", reply_markup=ReplyKeyboardRemove())
        ws, row_num, row_data = find_sn_in_all_sheets(sn)
        if not row_num or ws.title != "SFP": await message.reply_text("SN tidak ditemukan di sheet SFP.", reply_markup=NAVIGATION_KEYBOARD); return
        summary = build_summary_text(ws.title, row_data)
        bullets = bullets_from_detail(ws.title, summary)
        user_states[user_id].append("awaiting_delete_confirmation")
        user_data[user_id].update({'worksheet_to_edit': ws, 'row_to_delete': row_num, 'item_summary': summary, 'item_rowdata': row_data})
        return await message.reply_text(f"Konfirmasi Hapus - {ws.title}\n\n{bullets}\n\nYakin hapus?", reply_markup=DELETE_CONFIRM_KEYBOARD)

    if state == "awaiting_pc_detail_delete":
        if invalid_choice(text, PC_PROMPTS["detail"][1]): return await reply_invalid_choice(message)
        pc_store(user_id, "detail", text); user_states[user_id].append("awaiting_pc_k1_delete")
        return await pc_prompt(message, "k1")
    if state == "awaiting_pc_k1_delete":
        if invalid_choice(text, PC_PROMPTS["k1"][1]): return await reply_invalid_choice(message)
        pc_store(user_id, "k1", text); user_states[user_id].append("awaiting_pc_k2_delete")
        return await pc_prompt(message, "k2")
    if state == "awaiting_pc_k2_delete":
        if invalid_choice(text, PC_PROMPTS["k2"][1]): return await reply_invalid_choice(message)
        pc_store(user_id, "k2", text); user_states[user_id].append("awaiting_pc_uk_delete")
        return await pc_prompt(message, "uk")
    if state == "awaiting_pc_uk_delete":
        if invalid_choice(text, PC_PROMPTS["uk"][1]): return await reply_invalid_choice(message)
        pc_store(user_id, "uk", text)
        return await pc_find_and_prepare(message, "delete")

    if state == "awaiting_delete_confirmation":
        if text == LABEL_CONFIRM_DELETE:
            ws = user_data[user_id]['worksheet_to_edit']
            row_num = user_data[user_id].get('row_to_delete') or user_data[user_id].get('row_to_edit')
            row_data = user_data[user_id].get('item_rowdata', {})
            await message.reply_text("Menghapus data dan foto terkait...", reply_markup=ReplyKeyboardRemove())
            try:
                photo_link = row_data.get("Link Foto")
                if photo_link:
                    file_id = extract_drive_id_from_url(photo_link)
                    if file_id:
                        delete_photo_from_drive(file_id)
                    else:
                        logger.warning(f"Gagal mengekstrak ID Drive dari link: {photo_link}")

                if ws.title == "Patch Cord":
                    detail_no_ket = join_detail_pc_no_ket(row_data.get('Detail Perangkat','-'),
                                                          row_data.get('Konektor 1','-'),
                                                          row_data.get('Konektor 2','-'),
                                                          row_data.get('Ukuran (PC)','-'))
                else:
                    detail_no_ket = join_detail_sfp_no_ket(row_data)

                ws.delete_rows(row_num)
                renumber_worksheet(ws)
                append_log("DELETE", ws.title, detail_no_ket, user_id, username, ket=row_data.get("Keterangan",""))
                await message.reply_text("Data dan foto berhasil dihapus.")
            except Exception:
                logger.exception("Gagal hapus"); await message.reply_text("Gagal menghapus data.")
            return await show_main_menu(message)
        await message.reply_text("Dibatalkan.", reply_markup=ReplyKeyboardRemove()); return await show_main_menu(message)

    if state == "awaiting_edit_menu_choice":
        if text == OPT_EDIT_KET:
            user_states[user_id].append("awaiting_device_to_edit")
            device_options = list(DEVICE_CONFIG.keys())
            return await message.reply_text("Pilih jenis perangkat yang akan diubah keterangannya:", reply_markup=get_dynamic_keyboard(device_options))
        
        if text == OPT_EDIT_QTY:
            await clear_user_session(user_id)
            user_states[user_id].append("awaiting_pc_selection_for_edit_qty")
            try:
                ws = ss.worksheet("Patch Cord")
                records = ws.get_all_records()
                
                if not records:
                    await message.reply_text("Tidak ada data Patch Cord untuk diubah.", reply_markup=ReplyKeyboardRemove())
                    return await show_main_menu(message)

                buttons = []
                grouped = defaultdict(int)
                for rec in records:
                    key_tuple = (
                        rec.get("Detail Perangkat", ""), 
                        rec.get("Konektor 1", ""), 
                        rec.get("Konektor 2", ""), 
                        rec.get("Ukuran (PC)", "")
                    )
                    key = tuple(str(k) for k in key_tuple)
                    
                    try: qty = int(str(rec.get("Jumlah", "0")).strip() or "0")
                    except ValueError: qty = 0
                    grouped[key] += qty
                
                for key, total_qty in sorted(grouped.items()):
                    if total_qty > 0:
                        callback_data = f"editqty_pc_{'_'.join(key)}"
                        buttons.append([InlineKeyboardButton(f"{join_detail_pc_no_ket(*key)} (Stok: {total_qty})", callback_data=callback_data)])
                
                await message.reply_text(
                    "Pilih kombinasi Patch Cord yang ingin diubah jumlahnya:", 
                    reply_markup=NAVIGATION_KEYBOARD
                )
                await message.reply_text("Daftar item:", reply_markup=InlineKeyboardMarkup(buttons))

            except Exception:
                logger.exception("Gagal memuat item untuk ubah jumlah.")
                await message.reply_text("Gagal memuat data. Mohon coba lagi.", reply_markup=ReplyKeyboardRemove())
                return await show_main_menu(message)
        
        elif text == BTN_BACK:
             return await show_main_menu(message)
        else:
            await message.reply_text("Pilihan tidak valid. Silakan pilih dari menu yang disediakan.", reply_markup=EDIT_SUBMENU_KEYBOARD)
            return

    if state == "awaiting_device_to_edit":
        if text in DEVICE_CONFIG:
            await clear_user_session(user_id)
            user_states[user_id].append("awaiting_item_selection_for_edit_ket")
            try:
                ws = ss.worksheet(DEVICE_CONFIG[text]["worksheet_name"])
                records = ws.get_all_records()

                if not records:
                    await message.reply_text(f"Tidak ada data {text} untuk diubah.", reply_markup=ReplyKeyboardRemove())
                    return await show_main_menu(message)
                
                buttons = []
                if text == "SFP":
                    for rec in records:
                        sn = rec.get("SN")
                        if sn:
                            callback_data = f"editket_sfp_row_{records.index(rec)+2}"
                            buttons.append([InlineKeyboardButton(f"SN: {sn}", callback_data=callback_data)])
                elif text == "Patch Cord":
                    grouped = defaultdict(list)
                    for rec in records:
                        key_tuple = (
                            rec.get("Detail Perangkat", ""), 
                            rec.get("Konektor 1", ""), 
                            rec.get("Konektor 2", ""), 
                            rec.get("Ukuran (PC)", "")
                        )
                        key = tuple(str(k) for k in key_tuple)
                        grouped[key].append(rec)
                    
                    for key, items in sorted(grouped.items()):
                        total_qty = sum(int(str(item.get("Jumlah", "0")).strip() or "0") for item in items)
                        callback_data = f"editket_pc_detail_{'_'.join(key)}"
                        buttons.append([InlineKeyboardButton(f"{join_detail_pc_no_ket(*key)} (Stok: {total_qty})", callback_data=callback_data)])
                
                await message.reply_text(f"Pilih item yang ingin diubah keterangannya:", reply_markup=NAVIGATION_KEYBOARD)
                await message.reply_text("Daftar item:", reply_markup=InlineKeyboardMarkup(buttons))

            except Exception:
                logger.exception("Gagal memuat item untuk edit keterangan.")
                await message.reply_text("Gagal memuat data. Mohon coba lagi.", reply_markup=ReplyKeyboardRemove())
                return await show_main_menu(message)
        else:
            await message.reply_text("Jenis perangkat tidak valid.", reply_markup=get_dynamic_keyboard(list(DEVICE_CONFIG.keys())))

    if state == "awaiting_new_ket":
        if is_non_text_message(message): return await message.reply_text("Keterangan harus teks. Jangan kirim media.")
        user_data[user_id]['new_ket'] = text.strip()
        ws_name = user_data[user_id]['worksheet_to_edit'].title
        bullets = bullets_from_detail(ws_name, user_data[user_id]['item_summary'])
        old_ket = user_data[user_id].get('old_ket','')
        user_states[user_id].append("awaiting_edit_confirmation")
        return await message.reply_text(f"Konfirmasi Ubah Keterangan - {ws_name}\n\n{bullets}\n- Keterangan Lama: '{old_ket}'\n- Keterangan Baru: '{user_data[user_id]['new_ket']}'\n\nLanjut?", reply_markup=EDIT_CONFIRM_KEYBOARD)

    if state == "awaiting_edit_confirmation":
        if text == LABEL_CONFIRM_UPDATE:
            ws = user_data[user_id]['worksheet_to_edit']; row_num = user_data[user_id]['row_to_edit']
            new_ket = user_data[user_id]['new_ket']
            await message.reply_text("Mengubah keterangan...", reply_markup=ReplyKeyboardRemove())
            try:
                ket_col = ws.row_values(1).index("Keterangan") + 1
                ws.update_cell(row_num, ket_col, new_ket)
                headers = ws.row_values(1); row_vals = ws.row_values(row_num)
                row_map = dict(zip(headers, row_vals))
                if ws.title == "Patch Cord":
                    detail_no_ket = join_detail_pc_no_ket(row_map.get('Detail Perangkat','-'), row_map.get('Konektor 1','-'),
                                                          row_map.get('Konektor 2','-'), row_map.get('Ukuran (PC)','-'))
                else:
                    detail_no_ket = join_detail_sfp_no_ket(row_map)
                append_log("UPDATE", ws.title, detail_no_ket, user_id, username, ket=new_ket)
                await message.reply_text("Keterangan berhasil diubah.")
            except Exception:
                logger.exception("Gagal ubah keterangan"); await message.reply_text("Gagal mengubah keterangan.")
            return await show_main_menu(message)
        await message.reply_text("Dibatalkan.", reply_markup=ReplyKeyboardRemove()); return await show_main_menu(message)

    if state == "awaiting_new_jumlah":
        if not re.fullmatch(r"\d+", text.strip()): return await message.reply_text("Jumlah harus angka. Contoh: 5", reply_markup=NAVIGATION_KEYBOARD)
        user_data[user_id]['new_qty'] = text.strip()
        ws_name = user_data[user_id]['worksheet_to_edit'].title
        bullets = bullets_from_detail(ws_name, user_data[user_id]['item_summary'])
        old_qty = user_data[user_id].get('old_qty','0'); new_qty = user_data[user_id]['new_qty']
        user_states[user_id].append("awaiting_edit_jumlah_confirmation")
        return await message.reply_text(f"Konfirmasi Ubah Jumlah - {ws_name}\n\n{bullets}\n- Jumlah Lama: {old_qty}\n- Jumlah Baru: {new_qty}\n\nLanjut?", reply_markup=EDIT_CONFIRM_KEYBOARD)

    if state == "awaiting_edit_jumlah_confirmation":
        if text == LABEL_CONFIRM_UPDATE:
            ws = user_data[user_id]['worksheet_to_edit']; row_num = user_data[user_id]['row_to_edit']
            new_qty = user_data[user_id]['new_qty']
            await message.reply_text("Mengubah jumlah...", reply_markup=ReplyKeyboardRemove())
            try:
                qty_col = ws.row_values(1).index("Jumlah") + 1
                ws.update_cell(row_num, qty_col, new_qty)
                headers = ws.row_values(1); row_vals = ws.row_values(row_num)
                row_map = dict(zip(headers, row_vals))
                if ws.title == "Patch Cord":
                    detail_no_ket = join_detail_pc_no_ket(row_map.get('Detail Perangkat','-'), row_map.get('Konektor 1','-'),
                                                          row_map.get('Konektor 2','-'), row_map.get('Ukuran (PC)','-'))
                else:
                    detail_no_ket = join_detail_sfp_no_ket(row_map)
                append_log("UPDATE", ws.title, detail_no_ket, user_id, username, ket=f"Jumlah diubah dari {user_data[user_id].get('old_qty','N/A')} ke {new_qty}")
                await message.reply_text("Jumlah berhasil diubah.")
            except Exception:
                logger.exception("Gagal ubah jumlah"); await message.reply_text("Gagal mengubah jumlah.")
            return await show_main_menu(message)
        await message.reply_text("Dibatalkan.", reply_markup=ReplyKeyboardRemove()); return await show_main_menu(message)

    if state == "awaiting_pemakaian_menu":
        if text == BTN_PEMAKAIAN_LOG:
            try:
                ws = get_or_create_pemakaian_ws()
                rows = ws.get_all_values()
                if len(rows) <= 1:
                    return await message.reply_text("Belum ada log pemakaian.", reply_markup=MAIN_MENU_KEYBOARD)
                last = rows[-10:] if len(rows) > 11 else rows[1:]
                blocks = ["Log Pemakaian (terbaru di bawah):",""]
                for r in last:
                    waktu, uid, uname, jenis, detail, jml, ket_barang, ket = (r+[""]*8)[:8]
                    blocks.append(f"[{waktu}] {jenis}")
                    blocks.append(bullets_from_detail(jenis, detail))
                    if jml: blocks.append(f"- Jumlah: {jml}")
                    if ket_barang: blocks.append(f"- Keterangan Barang: {ket_barang}")
                    if ket: blocks.append(f"- Keterangan Pemakaian: {ket}")
                    blocks.append("")
                return await message.reply_text("\n".join(blocks), reply_markup=MAIN_MENU_KEYBOARD)
            except Exception:
                logger.exception("Gagal ambil log pemakaian"); await message.reply_text("Gagal memuat log pemakaian.", reply_markup=MAIN_MENU_KEYBOARD)
                return await show_main_menu(message)
        if text == BTN_PEMAKAIAN_AMBIL:
            user_states[user_id].append("awaiting_consume_device_type")
            device_options = list(DEVICE_CONFIG.keys())
            return await message.reply_text("Pilih jenis perangkat yang akan diambil:", reply_markup=get_dynamic_keyboard(device_options))
        
        if text == BTN_BACK:
             return await show_main_menu(message)
        
        return await message.reply_text("Pilih salah satu menu pemakaian.", reply_markup=PEMAKAIAN_KEYBOARD)

    if state == "awaiting_consume_device_type":
        if text in DEVICE_CONFIG:
            await clear_user_session(user_id)
            if text == "SFP":
                user_states[user_id].append("awaiting_consume_sfp_type")
                sfp_types = DEVICE_CONFIG["SFP"]["questions"][0]["options"]
                buttons = [InlineKeyboardButton(t, callback_data=f"consume_sfp_type_{t}") for t in sfp_types]
                rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
                await message.reply_text("Pilih jenis SFP yang akan diambil:", reply_markup=NAVIGATION_KEYBOARD)
                return await message.reply_text("Daftar jenis:", reply_markup=InlineKeyboardMarkup(rows))
            else:
                user_states[user_id].append("awaiting_item_selection_for_consume")
                try:
                    ws = ss.worksheet(DEVICE_CONFIG[text]["worksheet_name"])
                    records = ws.get_all_records()
                    if not records:
                        await message.reply_text("Tidak ada stok untuk perangkat ini.", reply_markup=ReplyKeyboardRemove())
                        return await show_main_menu(message)
                    buttons = []
                    if text == "Patch Cord":
                        grouped = defaultdict(int)
                        for rec in records:
                            key_tuple = (rec.get(k) for k in ["Detail Perangkat", "Konektor 1", "Konektor 2", "Ukuran (PC)"])
                            key = tuple(str(k) for k in key_tuple)
                            try:
                                qty = int(str(rec.get("Jumlah", "0")).strip() or "0")
                            except ValueError:
                                qty = 0
                            grouped[key] += qty
                        for key, total_qty in sorted(grouped.items()):
                            if total_qty > 0:
                                callback_data = f"consume_pc_detail_{'_'.join(key)}"
                                buttons.append([InlineKeyboardButton(f"{join_detail_pc_no_ket(*key)} (Stok: {total_qty})", callback_data=callback_data)])
                    
                    await message.reply_text(f"Pilih item yang ingin diambil:", reply_markup=NAVIGATION_KEYBOARD)
                    await message.reply_text("Daftar item:", reply_markup=InlineKeyboardMarkup(buttons))
                except Exception:
                    logger.exception("Gagal memuat item untuk ambil barang.")
                    await message.reply_text("Gagal memuat data. Mohon coba lagi.", reply_markup=ReplyKeyboardRemove())
                    return await show_main_menu(message)
        else:
            await message.reply_text("Jenis perangkat tidak valid.", reply_markup=get_dynamic_keyboard(list(DEVICE_CONFIG.keys())))

    if state == "awaiting_consume_pc_qty":
        if not re.fullmatch(r"\d+", text.strip()): return await message.reply_text("Jumlah harus angka.", reply_markup=NAVIGATION_KEYBOARD)
        qty = int(text.strip())
        if qty <= 0: return await message.reply_text("Jumlah harus lebih dari 0.")
        data = user_data[user_id]
        
        d, k1, k2, uk = data['consume_detail'], data['consume_k1'], data['consume_k2'], data['consume_uk']
        _, _, row_data = find_patchcord_row(d,k1,k2,uk)
        if not row_data: await message.reply_text("Item tidak ditemukan.", reply_markup=NAVIGATION_KEYBOARD); return
        try:
            stok_lama = int(str(row_data.get("Jumlah","0")).strip() or "0")
        except ValueError:
            stok_lama = 0
        if qty > stok_lama: return await message.reply_text(f"Stok tidak cukup. Stok tersedia: {stok_lama}")
        user_data[user_id].update({
            "consume_qty": qty, "consume_before": stok_lama, "consume_row_data": row_data
        })
        user_states[user_id].append("awaiting_consume_pc_note")
        return await message.reply_text("Masukkan keterangan pemakaian:", reply_markup=NAVIGATION_KEYBOARD)

    if state == "awaiting_consume_pc_note":
        ket_pemakaian = text.strip()
        if not ket_pemakaian: return await message.reply_text("Keterangan pemakaian tidak boleh kosong.", reply_markup=NAVIGATION_KEYBOARD)
        data = user_data[user_id]
        d, k1, k2, uk = data['consume_detail'], data['consume_k1'], data['consume_k2'], data['consume_uk']
        qty = data["consume_qty"]
        detail_no_ket = join_detail_pc_no_ket(d, k1, k2, uk)
        user_data[user_id]["consume_ket_pemakaian"] = ket_pemakaian
        user_data[user_id]["consume_detail_no_ket"] = detail_no_ket
        preview = f"{detail_no_ket} | Jumlah: {qty} | Ket: {ket_pemakaian}"
        bullets = bullets_from_detail("Patch Cord", preview)
        user_states[user_id].append("awaiting_consume_confirm_pc")
        return await message.reply_text(f"Konfirmasi Ambil - Patch Cord\n\n{bullets}\n\nLanjut ambil?", reply_markup=TAKE_CONFIRM_KEYBOARD)

    if state == "awaiting_consume_confirm_pc":
        if text == LABEL_CONFIRM_TAKE:
            data = user_data[user_id]
            ws_name = data["consume_ws_name"]
            qty = data["consume_qty"]
            
            if 'consume_detail_no_ket' not in data:
                d, k1, k2, uk = data['consume_detail'], data['consume_k1'], data['consume_k2'], data['consume_uk']
                detail_no_ket = join_detail_pc_no_ket(d, k1, k2, uk)
                data["consume_detail_no_ket"] = detail_no_ket
            else:
                detail_no_ket = data["consume_detail_no_ket"]

            ket_pemakaian = data["consume_ket_pemakaian"]
            ket_barang = data["consume_row_data"].get("Keterangan", "")
            ws = ss.worksheet(ws_name)
            d, k1, k2, uk = data["consume_detail"], data["consume_k1"], data["consume_k2"], data["consume_uk"]
            
            await message.reply_text("Memproses pengambilan...", reply_markup=ReplyKeyboardRemove())
            try:
                _, row_num, row_data = find_patchcord_row(d, k1, k2, uk)
                if not row_num:
                    await message.reply_text("Item tidak ditemukan. Mungkin sudah diambil oleh user lain.")
                    await clear_user_session(user_id)
                    return await show_main_menu(message)
                
                stok_lama = int(str(row_data.get("Jumlah","0")).strip() or "0")
                if qty > stok_lama:
                    await message.reply_text("Stok tidak cukup lagi.")
                    await clear_user_session(user_id)
                    return await show_main_menu(message)

                stok_baru = stok_lama - qty
                qty_col = ws.row_values(1).index("Jumlah") + 1
                
                if stok_baru > 0:
                    ws.update_cell(row_num, qty_col, str(stok_baru))
                else:
                    ws.update_cell(row_num, qty_col, "0")
                
                append_pemakaian("Patch Cord", detail_no_ket, str(qty), ket_barang, ket_pemakaian, user_id, username)
                await message.reply_text(f"Barang berhasil diambil dan dicatat di log pemakaian. Sisa stok: {stok_baru}", reply_markup=MAIN_MENU_KEYBOARD)
            except Exception:
                logger.exception("Gagal proses ambil Patch Cord"); await message.reply_text("Gagal memproses pengambilan.")
            finally:
                await clear_user_session(user_id)
            return await show_main_menu(message)
        await message.reply_text("Dibatalkan.", reply_markup=ReplyKeyboardRemove()); return await show_main_menu(message)
        
    if state == "awaiting_consume_note_sfp":
        ket_pemakaian = text.strip()
        if not ket_pemakaian: return await message.reply_text("Keterangan pemakaian tidak boleh kosong.", reply_markup=NAVIGATION_KEYBOARD)
        data = user_data[user_id]
        sn = data["consume_sn"]
        detail_no_ket = join_detail_sfp_no_ket(data["consume_rowdata"])
        user_data[user_id]["consume_ket_pemakaian"] = ket_pemakaian
        user_data[user_id]["consume_detail_no_ket"] = detail_no_ket
        preview = f"SN: {sn} | Ket: {ket_pemakaian}"
        bullets = bullets_from_detail("SFP", preview)
        user_states[user_id].append("awaiting_consume_confirm_sfp")
        return await message.reply_text(f"Konfirmasi Ambil - SFP\n\n{bullets}\n\nLanjut ambil?", reply_markup=TAKE_CONFIRM_KEYBOARD)
    
    if state == "awaiting_consume_confirm_sfp":
        if text == LABEL_CONFIRM_TAKE:
            data = user_data[user_id]
            sn = data["consume_sn"]
            ket_pemakaian = data["consume_ket_pemakaian"]
            detail_no_ket = data["consume_detail_no_ket"]
            ket_barang = data["consume_rowdata"].get("Keterangan", "")
            ws = ss.worksheet(data["consume_ws_name"])
            
            await message.reply_text("Memproses pengambilan...", reply_markup=ReplyKeyboardRemove())
            try:
                headers = ws.row_values(1)
                sn_col = headers.index("SN") + 1
                try:
                    row_to_delete = ws.find(sn, in_column=sn_col).row
                    ws.delete_rows(row_to_delete)
                    renumber_worksheet(ws)
                    append_pemakaian("SFP", detail_no_ket, "1", ket_barang, ket_pemakaian, user_id, username)
                    await message.reply_text("Barang berhasil diambil dan dicatat di log pemakaian.", reply_markup=MAIN_MENU_KEYBOARD)
                except gspread.exceptions.CellNotFound:
                    await message.reply_text("SN tidak ditemukan atau sudah diambil. Mohon pilih dari daftar.", reply_markup=ReplyKeyboardRemove())
                    await clear_user_session(user_id)
                    return await show_main_menu(message)
            except Exception:
                logger.exception("Gagal proses ambil SFP"); await message.reply_text("Gagal memproses pengambilan.")
            finally:
                await clear_user_session(user_id)
            return await show_main_menu(message)
        await message.reply_text("Dibatalkan.", reply_markup=ReplyKeyboardRemove()); return await show_main_menu(message)


# =========================
# CALLBACK (DISPLAY & EDIT)
# =========================
@app.on_callback_query()
async def handle_display_callback(client: Client, q: CallbackQuery):
    user_id = q.from_user.id
    username = q.from_user.username
    await q.answer() 
    
    if q.data == "cancel_inline":
        await q.message.edit_text("Operasi dibatalkan.", reply_markup=None)
        await clear_user_session(user_id)
        await q.message.reply_text("Menu Utama:", reply_markup=MAIN_MENU_KEYBOARD)
        return

    if q.data == "consume_back":
        await q.message.delete()
        user_states[user_id].append("awaiting_pemakaian_menu")
        await q.message.reply_text("Pilih menu pemakaian:", reply_markup=PEMAKAIAN_KEYBOARD)
        return
        
    if q.data.startswith("display_"):
        if q.data == "display_close": await q.message.delete(); return
        if q.data == "display_back_to_select": await q.edit_message_text("Pilih jenis perangkat untuk rekap:", reply_markup=get_device_selection_keyboard("display")); return
        device_type = q.data.split("_", 1)[1]
        await q.edit_message_text(f"Menghitung stok untuk {device_type}...")
        try:
            config = DEVICE_CONFIG[device_type]; ws = ss.worksheet(config["worksheet_name"]); records = ws.get_all_records()
            if device_type == "Patch Cord":
                totals: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
                for r in records:
                    detail = r.get("Detail Perangkat")
                    if not detail: continue
                    key = " / ".join(str(r.get(k, "N/A")) for k in config["display_group_by"])
                    try: qty = int(str(r.get("Jumlah", "0")).strip() or "0")
                    except ValueError: qty = 0
                    totals[detail][key] += qty
                lines = ["Rekapitulasi Stok untuk Patch Cord", ""]
                for d, combos in sorted(totals.items()):
                    lines.append(d)
                    for c, t in sorted(combos.items()):
                        lines.append(f"  - {c}: {t} units")
                    lines.append("")
                resp = "\n".join(lines) if totals else "Tidak ada data untuk Patch Cord."
            else:
                grouped: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
                for r in records:
                    detail = r.get("Detail Perangkat"); sn = r.get("SN")
                    if not detail or not sn: continue
                    key = " / ".join(str(r.get(k, "N/A")) for k in config["display_group_by"])
                    grouped[detail][key].append(str(sn))
                if not grouped:
                    resp = "Tidak ada data untuk SFP."
                else:
                    MAX_SN = 20
                    lines = ["Rekapitulasi Stok untuk SFP", ""]
                    for d, combos in sorted(grouped.items()):
                        lines.append(d)
                        for c, lst in sorted(combos.items()):
                            lines.append(f"  • {c}: {len(lst)} unit")
                            show = lst[:MAX_SN]
                            lines.extend([f"    - {s}" for s in show])
                            if len(lst) > MAX_SN:
                                lines.append(f"    (+{len(lst)-MAX_SN} lainnya)")
                        lines.append("")
                    resp = "\n".join(lines)
            await q.edit_message_text(resp, reply_markup=get_device_selection_keyboard("display"))
        except Exception:
            logger.exception("Gagal ambil data display"); await q.edit_message_text("Gagal mengambil data.")
        
    if q.data.startswith("editket_"):
        await q.message.delete()
        parts = q.data.split("_")
        device_type = parts[1]
        
        if device_type == "sfp":
            row_num = int(parts[3])
            ws = ss.worksheet("SFP")
            headers = ws.row_values(1)
            row_data = dict(zip(headers, ws.row_values(row_num)))
            
            summary = build_summary_text(ws.title, row_data)
            user_data[user_id].update({'worksheet_to_edit': ws, 'row_to_edit': row_num, 'old_ket': row_data.get('Keterangan',''), 'item_summary': summary})
            user_states[user_id].append("awaiting_new_ket")
            await q.message.reply_text(f"Keterangan sekarang: {row_data.get('Keterangan','(kosong)')}\nKirim keterangan baru:", reply_markup=NAVIGATION_KEYBOARD)
        elif device_type == "pc":
            pc_detail_str = "_".join(parts[3:])
            pc_detail = pc_detail_str.split("_")
            d, k1, k2, uk = pc_detail[0], pc_detail[1], pc_detail[2], pc_detail[3]
            
            ws, row_num, row_data = find_patchcord_row(d,k1,k2,uk)
            if not row_num:
                await q.message.reply_text("Item tidak ditemukan. Mohon coba lagi.", reply_markup=ReplyKeyboardRemove())
                return await show_main_menu(q.message)

            summary = build_summary_text(ws.title, row_data)
            user_data[user_id].update({
                'worksheet_to_edit': ws, 
                'row_to_edit': row_num, 
                'old_ket': row_data.get('Keterangan',''),
                'item_summary': summary
            })
            user_states[user_id].append("awaiting_new_ket")
            await q.message.reply_text(f"Keterangan sekarang: {row_data.get('Keterangan','(kosong)')}\nKirim keterangan baru:", reply_markup=NAVIGATION_KEYBOARD)
        
    if q.data.startswith("editqty_pc_"):
        await q.message.delete()
        parts = q.data.split("_")
        pc_detail_str = "_".join(parts[2:])
        d, k1, k2, uk = pc_detail_str.split("_")
        
        ws, row_num, row_data = find_patchcord_row(d,k1,k2,uk)
        if not row_num:
            await q.message.reply_text("Item tidak ditemukan. Mungkin sudah dihapus.", reply_markup=ReplyKeyboardRemove())
            await clear_user_session(user_id)
            return await show_main_menu(q.message)

        user_data[user_id].update({
            'worksheet_to_edit': ws,
            'row_to_edit': row_num,
            'old_qty': str(row_data.get('Jumlah','0')),
            'item_summary': build_summary_text(ws.title, row_data),
        })
        
        user_states[user_id].append("awaiting_new_jumlah")
        await q.message.reply_text(f"Jumlah sekarang: {row_data.get('Jumlah','0')}\nKirim jumlah baru (angka):", reply_markup=NAVIGATION_KEYBOARD)
        
    if q.data.startswith("consume_"):
        await q.message.delete()
        parts = q.data.split("_")
        device_type = parts[1]
        
        if device_type == "sfp":
            if parts[2] == "type":
                sfp_type = parts[3]
                user_data[user_id]["consume_sfp_type"] = sfp_type
                try:
                    ws = ss.worksheet("SFP")
                    records = ws.get_all_records()
                    
                    if not records:
                        await clear_user_session(user_id)
                        await q.message.reply_text("Tidak ada stok SFP sama sekali di dalam sheet.", reply_markup=MAIN_MENU_KEYBOARD)
                        return
                    
                    filtered_sns = [rec["SN"] for rec in records if rec.get("Detail Perangkat") == sfp_type and rec.get("SN")]
                    
                    if not filtered_sns:
                        await clear_user_session(user_id)
                        await q.message.reply_text(f"Tidak ada stok untuk jenis SFP \"{sfp_type}\".", reply_markup=MAIN_MENU_KEYBOARD)
                        return
                    
                    buttons = [[InlineKeyboardButton(f"SN: {sn}", callback_data=f"consume_sfp_sn_{sfp_type}_{sn}")] for sn in filtered_sns]
                    await q.message.reply_text(f"Pilih SN {sfp_type} yang akan diambil:", reply_markup=NAVIGATION_KEYBOARD)
                    await q.message.reply_text("Daftar SN:", reply_markup=InlineKeyboardMarkup(buttons))
                except gspread.exceptions.WorksheetNotFound:
                    await clear_user_session(user_id)
                    await q.message.reply_text("Sheet 'SFP' tidak ditemukan. Stok dianggap kosong.", reply_markup=MAIN_MENU_KEYBOARD)
                    return
                except Exception:
                    logger.exception("Gagal memuat SN SFP."); await q.message.reply_text("Gagal memuat data. Mohon coba lagi.")
                    await show_main_menu(q.message)
                    
            elif parts[2] == "sn":
                sn = parts[4]
                ws, row_num, row_data = find_sn_in_all_sheets(sn)
                if not row_num:
                    await q.message.reply_text("SN tidak ditemukan atau sudah diambil.", reply_markup=ReplyKeyboardRemove())
                    await clear_user_session(user_id)
                    return await show_main_menu(q.message)
                    
                user_data[user_id].update({
                    "consume_ws_name": "SFP", 
                    "consume_row": row_num, 
                    "consume_sn": sn,
                    "consume_rowdata": row_data
                })
                user_states[user_id].append("awaiting_consume_note_sfp")
                await q.message.reply_text("Masukkan keterangan pemakaian:", reply_markup=NAVIGATION_KEYBOARD)
        
        elif device_type == "pc":
            pc_detail_str = "_".join(parts[3:])
            d, k1, k2, uk = pc_detail_str.split("_")
            
            ws, row_num, row_data = find_patchcord_row(d,k1,k2,uk)
            if not row_num:
                await q.message.reply_text("Kombinasi tidak ditemukan.", reply_markup=ReplyKeyboardRemove())
                await clear_user_session(user_id)
                return await show_main_menu(q.message)

            user_data[user_id].update({
                "consume_ws_name": "Patch Cord",
                "consume_detail": d,
                "consume_k1": k1,
                "consume_k2": k2,
                "consume_uk": uk,
                "consume_row_data": row_data,
                "consume_detail_no_ket": join_detail_pc_no_ket(d, k1, k2, uk)
            })
            user_states[user_id].append("awaiting_consume_pc_qty")
            await q.message.reply_text(f"Masukkan jumlah yang akan diambil (stok tersedia: {row_data.get('Jumlah','0')}):", reply_markup=NAVIGATION_KEYBOARD)

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    logger.info("Bot starting...")
    app.run()
    logger.info("Bot stopped.")