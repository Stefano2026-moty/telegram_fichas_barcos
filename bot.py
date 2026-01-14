import os
import re
import pandas as pd
from rapidfuzz import process, fuzz
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    MessageHandler,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ======================
# CONFIG
# ======================

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CATALOG_PATH = "catalog.csv"

DEFAULT_LANG = "es"
DEFAULT_VARIANT = "standard"

LANG_HINTS = {
    "es": {"es", "spa", "spanish", "español", "espanol", "castellano", "spagnolo"},
    "en": {"en", "eng", "english", "inglese", "inglés", "ingles"},
}
VARIANT_HINTS = {
    "broker": {"broker", "bf", "brok"},
    "standard": {"standard", "std", "normale"},
}

UNIT_REF_REGEX = re.compile(r"(#|unit|unidad|ordine|order|n\.?)\s*(\d{1,5})", re.IGNORECASE)

# callback data prefixes
CB_UNIT = "UNIT:"
CB_LANG = "LANG:"
CB_VAR = "VAR:"
CB_CANCEL = "CANCEL"

# ======================
# NORMALIZE / DETECT
# ======================

def normalize(text: str) -> str:
    text = str(text).lower().strip()
    text = re.sub(r"[^\w\s#\.]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text

def detect_lang(query: str):
    tokens = set(normalize(query).split())
    for lang, hints in LANG_HINTS.items():
        if tokens.intersection(hints):
            return lang
    return None

def detect_variant(query: str):
    tokens = set(normalize(query).split())
    for var, hints in VARIANT_HINTS.items():
        if tokens.intersection(hints):
            return var
    return None

def detect_unit_ref(query: str):
    m = UNIT_REF_REGEX.search(query)
    if m:
        return m.group(2)
    return None

# ======================
# CSV LOADING (robusto , o ;)
# ======================

def read_catalog_csv(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
        if df.shape[1] == 1:
            df = pd.read_csv(path, sep=";")
        return df
    except Exception:
        return pd.read_csv(path, sep=";")

def load_catalog():
    if not TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN non trovato. Controlla .env nella stessa cartella di bot.py")

    df = read_catalog_csv(CATALOG_PATH)
    df.columns = [str(c).strip() for c in df.columns]
    print("✅ Colonne lette dal CSV:", df.columns.tolist())

    required = ["Brand", "Model", "Unit_ref", "Variant", "Language", "Availability", "Delivery", "URL", "Aliases"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"❌ Mancano colonne nel CSV: {missing}")

    catalog = []
    for _, r in df.iterrows():
        raw_aliases = str(r.get("Aliases", ""))
        aliases = [a.strip() for a in raw_aliases.split("|") if a.strip()]

        keys = set()
        keys.add(str(r["Model"]))
        keys.add(f"{r['Brand']} {r['Model']}")
        for a in aliases:
            keys.add(a)

        keys_norm = [normalize(k) for k in keys]

        catalog.append({
            "brand": str(r["Brand"]).strip(),
            "model": str(r["Model"]).strip(),
            "unit_ref": str(r["Unit_ref"]).strip(),
            "variant": str(r["Variant"]).lower().strip(),
            "lang": str(r["Language"]).lower().strip(),
            "availability": str(r["Availability"]).lower().strip(),
            "delivery": str(r["Delivery"]).strip(),
            "url": str(r["URL"]).strip(),
            "keys_norm": keys_norm,
        })

    return catalog

CATALOG = load_catalog()

# ======================
# MATCH MODEL
# ======================

def best_model_match(query: str):
    qn = normalize(query)
    choices = []
    mapping = []

    for item in CATALOG:
        for k in item["keys_norm"]:
            choices.append(k)
            mapping.append(item)

    hit = process.extractOne(qn, choices, scorer=fuzz.WRatio)
    if not hit:
        return None, 0
    return mapping[hit[2]], hit[1]

def candidates_for_query(query: str):
    want_unit = detect_unit_ref(query)
    base_item, score = best_model_match(query)
    if not base_item or score < 70:
        return [], score

    cands = [x for x in CATALOG if x["brand"] == base_item["brand"] and x["model"] == base_item["model"]]

    if want_unit:
        filtered = [x for x in cands if x["unit_ref"] == want_unit]
        if filtered:
            cands = filtered

    return cands, score

def unique_units(cands):
    # ordina unità con criterio: in_stock prima, poi delivery, poi numero
    def unit_rank(u):
        # u: sample row
        av = u["availability"]
        av_rank = 0 if av == "in_stock" else 1
        return (av_rank, u.get("delivery", ""), u.get("unit_ref", ""))
    units = {}
    for x in cands:
        units.setdefault(x["unit_ref"], x)
    return [units[k] for k in sorted(units.keys(), key=lambda ur: unit_rank(units[ur]))]

def filter_doc(cands, unit_ref=None, lang=None, variant=None):
    out = cands
    if unit_ref:
        out = [x for x in out if x["unit_ref"] == unit_ref]
    if lang:
        out = [x for x in out if x["lang"] == lang]
    if variant:
        out = [x for x in out if x["variant"] == variant]
    return out

# ======================
# INTERACTIVE FLOW
# ======================

def reset_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("flow", None)

def ensure_flow(context: ContextTypes.DEFAULT_TYPE):
    if "flow" not in context.user_data:
        context.user_data["flow"] = {
            "query": None,
            "cands": [],
            "unit_ref": None,
            "lang": None,
            "variant": None,
            "stage": None,  # "unit" | "lang" | "variant"
        }
    return context.user_data["flow"]

async def ask_unit(update: Update, context: ContextTypes.DEFAULT_TYPE, flow):
    units = unique_units(flow["cands"])
    if len(units) <= 1:
        # nessuna scelta necessaria
        flow["unit_ref"] = units[0]["unit_ref"] if units else None
        return False

    buttons = []
    for u in units[:10]:
        title = f"#{u['unit_ref']} · {u['availability']} · {u.get('delivery','')}".strip()
        buttons.append([InlineKeyboardButton(title, callback_data=f"{CB_UNIT}{u['unit_ref']}")])

    buttons.append([InlineKeyboardButton("❌ Annulla", callback_data=CB_CANCEL)])
    flow["stage"] = "unit"

    await update.message.reply_text(
        "Ho trovato più unità per questo modello. Quale vuoi?",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return True

async def ask_lang(update: Update, context: ContextTypes.DEFAULT_TYPE, flow):
    # se già impostata, skip
    if flow["lang"]:
        return False

    buttons = [
        [InlineKeyboardButton("🇪🇸 Spagnolo (ES)", callback_data=f"{CB_LANG}es"),
         InlineKeyboardButton("🇬🇧 Inglese (EN)", callback_data=f"{CB_LANG}en")],
        [InlineKeyboardButton("❌ Annulla", callback_data=CB_CANCEL)],
    ]
    flow["stage"] = "lang"
    await update.effective_message.reply_text(
        "In che lingua vuoi la scheda?",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return True

async def ask_variant(update: Update, context: ContextTypes.DEFAULT_TYPE, flow):
    if flow["variant"]:
        return False

    buttons = [
        [InlineKeyboardButton("Standard", callback_data=f"{CB_VAR}standard"),
         InlineKeyboardButton("Broker Friendly", callback_data=f"{CB_VAR}broker")],
        [InlineKeyboardButton("❌ Annulla", callback_data=CB_CANCEL)],
    ]
    flow["stage"] = "variant"
    await update.effective_message.reply_text(
        "Che versione vuoi?",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return True

async def finalize_and_send(update: Update, context: ContextTypes.DEFAULT_TYPE, flow):
    # prova a trovare 1 documento
    docs = filter_doc(flow["cands"], flow["unit_ref"], flow["lang"], flow["variant"])

    # fallback intelligenti
    if not docs and flow["lang"] and flow["variant"]:
        # prova stessa unità e lingua, ma qualsiasi variant
        docs = filter_doc(flow["cands"], flow["unit_ref"], flow["lang"], None)
    if not docs and flow["lang"]:
        docs = filter_doc(flow["cands"], flow["unit_ref"], flow["lang"], None)
    if not docs:
        docs = filter_doc(flow["cands"], flow["unit_ref"], None, None)

    if not docs:
        await update.effective_message.reply_text("❌ Non ho trovato un documento compatibile con la scelta.")
        reset_flow(context)
        return

    item = docs[0]
    msg = (
        f"📄 {item['brand']} {item['model']}  #{item['unit_ref']}\n"
        f"🌐 {item['lang'].upper()} | 🧾 {item['variant']}\n"
        f"📦 {item['availability']} | 🚚 {item.get('delivery','')}\n"
        f"🔗 {item['url']}"
    )
    await update.effective_message.reply_text(msg)
    reset_flow(context)

async def run_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    flow = ensure_flow(context)
    flow["query"] = query

    # pre-imposta preferenze se l’utente le ha scritte
    flow["lang"] = detect_lang(query) or flow["lang"]
    flow["variant"] = detect_variant(query) or flow["variant"]
    unit = detect_unit_ref(query)
    if unit:
        flow["unit_ref"] = unit

    cands, score = candidates_for_query(query)
    if not cands:
        await update.message.reply_text("❌ Nessuna scheda trovata. Prova con marca + modello (es. 'Pardo P43') o con alias (es. 'SO455 #49').")
        reset_flow(context)
        return

    flow["cands"] = cands

    # 1) chiedi unità se necessario
    if not flow["unit_ref"]:
        asked = await ask_unit(update, context, flow)
        if asked:
            return

    # 2) chiedi lingua
    asked = await ask_lang(update, context, flow)
    if asked:
        return

    # 3) chiedi variante
    asked = await ask_variant(update, context, flow)
    if asked:
        return

    # 4) invia documento
    await finalize_and_send(update, context, flow)

# ======================
# TELEGRAM HANDLERS
# ======================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reset_flow(context)
    await update.message.reply_text(
        "✅ Bot schede pronto.\n\n"
        "Scrivi un modello/alias.\n"
        "Esempi:\n"
        "• Pardo P43\n"
        "• Pardo P43 #126 broker en\n"
        "• SO455 #49\n"
        "• JSO415 unit 12 es\n\n"
        "Tip: se ci sono più opzioni, ti farò scegliere con pulsanti."
    )

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reset_flow(context)
    await update.message.reply_text("Operazione annullata ✅")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    await run_flow(update, context, query)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    flow = ensure_flow(context)

    if q.data == CB_CANCEL:
        reset_flow(context)
        await q.edit_message_text("Operazione annullata ✅")
        return

    # unit selection
    if q.data.startswith(CB_UNIT):
        unit = q.data.split(":", 1)[1]
        flow["unit_ref"] = unit
        await q.edit_message_text(f"Ok, scelgo unità #{unit} ✅")
        # continua con domande successive
        await ask_lang(update, context, flow)
        return

    # language selection
    if q.data.startswith(CB_LANG):
        lang = q.data.split(":", 1)[1]
        flow["lang"] = lang
        await q.edit_message_text(f"Ok, lingua {lang.upper()} ✅")
        await ask_variant(update, context, flow)
        return

    # variant selection
    if q.data.startswith(CB_VAR):
        var = q.data.split(":", 1)[1]
        flow["variant"] = var
        await q.edit_message_text(f"Ok, versione {var} ✅")
        await finalize_and_send(update, context, flow)
        return

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling()

if __name__ == "__main__":
    main()