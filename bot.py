# bot_v14.py (VERSION 14.0) — Production-ready, Database-backed State
# - Fixes critical in-memory state loss on restart by moving all user
#   sessions (batching, convos, edits) to MongoDB.
# - Fixes payment race condition with a unique DB index and retry logic.
# - Fixes Flask webhook blocking by making it non-blocking (HTTP 202).
# - Hardens the async coroutine scheduler.
# -----------------------------------------------------------------------------

import os
import logging
import random
import string
import asyncio
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import pytz
import re
from urllib.parse import urlparse, parse_qs, quote_plus

from pyrogram import Client, filters, enums, idle
from pyrogram.errors import UserNotParticipant, FloodWait, UserIsBlocked, InputUserDeactivated, MessageNotModified
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery

# Sync pymongo for APScheduler jobstore and index creation
from pymongo import MongoClient, IndexModel
from pymongo.errors import DuplicateKeyError

# Async motor for bot DB ops
from motor.motor_asyncio import AsyncIOMotorClient

from flask import Flask, request, jsonify
from threading import Thread
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.base import JobLookupError

load_dotenv()

# -------------------------
#
# Config from environment
#
# -------------------------

API_ID = int(os.environ.get("API_ID", "2468192"))
API_HASH = os.environ.get("API_HASH", "4906b3f8f198ec0e24edb2c197677678")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "7845953013:AAEwrzoPYM_5CGevF8n6xEqwbOqncoqnc6g")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://Rashmika1:Rashmika@rashmika1.sbnf8wy.mongodb.net/?retryWrites=true&w=majority&appName=Rashmika1")
LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", "-1001753089099"))
UPDATE_CHANNEL = os.environ.get("UPDATE_CHANNEL", "linkz_ki_duniyaa")  # channel username without @
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "2098589219")
ADMINS = [int(x) for x in ADMIN_IDS_STR.split(",") if x.strip().isdigit()]
PAYMENT_PAGE_URL = os.environ.get("PAYMENT_PAGE_URL", "https://t.me/Nikhil5757h")
AUTOMATION_SECRET = os.environ.get("AUTOMATION_SECRET", "payment4telegram")
FREE_DELETE_DELAY_MINUTES = int(os.environ.get("FREE_DELETE_DELAY_MINUTES", "30"))
PAID_DELETE_DELAY_HOURS = int(os.environ.get("PAID_DELETE_DELAY_HOURS", "24"))
PAYMENT_EXPIRATION_MINUTES = int(os.environ.get("PAYMENT_EXPIRATION_MINUTES", "30"))
APPROVAL_EXPIRATION_HOURS = int(os.environ.get("APPROVAL_EXPIRATION_HOURS", "24"))
IST = pytz.timezone("Asia/Kolkata")
FLASK_PORT = int(os.environ.get("PORT", "8080"))

# -------------------------
#
# Logging
#
# -------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# -------------------------
#
# Flask app (runs in separate thread)
#
# -------------------------

flask_app = Flask(__name__)

@flask_app.route("/", methods=["GET"])
def root_status():
    return "Bot is alive!", 200

@flask_app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"status": "ok", "time": datetime.now(IST).isoformat()}), 200

@flask_app.route("/status", methods=["GET"])
def status():
    try:
        ok = sync_mongo_client is not None
        return jsonify({"status": "ok" if ok else "degraded", "db": "ok" if ok else "down"}), 200 if ok else 500
    except Exception:
        return jsonify({"status": "error"}), 500

@flask_app.route("/api/shortcut", methods=["POST"])
def shortcut_webhook():
    # Automation webhook: will auto-approve payments if header matches AUTOMATION_SECRET
    provided_secret = request.headers.get("X-Shortcut-Secret")
    if not AUTOMATION_SECRET or provided_secret != AUTOMATION_SECRET:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    sms_text = request.get_data(as_text=True)
    if not sms_text:
        return jsonify({"status": "error", "message": "Bad Request: SMS text is missing"}), 400

    amount_match = re.search(r"(?:Rs\.?|₹|INR)\s*([\d,]+\.\d{2})", sms_text)
    if not amount_match:
        return jsonify({"status": "info", "message": "No valid amount found in SMS"}), 200

    unique_amount = amount_match.group(1).replace(",", "")
    
    # --- FIX 4: Run auto-approval in the background and return 202 Accepted ---
    # This prevents the webhook from blocking the Flask server.
    coro = _auto_approve_by_amount(unique_amount)
    schedule_coroutine(coro)
    
    return jsonify({"status": "accepted", "message": "Automation job scheduled"}), 202

def run_flask():
    logger.info(f"Starting Flask on 0.0.0.0:{FLASK_PORT}")
    flask_app.run(host="0.0.0.0", port=FLASK_PORT)

# -------------------------
#
# MongoDB setup
#
# -------------------------

if not MONGO_URI:
    logger.error("MONGO_URI is not set. Exiting.")
    raise SystemExit("MONGO_URI not provided")

sync_mongo_client = MongoClient(MONGO_URI)
async_mongo_client = AsyncIOMotorClient(MONGO_URI)
db_async = async_mongo_client["file_link_bot"]
files_collection = db_async["file_batches"]
users_collection = db_async["users"]
settings_collection = db_async["settings"]
payments_collection = db_async["pending_payments"]

# --- FIX 1: New collections for database-backed state ---
# These replace the in-memory dicts (user_sessions, user_states, EDIT_SESSIONS)
sessions_collection = db_async["user_sessions"]
states_collection = db_async["user_states"]
edit_sessions_collection = db_async["edit_sessions"]
# --------------------------------------------------------

jobstores = {"default": MongoDBJobStore(database="file_link_bot", collection="scheduler_jobs", client=sync_mongo_client)}
scheduler = BackgroundScheduler(jobstores=jobstores, timezone="Asia/Kolkata")

async def setup_database_indexes():
    """Creates required indexes on startup, including TTLs for state."""
    logger.info("Setting up database indexes...")
    try:
        # --- FIX 2: Unique index for payment race condition ---
        await payments_collection.create_indexes([
            IndexModel("unique_amount", unique=True, sparse=True)
        ])
        
        # --- FIX 1: TTL (Time-To-Live) indexes for state collections ---
        # Expire file-batching sessions after 1 hour of inactivity
        await sessions_collection.create_indexes([
            IndexModel("last_updated", expireAfterSeconds=3600)
        ])
        # Expire conversational states (like 'waiting_for_price') after 15 mins
        await states_collection.create_indexes([
            IndexModel("last_updated", expireAfterSeconds=900)
        ])
        # Expire /editlink sessions after 2 hours of inactivity
        await edit_sessions_collection.create_indexes([
            IndexModel("last_updated", expireAfterSeconds=7200)
        ])
        
        logger.info("Database indexes are set up.")
    except Exception as e:
        logger.error(f"Failed to create indexes: {e}")
        raise

# -------------------------
#
# Pyrogram client (bot)
#
# -------------------------

app = Client("FileLinkBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# -------------------------
#
# In-memory states (REMOVED)
#
# --- FIX 1: All state is now in MongoDB ---
# user_sessions = {}   # REPLACED by sessions_collection
# user_states = {}     # REPLACED by states_collection
# EDIT_SESSIONS = {}   # REPLACED by edit_sessions_collection
#
# -------------------------

# -------------------------
#
# Utility helpers
#
# -------------------------

def generate_random_string(length: int = 10) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))

async def get_bot_mode() -> str:
    setting = await settings_collection.find_one({"_id": "bot_mode"})
    return setting.get("mode", "public") if setting else "public"

async def add_user_to_db(message: Message):
    usr = message.from_user
    doc = {
        "first_name": getattr(usr, "first_name", ""),
        "last_name": getattr(usr, "last_name", ""),
        "username": getattr(usr, "username", None),
        "banned": False,
    }
    await users_collection.update_one({"_id": usr.id}, {"$set": doc, "$setOnInsert": {"joined_date": datetime.now(timezone.utc)}}, upsert=True)

async def is_user_member(client: Client, user_id: int) -> bool:
    if not UPDATE_CHANNEL:
        return True
    try:
        await client.get_chat_member(chat_id=f"@{UPDATE_CHANNEL}", user_id=user_id)
        return True
    except UserNotParticipant:
        return False
    except Exception:
        return False

def sanitize_upi(upi: str) -> str:
    return upi.strip()

def schedule_coroutine(coro):
    # --- FIX 3: Hardened async scheduler helper ---
    if app.is_connected and app.loop:
        return asyncio.run_coroutine_threadsafe(coro, app.loop)
    else:
        logger.warning("Bot is not connected or loop is not available. Coroutine scheduling skipped.")
        return None

def delete_message_job(chat_id: int, message_ids: list):
    async def task():
        try:
            await app.delete_messages(chat_id=chat_id, message_ids=message_ids)
        except Exception as e:
            logger.warning(f"delete_message_job failed for {chat_id}/{message_ids}: {e}")
    schedule_coroutine(task())

def expire_payment_job(payment_id: str, user_id: int, batch_id: str):
    async def task():
        pr = await payments_collection.find_one({"_id": payment_id})
        if pr:
            await payments_collection.delete_one({"_id": payment_id})
            logger.info(f"Payment session {payment_id} expired for user {user_id}.")
            try:
                bot_me = await app.get_me()
                share_link = f"https://t.me/{bot_me.username}?start={batch_id}"
                await app.send_message(user_id, f"⏳ Your payment session has expired because you did not confirm the payment within {PAYMENT_EXPIRATION_MINUTES} minutes.\n\n__Please generate a new payment link by clicking here: {share_link}")
            except Exception:
                pass
    schedule_coroutine(task())

def expire_approval_job(payment_id: str, user_id: int, owner_id: int):
    async def task():
        pr = await payments_collection.find_one({"_id": payment_id})
        if pr:
            await payments_collection.delete_one({"_id": payment_id})
            logger.info(f"Approval request {payment_id} for user {user_id} expired.")
            try:
                await app.send_message(user_id, f"😔 We are sorry, but the seller did not respond to your payment confirmation within {APPROVAL_EXPIRATION_HOURS} hours. Your request has been cancelled.")
                await app.send_message(owner_id, f"⚠️ The payment approval request for user **{user_id}** has expired because you did not take action.")
            except Exception:
                pass
    schedule_coroutine(task())

# -------------------------
#
# UI texts & keyboard helpers
#
# -------------------------

def get_start_text():
    return "Hey! I am PermaStore Bot 🤖\n\nSend Me Any File! And I'll Give You A Permanent Shareable Link! Which Never EXPIRES."

def get_start_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("ℹ️ Help", callback_data="show_help")],
    ])

def get_help_text_and_keyboard(user_id: int):
    base_help = (
        "Here's how to use me:\n\n"
        "1. Send Files: Send me any file, or forward multiple files at once.\n\n"
        "2. Use the Menu: After you send a file, a menu will appear:\n\n"
        "   - 🔗 Get Free Link: Creates a permanent link for all files in your batch.\n\n"
        "   - ➕ Add More Files: Allows you to send more files to the current batch.\n\n"
        "Available Commands:\n"
        "/start - Restart the bot and clear any session.\n\n"
        "/editlink <batch_id> - Edit an existing link you created.\n\n"
        "/help - Show this help message.\n\n"
    )
    if user_id in ADMINS:
        admin_extra = (
            "/setupi - Save or update your UPI ID for receiving payments.\n"
            "/myupi - Check your currently saved UPI ID.\n"
            "👑 Admin Commands:\n"
            "/stats - Get bot statistics.\n"
            "/settings - Change bot mode (Public/Private).\n"
            "/ban <user_id> - Ban a user.\n"
            "/unban <user_id> - Unban a user.\n"
            "/linkinfo <batch_id> - Get details of a specific link.\n"
        )
        return base_help + admin_extra, InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Home", callback_data="back_to_start")]
        ])
    return base_help, InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Home", callback_data="back_to_start")]
    ])

# -------------------------
#
# Handlers
#
# -------------------------

@app.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    await add_user_to_db(message)
    user_id = message.from_user.id
    
    # --- FIX 1: Clear existing session from DB ---
    session = await sessions_collection.find_one_and_delete({"_id": user_id})
    if session:
        try:
            menu_msg_id = session.get("menu_msg_id")
            if menu_msg_id:
                await client.delete_messages(user_id, menu_msg_id)
        except Exception:
            pass
    # Clear any pending conversational state
    await states_collection.delete_one({"_id": user_id})
    # ----------------------------------------------

    if len(message.command) > 1:
        batch_id = message.command[1]
        if not await is_user_member(client, user_id):
            join_btn = InlineKeyboardButton("🔗 Join Channel", url=f"https://t.me/{UPDATE_CHANNEL}")
            joined_btn = InlineKeyboardButton("✅ I Have Joined", callback_data=f"check_join_{batch_id}")
            await message.reply("__👋 **Hello!**\n\nJoin Our Update Channel To Access The Content.__", reply_markup=InlineKeyboardMarkup([[join_btn], [joined_btn]]))
            return
        await process_link_click(client, user_id, batch_id)
    else:
        await message.reply(get_start_text(), reply_markup=get_start_keyboard())

@app.on_callback_query(filters.regex(r"^check_join_"))
async def check_join_callback(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    try:
        batch_id = query.data.split("_", 2)[2]
    except IndexError:
        await query.answer("Invalid request.", show_alert=True)
        return

    if await is_user_member(client, user_id):
        await query.answer("Welcome! You can now access the content.", show_alert=False)
        try:
            await query.message.delete()
        except Exception as e:
            logger.warning(f"Could not delete join message for user {user_id}: {e}")
        await process_link_click(client, user_id, batch_id)
    else:
        await query.answer("❌ You haven't joined the channel yet. Please join and then click the button again.", show_alert=True)

@app.on_message(filters.command("help") & filters.private)
async def help_handler(client: Client, message: Message):
    help_text, help_keyboard = get_help_text_and_keyboard(message.from_user.id)
    await message.reply(help_text, reply_markup=help_keyboard, disable_web_page_preview=True)

@app.on_callback_query(filters.regex("^(show_help|back_to_start)$"))
async def navigation_callbacks(client: Client, query: CallbackQuery):
    action = query.data
    try:
        if action == "show_help":
            help_text, help_keyboard = get_help_text_and_keyboard(query.from_user.id)
            await query.message.edit_text(help_text, reply_markup=help_keyboard, disable_web_page_preview=True)
        elif action == "back_to_start":
            await query.message.edit_text(get_start_text(), reply_markup=get_start_keyboard())
    except MessageNotModified:
        await query.answer()
    except Exception as e:
        logger.exception("navigation_callbacks error")
        await query.answer("An error occurred.", show_alert=True)

@app.on_callback_query(filters.regex("^close_msg$"))
async def close_message_callback(client: Client, query: CallbackQuery):
    try:
        await query.message.delete()
        # --- FIX 1: Clear session from DB ---
        await sessions_collection.delete_one({"_id": query.from_user.id})
    except Exception as e:
        logger.warning(f"close_msg failed: {e}")
        await query.answer("Could not delete message.", show_alert=True)

@app.on_message(filters.command("stats") & filters.private & filters.user(ADMINS))
async def stats_handler(client: Client, message: Message):
    total_users = await users_collection.count_documents({})
    banned_users = await users_collection.count_documents({"banned": True})
    total_batches = await files_collection.count_documents({})
    paid_batches = await files_collection.count_documents({"is_paid": True})
    await message.reply(
        "📊 Bot Statistics\n\n"
        f"👤 Users:\n   - Total Users: **{total_users}**\n   - Banned Users: **{banned_users}**\n\n"
        f"🔗 Links (Batches):\n   - Total Batches: **{total_batches}**\n   - Paid Batches: **{paid_batches}**\n   - Free Batches: **{total_batches - paid_batches}**"
    )

@app.on_message(filters.command("ban") & filters.private & filters.user(ADMINS))
async def ban_handler(client: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("Usage: `/ban <user_id>`")
        return
    try:
        user_id_to_ban = int(message.command[1])
        if user_id_to_ban in ADMINS:
            await message.reply("❌ You cannot ban an admin.")
            return
        await users_collection.update_one({"_id": user_id_to_ban}, {"$set": {"banned": True}}, upsert=True)
        await message.reply(f"✅ User **{user_id_to_ban}** has been banned successfully.")
    except ValueError:
        await message.reply("Invalid User ID provided.")

@app.on_message(filters.command("unban") & filters.private & filters.user(ADMINS))
async def unban_handler(client: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("Usage: `/unban <user_id>`")
        return
    try:
        user_id_to_unban = int(message.command[1])
        await users_collection.update_one({"_id": user_id_to_unban}, {"$set": {"banned": False}}, upsert=True)
        await message.reply(f"✅ User **{user_id_to_unban}** has been unbanned.")
    except ValueError:
        await message.reply("Invalid User ID provided.")

@app.on_message(filters.command("linkinfo") & filters.private & filters.user(ADMINS))
async def linkinfo_handler(client: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("Usage: `/linkinfo <batch_id>`")
        return
    batch_id = message.command[1]
    batch_record = await files_collection.find_one({"_id": batch_id})
    if not batch_record:
        await message.reply(f"❌ No link found with Batch ID: **{batch_id}**")
        return
    owner_id = batch_record.get("owner_id")
    owner_info = await users_collection.find_one({"_id": owner_id})
    owner_details = f"{owner_info.get('first_name', '')} (@{owner_info.get('username', 'N/A')})" if owner_info else "Unknown (Not in DB)"
    link_type = "Paid 💰" if batch_record.get("is_paid") else "Free 🆓"
    file_count = len(batch_record.get("message_ids", []))
    text = (
        f"🔗 Link Information\n\n- Batch ID: **{batch_id}**\n- Link Type: {link_type}\n- File Count: **{file_count}**\n\n"
        f"👤 Owner Details:\n- User ID: **{owner_id}**\n- Name/Username: {owner_details}\n"
    )
    if batch_record.get("is_paid"):
        text += f"- Price: **₹{batch_record.get('price', 0):.2f}**\n- UPI ID: **{batch_record.get('upi_id', 'N/A')}**"
    await message.reply(text)

@app.on_message(filters.command("settings") & filters.private & filters.user(ADMINS))
async def settings_handler(client: Client, message: Message):
    current_mode = await get_bot_mode()
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🌐 Public", callback_data="set_mode_public"),
            InlineKeyboardButton("🔒 Private", callback_data="set_mode_private")
        ]
    ])
    await message.reply(f"⚙️ Bot Settings\n\nCurrent Mode: **{current_mode.upper()}**.\nSelect A New Mode:", reply_markup=keyboard)

@app.on_callback_query(filters.regex(r"^set_mode_") & filters.user(ADMINS))
async def set_mode_callback(client: Client, query: CallbackQuery):
    new_mode = query.data.split("_")[2]
    await settings_collection.update_one({"_id": "bot_mode"}, {"$set": {"mode": new_mode}}, upsert=True)
    await query.answer(f"Mode set to {new_mode.upper()}!", show_alert=True)
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🌐 Public", callback_data="set_mode_public"),
            InlineKeyboardButton("🔒 Private", callback_data="set_mode_private")
        ]
    ])
    await query.message.edit_text(f"⚙️ Bot Settings\n\nMode Is Now **{new_mode.upper()}**.\nSelect A New Mode:", reply_markup=keyboard)

# -------------------------
#
# UPI commands (Admin only)
#
# -------------------------

@app.on_message(filters.command(["setupi", "myupi"]) & filters.private & filters.user(ADMINS))
async def upi_commands_handler(client: Client, message: Message):
    user_id = message.from_user.id
    command = message.command[0].lower()
    if command == "setupi":
        # --- FIX 1: Save state to DB ---
        await states_collection.update_one(
            {"_id": user_id},
            {"$set": {
                "state": "waiting_for_new_upi", 
                "status_msgs": [message.id], 
                "last_updated": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        await message.reply("Please Send Your UPI ID To Save Or Update It.\nExample: `yourname@upi`")
    elif command == "myupi":
        user_doc = await users_collection.find_one({"_id": user_id})
        if user_doc and user_doc.get("upi_id"):
            await message.reply(f"Your saved UPI ID is: `{user_doc['upi_id']}`\n\nTo change it, use /setupi.")
        else:
            await message.reply("You Have Not Set A UPI ID Yet. Use /setupi To Save One.")

# -------------------------
#
# File upload handlers (new batch creation)
#
# -------------------------

@app.on_message(filters.private & (filters.document | filters.video | filters.photo | filters.audio), group=1)
async def file_handler(client: Client, message: Message):
    user_id = message.from_user.id
    user_doc = await users_collection.find_one({"_id": user_id})
    bot_mode = await get_bot_mode()
    
    if user_doc and user_doc.get("banned", False):
        await message.reply("❌ You are banned.\nYou are no longer allowed to use this bot.")
        return
    if bot_mode == "private" and user_id not in ADMINS:
        await message.reply("😔 Sorry! Only Admins Are Allowed To Upload Files At The Moment.")
        return
    
    # --- FIX 1: Check for conversational state in DB ---
    if await states_collection.find_one({"_id": user_id}):
        return # User is in another conversation, ignore files
    
    # --- FIX 1: Add file to session in DB ---
    # We store message IDs, not the full pyrogram object
    await sessions_collection.update_one(
        {"_id": user_id},
        {
            "$push": {"file_msg_ids": message.id},
            "$set": {"last_updated": datetime.now(timezone.utc)}
        },
        upsert=True
    )
    
    # Removed the 0.75s debounce task for simplicity and DB-state compatibility.
    # The menu will now update instantly on every file.
    await update_batch_menu(client, user_id, message)

async def update_batch_menu(client: Client, user_id: int, last_message: Message):
    # --- FIX 1: Read session from DB ---
    session = await sessions_collection.find_one({"_id": user_id})
    if not session:
        logger.warning(f"No session found for user {user_id} in update_batch_menu")
        return

    file_count = len(session.get("file_msg_ids", []))
    text = f"✅ Batch Updated! You Have **{file_count}** Files In The Queue. What's Next?"
    buttons = [
        [InlineKeyboardButton("🔗 Get Free Link", callback_data="get_link"),
         InlineKeyboardButton("➕ Add More Files", callback_data="add_more")]
    ]
    if user_id in ADMINS:
        buttons[0].append(InlineKeyboardButton("💰 Set Price & Sell", callback_data="set_price"))
    keyboard = InlineKeyboardMarkup(buttons)
    
    old_menu_id = session.get("menu_msg_id")
    if old_menu_id:
        try:
            await client.delete_messages(user_id, old_menu_id)
        except Exception:
            pass
            
    new_menu_msg = await last_message.reply_text(text, reply_markup=keyboard, quote=True)
    
    # --- FIX 1: Save new menu ID to DB session ---
    await sessions_collection.update_one(
        {"_id": user_id},
        {"$set": {
            "menu_msg_id": new_menu_msg.id, 
            "last_updated": datetime.now(timezone.utc)
        }}
    )

@app.on_callback_query(filters.regex("^(get_link|add_more|set_price)$"))
async def batch_options_callback(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    
    # --- FIX 1: Get session from DB ---
    session = await sessions_collection.find_one({"_id": user_id})
    if not session or not session.get("file_msg_ids"):
        await query.answer("Your Session Has Expired. Please Send Files Again.", show_alert=True)
        try: await query.message.delete()
        except Exception: pass
        return
        
    if query.data == "set_price" and user_id not in ADMINS:
        await query.answer("❗️ This feature is available for Admins only.", show_alert=True)
        return
    if query.data == "add_more":
        await query.answer("✅ OK. Send Me More Files To Add To This Batch. ✅", show_alert=True)
        return

    await query.message.edit_text("__⏳ `Step 1/2`: Copying Files To Secure Storage...__")
    
    log_message_ids = []
    file_msg_ids = session.get("file_msg_ids", [])
    
    try:
        # --- FIX 1: Get messages from chat history using saved IDs ---
        messages_to_copy = await client.get_messages(user_id, file_msg_ids)
        if not isinstance(messages_to_copy, list): # Handle single message case
            messages_to_copy = [messages_to_copy]

        for msg in messages_to_copy:
            copied = await msg.copy(LOG_CHANNEL)
            log_message_ids.append(copied.id)
            
    except Exception as e:
        logger.exception("Error copying files to log channel")
        await query.message.edit_text(f"__❌ Error Copying Files: `{e}`. Please Start Again.__")
        # Clear the bad session
        await sessions_collection.delete_one({"_id": user_id})
        return

    batch_id = generate_random_string(12)
    bot_me = await app.get_me()
    share_link = f"https://t.me/{bot_me.username}?start={batch_id}"

    if query.data == "get_link":
        await query.message.edit_text("__⏳ `Step 2/2`: Generating Your Link...__")
        await files_collection.insert_one({
            "_id": batch_id,
            "message_ids": log_message_ids,
            "owner_id": user_id,
            "is_paid": False,
            "created_at": datetime.now(timezone.utc),
        })
        await query.message.edit_text(f"__✅ **Free Link Generated for {len(log_message_ids)} file(s)!**\n\n`{share_link}`__", disable_web_page_preview=True)
        # --- FIX 1: Clear session from DB ---
        await sessions_collection.delete_one({"_id": user_id})
        
    elif query.data == "set_price":
        status_msg = await query.message.edit_text("__💰 **Set A Price For File!**\n\n__Please Send The Price For This Batch In INR **(e.g., `10`)**.__")
        # --- FIX 1: Save conversational state to DB ---
        await states_collection.update_one(
            {"_id": user_id},
            {"$set": {
                "state": "waiting_for_price", 
                "log_ids": log_message_ids, 
                "batch_id": batch_id, 
                "status_msgs": [status_msg.id],
                "last_updated": datetime.now(timezone.utc)
            }},
            upsert=True
        )

# -------------------------
#
# Conversation handler for price, upi, editing flows
#
# -------------------------

@app.on_message(filters.private & filters.text & ~filters.command(["start", "help", "setupi", "myupi", "stats", "settings", "ban", "unban", "linkinfo", "editlink"]), group=1)
async def conversation_handler(client: Client, message: Message):
    user_id = message.from_user.id
    
    # --- FIX 1: Get conversational state from DB ---
    state_info = await states_collection.find_one({"_id": user_id})
    if not state_info:
        return
        
    state = state_info.get("state")
    current_status_msgs = state_info.get("status_msgs", [])
    current_status_msgs.append(message.id)

    if state == "waiting_for_price":
        try:
            price = float(message.text.strip())
            if price <= 0:
                raise ValueError
            
            user_doc = await users_collection.find_one({"_id": user_id})
            if user_doc and user_doc.get("upi_id"):
                upi_id = user_doc["upi_id"]
                status_msg = await message.reply(f"__✅ Price: `₹{price:.2f}` | UPI: `{upi_id}`\n⏳ Finalizing link...__")
                current_status_msgs.append(status_msg.id)
                # Pass price and status_msgs to creation function
                await create_paid_batch_in_db(client, message, state_info, upi_id, price, current_status_msgs)
            else:
                status_msg = await message.reply(f"__✅ Price: `₹{price:.2f}`.\n\nNow Send Your UPI ID (It Will Be Saved).__")
                current_status_msgs.append(status_msg.id)
                # --- FIX 1: Update state in DB ---
                await states_collection.update_one(
                    {"_id": user_id},
                    {"$set": {
                        "state": "waiting_for_upi",
                        "price": price, # Save price to state
                        "status_msgs": current_status_msgs,
                        "last_updated": datetime.now(timezone.utc)
                    }}
                )
        except Exception:
            status_msg = await message.reply("__**Invalid Price.** Send A Number Like `10`.__")
            current_status_msgs.append(status_msg.id)
            # --- FIX 1: Update state in DB (to save status_msg ID) ---
            await states_collection.update_one(
                {"_id": user_id},
                {"$set": {"status_msgs": current_status_msgs, "last_updated": datetime.now(timezone.utc)}}
            )

    elif state == "waiting_for_upi":
        upi_id = sanitize_upi(message.text)
        if not re.match(r"^[a-zA-Z0-9.\-_]{2,256}@[a-zA-Z]{2,64}$", upi_id):
            status_msg = await message.reply("__Invalid UPI ID format. Try again.__")
            current_status_msgs.append(status_msg.id)
            # --- FIX 1: Update state in DB ---
            await states_collection.update_one(
                {"_id": user_id},
                {"$set": {"status_msgs": current_status_msgs, "last_updated": datetime.now(timezone.utc)}}
            )
            return
            
        await users_collection.update_one({"_id": user_id}, {"$set": {"upi_id": upi_id}}, upsert=True)
        status_msg = await message.reply(f"__✅ UPI ID `{upi_id}` Saved.\n⏳ Finalizing Link...__")
        current_status_msgs.append(status_msg.id)
        
        price = state_info.get("price") # Get price from state
        await create_paid_batch_in_db(client, message, state_info, upi_id, price, current_status_msgs)

    elif state == "waiting_for_new_upi":
        upi_id = sanitize_upi(message.text)
        if not re.match(r"^[a-zA-Z0-9.\-_]{2,256}@[a-zA-Z]{2,64}$", upi_id):
            await message.reply("__Invalid UPI ID Format. Try Again.__")
            return
            
        await users_collection.update_one({"_id": user_id}, {"$set": {"upi_id": upi_id}}, upsert=True)
        await message.reply(f"__✅ Success! Your UPI ID Is Updated To: `{upi_id}`__")
        
        try:
            # Delete all status messages
            await client.delete_messages(user_id, current_status_msgs)
        except Exception:
            pass
        
        # --- FIX 1: Clear state from DB ---
        await states_collection.delete_one({"_id": user_id})

# -------------------------
#
# Function to create paid batch
#
# -------------------------

# --- FIX 1: Modified function signature ---
async def create_paid_batch_in_db(client: Client, message: Message, state_info: dict, upi_id: str, price: float, status_msgs: list):
    user_id = message.from_user.id
    try:
        batch_id = state_info["batch_id"]
        log_ids = state_info["log_ids"]
        bot_me = await app.get_me()
        share_link = f"https://t.me/{bot_me.username}?start={batch_id}"
        
        await files_collection.insert_one({
            "_id": batch_id,
            "message_ids": log_ids,
            "owner_id": user_id,
            "is_paid": True,
            "price": price, # Use price from argument
            "upi_id": upi_id,
            "payee_name": message.from_user.first_name,
            "created_at": datetime.now(timezone.utc),
        })
        
        await message.reply(f"✅ Paid Link Generated For {len(log_ids)} file(s)!\n\nPrice: **₹{price:.2f}**\n\n`{share_link}`", disable_web_page_preview=True)
        
        try:
            # Delete all status messages
            await client.delete_messages(user_id, status_msgs)
        except Exception:
            pass
            
    except Exception as e:
        logger.exception("create_paid_batch_in_db error")
        await message.reply(f"❌ An error occurred: `{e}`")
    finally:
        # --- FIX 1: Clear state and session from DB ---
        await states_collection.delete_one({"_id": user_id})
        await sessions_collection.delete_one({"_id": user_id})

# -------------------------
#
# Edit link feature
#
# -------------------------

async def get_file_details(msg_id):
    try:
        msg = await app.get_messages(LOG_CHANNEL, msg_id)
        if msg.document:
            return msg.document.file_name, f"{(msg.document.file_size / 1024 / 1024):.2f} MB"
        elif msg.video:
            return msg.video.file_name or "Video File", f"{(msg.video.file_size / 1024 / 1024):.2f} MB"
        elif msg.photo:
            return "Photo File", f"{(msg.photo.file_size / 1024 / 1024):.2f} MB"
        elif msg.audio:
            return msg.audio.file_name or "Audio File", f"{(msg.audio.file_size / 1024 / 1024):.2f} MB"
        return "Unknown File", "N/A"
    except Exception as e:
        logger.warning(f"Could not get details for msg_id {msg_id}: {e}")
        return "DELETED/UNAVAILABLE FILE", "N/A"

# --- FIX 1: Function must be async to read from DB ---
async def generate_edit_menu(batch_id: str):
    # Read session from DB
    session = await edit_sessions_collection.find_one({"_id": batch_id}) or {}
    batch_files = session.get("files", [])
    
    text = f"✍️ Editing Link: **{batch_id}**\n\n__You have {len(batch_files)} files in this batch. You can delete files or add more.__"
    buttons = []
    for i, msg_id in enumerate(batch_files):
        file_name, file_size = await get_file_details(msg_id)
        buttons.append([InlineKeyboardButton(f"❌ Delete {file_name} ({file_size})", callback_data=f"edit_delete_{batch_id}_{i}")])
    buttons.append([InlineKeyboardButton("➕ Add More Files", callback_data=f"edit_add_{batch_id}")])
    buttons.append([InlineKeyboardButton("💾 Save Changes", callback_data=f"edit_save_{batch_id}"),
                    InlineKeyboardButton("❌ Cancel", callback_data=f"edit_cancel_{batch_id}")])
    return text, InlineKeyboardMarkup(buttons)

@app.on_message(filters.command("editlink") & filters.private)
async def edit_link_command(client: Client, message: Message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        await message.reply("How to use:\n`/editlink <batch_id>`\n\n_You can only edit links that you have created._")
        return

    batch_id_input = message.command[1]
    if "http" in batch_id_input:
        try:
            parsed = urlparse(batch_id_input)
            batch_id = parse_qs(parsed.query)["start"][0]
        except Exception:
            await message.reply("__❗️ **Invalid Link format.**__\n\nPlease provide just the Batch ID, not the full URL.")
            return
    else:
        batch_id = batch_id_input

    batch_record = await files_collection.find_one({"_id": batch_id})
    if not batch_record:
        await message.reply(f"__❌ No link found with Batch ID: `{batch_id}`__")
        return
    if batch_record.get("owner_id") != user_id:
        await message.reply("__🔒 You can only edit links that you have created.__")
        return

    # --- FIX 1: Create edit session and user state in DB ---
    await edit_sessions_collection.update_one(
        {"_id": batch_id},
        {"$set": {
            "owner_id": user_id,
            "files": list(batch_record.get("message_ids", [])),
            "edit_msg_id": None, # Will be set in a moment
            "last_updated": datetime.now(timezone.utc)
        }},
        upsert=True
    )
    
    await states_collection.update_one(
        {"_id": user_id},
        {"$set": {
            "state": "editing_link", 
            "batch_id": batch_id, 
            "last_updated": datetime.now(timezone.utc)
        }},
        upsert=True
    )
    # ----------------------------------------------------

    text, keyboard = await generate_edit_menu(batch_id)
    edit_msg = await message.reply(text, reply_markup=keyboard)
    
    # --- FIX 1: Save the menu message ID to the DB session ---
    await edit_sessions_collection.update_one(
        {"_id": batch_id},
        {"$set": {"edit_msg_id": edit_msg.id}}
    )

@app.on_callback_query(filters.regex("^edit_"))
async def edit_link_callbacks(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    parts = query.data.split("_")
    action = parts[1]
    batch_id = parts[2]

    # --- FIX 1: Get edit session from DB ---
    session = await edit_sessions_collection.find_one({"_id": batch_id})
    if not session or session.get("owner_id") != user_id:
        await query.answer("This edit session is invalid or has expired.", show_alert=True)
        return

    if action == "delete":
        try:
            idx = int(parts[3])
            current_files = session.get("files", [])
            del current_files[idx]
            # --- FIX 1: Update file list in DB ---
            await edit_sessions_collection.update_one(
                {"_id": batch_id}, 
                {"$set": {"files": current_files, "last_updated": datetime.now(timezone.utc)}}
            )
            await query.answer("✅ File removed.")
            
            text, keyboard = await generate_edit_menu(batch_id)
            await query.message.edit_text(text, reply_markup=keyboard)
        except (ValueError, IndexError):
            await query.answer("Could not delete this file. It may have already been removed.", show_alert=True)
            
    elif action == "add":
        await query.answer("✅ OK. Send me more files to add to this batch. When you are done, click 'Save Changes'.", show_alert=True)
        # --- FIX 1: Set user state in DB ---
        await states_collection.update_one(
            {"_id": user_id}, 
            {"$set": {"state": "editing_adding_files", "batch_id": batch_id, "last_updated": datetime.now(timezone.utc)}}, 
            upsert=True
        )
        
    elif action == "cancel":
        # --- FIX 1: Delete session and state from DB ---
        await edit_sessions_collection.delete_one({"_id": batch_id})
        await states_collection.delete_one({"_id": user_id})
        await query.message.edit_text("__❌ Edit cancelled.__")
        
    elif action == "save":
        new_file_list = session.get("files", [])
        if not new_file_list:
            await query.answer("❗️ You cannot save an empty link. Add at least one file.", show_alert=True)
            return
            
        await files_collection.update_one({"_id": batch_id}, {"$set": {"message_ids": new_file_list}})
        
        # --- FIX 1: Delete session and state from DB ---
        await edit_sessions_collection.delete_one({"_id": batch_id})
        await states_collection.delete_one({"_id": user_id})
        
        await query.message.edit_text(f"__✅ **Link `{batch_id}` updated successfully!** It now contains **{len(new_file_list)}** files.__")

@app.on_message(filters.private & (filters.document | filters.video | filters.photo | filters.audio), group=2)
async def file_handler_for_editing(client: Client, message: Message):
    user_id = message.from_user.id
    
    # --- FIX 1: Get user state from DB ---
    user_state = await states_collection.find_one({"_id": user_id})
    state = user_state.get("state") if user_state else None
    
    if state == "editing_adding_files":
        batch_id = user_state["batch_id"]
        
        # --- FIX 1: Get edit session from DB ---
        edit_session = await edit_sessions_collection.find_one({"_id": batch_id})
        if edit_session:
            try:
                copied = await message.copy(LOG_CHANNEL)
                # --- FIX 1: Add new file ID to DB ---
                await edit_sessions_collection.update_one(
                    {"_id": batch_id},
                    {"$push": {"files": copied.id}, "$set": {"last_updated": datetime.now(timezone.utc)}}
                )
                
                edit_msg_id = edit_session["edit_msg_id"]
                text, keyboard = await generate_edit_menu(batch_id)
                await client.edit_message_text(user_id, edit_msg_id, text, reply_markup=keyboard)
                await message.reply_text("✅ File added to the edit session.", quote=True)
            except Exception as e:
                logger.exception("file_handler_for_editing error")
                await message.reply_text(f"❌ Could not add file: {e}")

# -------------------------
#
# Payment & delivery flows (completed and hardened)
#
# -------------------------

async def process_link_click(client: Client, user_id: int, batch_id: str):
    batch_record = await files_collection.find_one({"_id": batch_id})
    if not batch_record:
        await client.send_message(user_id, "🤔 Link Expired or Invalid")
        return

    if not batch_record.get("is_paid", False):
        await send_files_from_batch(client, user_id, batch_record, FREE_DELETE_DELAY_MINUTES, "Minutes")
        return

    # --- FIX 2: Payment Race Condition Fix ---
    base_price = float(batch_record.get("price", 0))
    unique_amount_str = None
    payment_id = generate_random_string(16)

    # Try to insert a unique payment record, handling race conditions
    for i in range(1, 100): # Try up to 99 variations (e.g., .01 to .99)
        temp_amount = f"{base_price + (i / 100.0):.2f}"
        try:
            # Try to insert. This will fail if 'unique_amount' is already taken
            # due to the unique index we created.
            await payments_collection.insert_one({
                "_id": payment_id,
                "batch_id": batch_id,
                "buyer_id": user_id,
                "unique_amount": temp_amount,
                "created_at": datetime.now(timezone.utc)
            })
            unique_amount_str = temp_amount # Success!
            break # Exit the loop
        except DuplicateKeyError:
            continue # This amount was taken (race condition), try the next one
        except Exception as e:
            logger.error(f"Error inserting payment record: {e}")
            break # A different error occurred
    
    if not unique_amount_str:
        # This happens if all 99 slots are full or an error occurred
        await client.send_message(user_id, "__🚦 Sorry, the server is very busy. Please try again in a minute.__")
        return
    # --- End of Fix 2 ---

    run_time = datetime.now(IST) + timedelta(minutes=PAYMENT_EXPIRATION_MINUTES)
    try:
        scheduler.add_job(expire_payment_job, "date", run_date=run_time, args=[payment_id, user_id, batch_id], id=payment_id, replace_existing=True)
    except Exception as e:
        logger.warning(f"Could not schedule expire_payment_job: {e}")

    bot_username = (await client.get_me()).username
    payee_name = quote_plus(batch_record.get("payee_name", "Seller"))
    upi_id = batch_record.get("upi_id", "")
    payment_url = f"{PAYMENT_PAGE_URL}?amount={unique_amount_str}&upi={upi_id}&name={payee_name}&bot={bot_username}"
    pay_btn = InlineKeyboardButton("💰 Pay Now", url=payment_url)
    paid_btn = InlineKeyboardButton("✅ I Have Paid", callback_data=f"i_paid_{payment_id}")

    await client.send_message(
        user_id,
        f"**__🔒 This Is A Premium File Batch.__**\n\n"
        f"**__IMPORTANT:__** __Pay the **EXACT AMOUNT** shown below. This session will expire in **{PAYMENT_EXPIRATION_MINUTES} minutes**.__\n\n"
        f"__💰 **Amount To Pay:** `₹{unique_amount_str}`__\n\n"
        "__Click **'Pay Now'**, then click **'I Have Paid'** to request access.__",
        reply_markup=InlineKeyboardMarkup([[pay_btn], [paid_btn]])
    )

@app.on_callback_query(filters.regex(r"^i_paid_"))
async def i_have_paid_callback(client: Client, query: CallbackQuery):
    try:
        _, _, payment_id = query.data.split("_", 2)
    except Exception:
        await query.answer("Invalid request.", show_alert=True)
        return

    try:
        scheduler.remove_job(payment_id)
        logger.info(f"Expired job for payment {payment_id} removed on 'I Have Paid' click.")
    except JobLookupError:
        pass
    except Exception as e:
        logger.warning(f"Error removing expiration job: {e}")

    payment_record = await payments_collection.find_one({"_id": payment_id})
    if not payment_record:
        await query.answer("__This payment session has expired. Please generate a new payment link.__", show_alert=True)
        return

    batch_id = payment_record["batch_id"]
    batch_record = await files_collection.find_one({"_id": batch_id})
    if not batch_record:
        await query.answer("__The File Batch Linked To This Payment Is No Longer Available.__", show_alert=True)
        return

    owner_id = batch_record["owner_id"]
    run_time = datetime.now(IST) + timedelta(hours=APPROVAL_EXPIRATION_HOURS)
    try:
        scheduler.add_job(expire_approval_job, "date", run_date=run_time, args=[payment_id, query.from_user.id, owner_id], id=f"approve_{payment_id}", replace_existing=True)
    except Exception as e:
        logger.warning(f"Could not schedule approve expiration job: {e}")

    await query.answer("__✅ Request Sent To The Seller. You Will Get The Files After Approval.__", show_alert=True)
    await query.message.edit_reply_markup(None)

    approve_btn = InlineKeyboardButton("✅ Approve", callback_data=f"approve_{payment_id}")
    decline_btn = InlineKeyboardButton("❌ Decline", callback_data=f"decline_{payment_id}")

    try:
        await client.send_message(
            owner_id,
            f"__🔔 **Payment Request**\n\n**User:** {query.from_user.mention} (`{query.from_user.id}`)\n**Batch ID:** `{batch_id}`\n\nThey Claim To Have Paid The Unique Amount Of **`₹{payment_record['unique_amount']}`**.\n\nPlease check your account for this **exact amount** and click **Approve**.__",
            reply_markup=InlineKeyboardMarkup([[approve_btn], [decline_btn]])
        )
    except Exception as e:
        logger.warning(f"Could not notify owner {owner_id}: {e}")

async def process_payment_approval(payment_id: str, approved_by: str = "Seller (Manual)"):
    try:
        try:
            scheduler.remove_job(f"approve_{payment_id}")
        except JobLookupError:
            pass
        except Exception:
            pass

        payment_record = await payments_collection.find_one({"_id": payment_id})
        if not payment_record:
            logger.warning(f"Approval failed: Payment record {payment_id} not found.")
            return "__This Payment Request Has Expired Or Is Invalid.__"

        batch_id = payment_record["batch_id"]
        buyer_id = payment_record["buyer_id"]
        unique_amount = payment_record["unique_amount"]

        batch_record = await files_collection.find_one({"_id": batch_id})
        if not batch_record:
            await payments_collection.delete_one({"_id": payment_id})
            logger.error(f"Critical Error: Batch {batch_id} not found for payment {payment_id}. Deleting orphan payment.")
            return f"__Error: The file batch `{batch_id}` no longer exists. Payment record deleted.__"

        owner_id = batch_record["owner_id"]
        delivered = await send_files_from_batch(app, buyer_id, batch_record, PAID_DELETE_DELAY_HOURS, "Hours")

        try:
            if delivered and approved_by.startswith("Automation"):
                await app.send_message(buyer_id, f"__✅ Your payment of `₹{unique_amount}` has been **automatically approved**! You are receiving the files.__")
        except Exception:
            pass

        final_message_for_button = ""
        try:
            if delivered:
                success_message = (
                    f"__**✅ Files Delivered Successfully!**\n\n**Approved By:** {approved_by}\n**Buyer:** `{buyer_id}`\n**Batch ID:** `{batch_id}`\n**Amount:** `₹{unique_amount}`__"
                )
                await app.send_message(owner_id, success_message)
                final_message_for_button = f"__✅ Payment Of `₹{unique_amount}` Approved For User `{buyer_id}`. Files have been sent.__"
            else:
                fail_message = f"__❌ **Delivery Failed!**\n\nThe user `{buyer_id}` might have blocked the bot.__"
                await app.send_message(owner_id, fail_message)
                final_message_for_button = fail_message
        except Exception as e:
            logger.warning(f"Could not send notification to owner {owner_id}: {e}")
            final_message_for_button = "__An error occurred while notifying the owner.__"

        await payments_collection.delete_one({"_id": payment_id})
        return final_message_for_button
    except Exception as e:
        logger.exception("process_payment_approval error")
        return "__An error occurred processing approval.__"

@app.on_callback_query(filters.regex(r"^(approve|decline)"))
async def payment_verification_callback(client: Client, query: CallbackQuery):
    try:
        action, payment_id = query.data.split("_", 1)
    except Exception:
        return

    owner_id = query.from_user.id
    payment_record = await payments_collection.find_one({"_id": payment_id})
    if not payment_record:
        await query.answer("__This Payment Request Has Expired Or Is Invalid.__", show_alert=True)
        return

    batch_record = await files_collection.find_one({"_id": payment_record["batch_id"]})
    if not batch_record or batch_record["owner_id"] != owner_id:
        await query.answer("__This Is Not For You.__", show_alert=True)
        return

    if action == "approve":
        result_message = await process_payment_approval(payment_id, approved_by="Seller (Manual)")
        try:
            await query.message.edit_text(result_message)
        except MessageNotModified:
            pass
    elif action == "decline":
        try:
            scheduler.remove_job(f"approve_{payment_id}")
        except JobLookupError:
            pass
        buyer_id = payment_record["buyer_id"]
        unique_amount = payment_record["unique_amount"]
        await query.message.edit_text(f"__❌ Payment Of `₹{unique_amount}` Declined For User `{buyer_id}`.__")
        try:
            await client.send_message(buyer_id, "__😔 **Payment Declined**\nThe Seller Could Not Verify Your Payment.__")
        except Exception:
            pass
        await payments_collection.delete_one({"_id": payment_id})

# -------------------------
#
# Delivery utility
#
# -------------------------

async def send_files_from_batch(client: Client, user_id: int, batch_record: dict, delay_amount: int, delay_unit: str):
    await client.send_message(user_id, f"✅ Access Granted! You are receiving {len(batch_record['message_ids'])} file(s).")
    all_sent_successfully = True
    sent_message_ids = []
    for msg_id in batch_record['message_ids']:
        try:
            sent_file_msg = await client.copy_message(chat_id=user_id, from_chat_id=LOG_CHANNEL, message_id=msg_id)
            sent_message_ids.append(sent_file_msg.id)
        except (UserIsBlocked, InputUserDeactivated):
            all_sent_successfully = False
            logger.warning(f"Failed to send file to {user_id}: User has blocked the bot or account deactivated.")
            break
        except Exception as e:
            all_sent_successfully = False
            logger.exception(f"Error sending file {msg_id} to {user_id}: {e}")
            try:
                await client.send_message(user_id, "__❌ Could Not Send One Of The Files. It Might Have Been Deleted From The Source.__")
            except Exception:
                pass
    
    if sent_message_ids:
        warning_text = f"\n\n⚠️ **IMPORTANT!**\nThese files will be automatically deleted in **{delay_amount} {delay_unit}**. Please forward them to your saved messages immediately."
        try:
            await client.send_message(user_id, warning_text, reply_to_message_id=sent_message_ids[-1])
        except Exception as e:
            logger.warning(f"Could not send deletion warning message to {user_id}: {e}")

        run_time = datetime.now(IST) + (timedelta(minutes=delay_amount) if delay_unit == "Minutes" else timedelta(hours=delay_amount))
        try:
            scheduler.add_job(delete_message_job, "date", run_date=run_time, args=[user_id, sent_message_ids], misfire_grace_time=300)
        except Exception as e:
            logger.warning(f"Could not schedule delete job for user {user_id}: {e}")

    return all_sent_successfully

# -------------------------
#
# Automation helper (webhook auto-approve by unique amount)
#
# -------------------------

async def _auto_approve_by_amount(unique_amount: str):
    payment_record = await payments_collection.find_one({"unique_amount": unique_amount})
    if not payment_record:
        logger.info(f"Automation: No pending payment with amount {unique_amount}.")
        return {"status": "info", "message": "No pending payment with that amount."}
    
    batch_record = await files_collection.find_one({"_id": payment_record["batch_id"]})
    if not batch_record:
        await payments_collection.delete_one({"_id": payment_record["_id"]})
        logger.warning(f"Automation: Batch {payment_record['batch_id']} not found; payment removed.")
        return {"status": "error", "message": "Batch not found; payment removed."}

    owner_doc = await users_collection.find_one({"_id": batch_record["owner_id"]})
    if not owner_doc:
        logger.warning(f"Automation: Owner {batch_record['owner_id']} not in DB.")
        return {"status": "info", "message": "Owner not found in DB; cannot auto-approve."}
        
    if batch_record.get("owner_id") not in ADMINS:
        logger.info(f"Automation: Payment for normal user {batch_record.get('owner_id')}; manual approval required.")
        return {"status": "info", "message": "Payment is for normal user; manual approval required."}

    payment_id = payment_record["_id"]
    logger.info(f"Automation: Auto-approving payment {payment_id} for amount {unique_amount}.")
    result = await process_payment_approval(payment_id, approved_by="Automation 🤖")
    return {"status": "success", "message": result}

# -------------------------
#
# Startup & Shutdown
#
# -------------------------

def start_background_services():
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    try:
        scheduler.start()
        logger.info("Scheduler started")
    except Exception as e:
        logger.exception("Failed to start scheduler")

async def main():
    # --- FIX 1 & 2: Set up database indexes before starting ---
    await setup_database_indexes()
    
    start_background_services()
    logger.info("Starting bot client...")
    await app.start()
    logger.info("Bot client started.")
    await idle()
    logger.info("Stopping bot...")
    await app.stop()
    scheduler.shutdown()
    logger.info("Bot stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot shutdown requested.")
