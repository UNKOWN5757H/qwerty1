#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import random
import string
import asyncio
import re
import signal
from datetime import datetime, timedelta, timezone
from threading import Thread
from urllib.parse import urlparse, parse_qs, quote_plus

import pytz
from dotenv import load_dotenv
from flask import Flask, request, jsonify

from pyrogram import Client, filters
from pyrogram.errors import UserNotParticipant, UserIsBlocked, InputUserDeactivated, MessageNotModified, RPCError
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery

from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.base import JobLookupError

# -------------------------
# Logging & config
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger("apscheduler").setLevel(logging.WARNING)

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")
LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", "0") or 0)
UPDATE_CHANNEL = os.environ.get("UPDATE_CHANNEL", "")  # public channel username (without @), leave empty to disable
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "")
ADMINS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip().isdigit()]
PAYMENT_PAGE_URL = os.environ.get("PAYMENT_PAGE_URL", "https://t.me/Sandalwood_Man")
AUTOMATION_SECRET = os.environ.get("AUTOMATION_SECRET", None)
PORT = int(os.environ.get("PORT", 8080))

# timing configs
FREE_DELETE_DELAY_MINUTES = int(os.environ.get("FREE_DELETE_DELAY_MINUTES", 60))
PAID_DELETE_DELAY_HOURS = int(os.environ.get("PAID_DELETE_DELAY_HOURS", 24))
PAYMENT_EXPIRATION_MINUTES = int(os.environ.get("PAYMENT_EXPIRATION_MINUTES", 60))
APPROVAL_EXPIRATION_HOURS = int(os.environ.get("APPROVAL_EXPIRATION_HOURS", 24))
IST = pytz.timezone("Asia/Kolkata")

# -------------------------
# Flask web app for webhook automation & health checks
# -------------------------
flask_app = Flask(__name__)

@flask_app.route("/", methods=["GET", "HEAD"])
def index_route():
    # simple health check endpoint
    return "FileLinkBot is alive!", 200

@flask_app.route("/api/shortcut", methods=["POST"])
def shortcut_webhook():
    """Automation webhook endpoint used by external services (protected by header secret)."""
    provided_secret = request.headers.get("X-Shortcut-Secret")
    if AUTOMATION_SECRET and provided_secret != AUTOMATION_SECRET:
        return jsonify({"status":"error","message":"Unauthorized"}), 403

    sms_text = request.get_data(as_text=True) or ""
    if not sms_text:
        return jsonify({"status":"error","message":"Bad Request: SMS text is missing"}), 400

    # parse amount like Rs. 1,234.00 or ₹1234.00 or INR 1234.00
    amount_match = re.search(r'(?:Rs\.?|₹|INR)\s*([\d,]+\.\d{2})', sms_text)
    if not amount_match:
        return jsonify({"status":"info","message":"No valid amount found in SMS"}), 200

    unique_amount = amount_match.group(1).replace(",", "")
    payment_record = payments_collection.find_one({"unique_amount": unique_amount})
    if not payment_record:
        return jsonify({"status":"info","message":"No pending user for this amount."}), 200

    batch_record = files_collection.find_one({"_id": payment_record.get("batch_id")})
    if batch_record and batch_record.get("owner_id") in ADMINS:
        payment_id = payment_record["_id"]
        # schedule process_payment_approval to run in bot loop
        future = asyncio.run_coroutine_threadsafe(process_payment_approval(payment_id, approved_by="Automation 🤖"), app.loop)
        try:
            future.result(timeout=20)
            return jsonify({"status":"success","message":f"Admin payment {unique_amount} approved."}), 200
        except Exception as e:
            logging.error("Automation approval task error: %s", e)
            return jsonify({"status":"error","message":f"Async task failed: {e}"}), 500
    else:
        return jsonify({"status":"info","message":"Payment is for a normal user, manual approval required."}), 200

def run_flask():
    # note: for production use a WSGI server instead of Flask built-in server
    flask_app.run(host="0.0.0.0", port=PORT)

# -------------------------
# MongoDB & APScheduler setup
# -------------------------
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["file_link_bot"]
    files_collection = db["file_batches"]
    users_collection = db["users"]
    settings_collection = db["settings"]
    payments_collection = db["pending_payments"]
    logging.info("Connected to MongoDB.")
except Exception as e:
    logging.exception("Failed to connect to MongoDB: %s", e)
    raise SystemExit("MongoDB connection required.")

jobstores = {"default": MongoDBJobStore(database="file_link_bot", collection="scheduler_jobs", client=mongo_client)}
scheduler = BackgroundScheduler(jobstores=jobstores, timezone="Asia/Kolkata")

# -------------------------
# Pyrogram bot client
# -------------------------
app = Client("filelinkbot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# in-memory session/state holders (non-persistent)
user_sessions = {}   # {user_id: {'files': [Message,...], 'menu_msg_id': int, 'job': asyncio.Task|None}}
user_states = {}     # {user_id: {...}} for multi-step flows
EDIT_SESSIONS = {}   # {batch_id: {'owner_id': id, 'files': [log_msg_ids], 'edit_msg_id': int}}

# -------------------------
# Utility helpers
# -------------------------
def generate_random_string(length=10):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))

async def is_user_member(client: Client, user_id: int) -> bool:
    """Check if user is member of UPDATE_CHANNEL (public username). If UPDATE_CHANNEL is empty, return True."""
    if not UPDATE_CHANNEL:
        return True
    try:
        chat_id = UPDATE_CHANNEL if UPDATE_CHANNEL.startswith("@") else f"@{UPDATE_CHANNEL}"
        member = await client.get_chat_member(chat_id=chat_id, user_id=user_id)
        status = getattr(member, "status", None)
        if status in ("left", "kicked", None):
            return False
        return True
    except UserNotParticipant:
        return False
    except RPCError as e:
        logging.warning("is_user_member RPCError for user %s: %s", user_id, e)
        return False
    except Exception as e:
        logging.warning("is_user_member unexpected error: %s", e)
        return False

async def add_user_to_db(message: Message):
    """Add or update user basic info, avoid failing if fields missing."""
    u = getattr(message, "from_user", None)
    if not u:
        return
    doc = {
        "first_name": getattr(u, "first_name", None),
        "last_name": getattr(u, "last_name", None),
        "username": getattr(u, "username", None),
    }
    try:
        users_collection.update_one({"_id": u.id}, {"$set": doc, "$setOnInsert": {"banned": False, "joined_date": datetime.now(timezone.utc)}}, upsert=True)
    except Exception as e:
        logging.warning("Failed to upsert user: %s", e)

async def get_bot_mode() -> str:
    setting = settings_collection.find_one({"_id": "bot_mode"})
    return setting.get("mode", "public") if setting else "public"

def get_start_text():
    return "__**Hey! I am PermaStore Bot 🤖**\n\nSend Me Any File! And I'll Give You A **Permanent** Shareable Link! Which **Never Expires.**__"

def get_start_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❓ How to Use / Help", callback_data="show_help")]])

def get_help_text_and_keyboard(user_id: int):
    """Return help text and keyboard; admin sees additional commands."""
    help_text = (
        "__**Here's how to use me:**__\n\n"
        "__1. **Send Files:** Send me any file, or forward multiple files at once.__\n\n"
        "__2. **Use the Menu:** After you send a file, a menu will appear:__\n\n"
        "   __- **🔗 Get Free Link:** Creates a permanent link for all files in your batch.__\n\n"
        "   __- **➕ Add More Files:** Allows you to send more files to the current batch.__\n\n"
        "__**Available Commands:**__\n"
        "__/start__ `- Restart the bot and clear any session.`\n"
        "__/editlink <batch_id>__ `- Edit an existing link you created.`\n"
        "__/help__ `- Show this help message.`\n\n"
    )
    if user_id in ADMINS:
        help_text = help_text + (
            "__Admin Commands:__\n"
            "__/setupi__ - Save or update your UPI ID.\n"
            "__/myupi__ - Show saved UPI ID.\n"
            "__/stats__ - Bot stats.\n"
            "__/settings__ - Change bot mode.\n"
            "__/ban <user_id>__, /unban <user_id>__\n"
            "__/linkinfo <batch_id>__ - Get details of a link.\n\n"
        )
    return help_text, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start")]])

# -------------------------
# Scheduler-backed asynchronous actions
# -------------------------
def delete_message_job(chat_id: int, message_ids: list):
    """Schedule wrapper used by APScheduler: run in event loop to delete messages."""
    async def _task():
        try:
            await app.delete_messages(chat_id=chat_id, message_ids=message_ids)
        except Exception as e:
            logging.warning("Failed to delete messages %s in %s: %s", message_ids, chat_id, e)
    if app.is_connected:
        asyncio.run_coroutine_threadsafe(_task(), app.loop)

def expire_payment_job(payment_id: str, user_id: int, batch_id: str):
    async def _task():
        if payments_collection.find_one_and_delete({"_id": payment_id}):
            logging.info("Payment session %s expired for user %s.", payment_id, user_id)
            try:
                me = await app.get_me()
                share_link = f"https://t.me/{me.username}?start={batch_id}"
                await app.send_message(user_id, f"__⏳ Your payment session has expired. Please generate a new payment link: {share_link}__")
            except Exception:
                pass
    if app.is_connected:
        asyncio.run_coroutine_threadsafe(_task(), app.loop)

def expire_approval_job(payment_id: str, user_id: int, owner_id: int):
    async def _task():
        if payments_collection.find_one_and_delete({"_id": payment_id}):
            logging.info("Approval request %s expired.", payment_id)
            try:
                await app.send_message(user_id, f"__😔 The seller did not respond to your payment confirmation. Request cancelled.__")
                await app.send_message(owner_id, f"__⚠️ The approval request {payment_id} expired.__")
            except Exception:
                pass
    if app.is_connected:
        asyncio.run_coroutine_threadsafe(_task(), app.loop)

# -------------------------
# File info fetcher (from LOG_CHANNEL)
# -------------------------
async def get_file_details(msg_id: int):
    try:
        msg = await app.get_messages(LOG_CHANNEL, msg_id)
        if msg is None:
            return "DELETED/UNAVAILABLE FILE", "N/A"
        if getattr(msg, "document", None):
            return msg.document.file_name or "Document", f"{(msg.document.file_size or 0) / 1024 / 1024:.2f} MB"
        elif getattr(msg, "video", None):
            return msg.video.file_name or "Video File", f"{(msg.video.file_size or 0) / 1024 / 1024:.2f} MB"
        elif getattr(msg, "photo", None):
            return "Photo File", f"{(msg.photo.file_size or 0) / 1024 / 1024:.2f} MB"
        elif getattr(msg, "audio", None):
            return msg.audio.file_name or "Audio File", f"{(msg.audio.file_size or 0) / 1024 / 1024:.2f} MB"
        return "Unknown File", "N/A"
    except Exception as e:
        logging.warning("Could not get details for msg_id %s: %s", msg_id, e)
        return "DELETED/UNAVAILABLE FILE", "N/A"

async def generate_edit_menu(batch_id: str):
    batch_files = EDIT_SESSIONS.get(batch_id, {}).get("files", [])
    text = f"__✍️ **Editing Link:** `{batch_id}`__\n\n__You have **{len(batch_files)}** files in this batch. You can delete files or add more.__"
    buttons = []
    for idx, log_msg_id in enumerate(batch_files):
        fname, fsize = await get_file_details(log_msg_id)
        buttons.append([InlineKeyboardButton(f"❌ {fname} ({fsize})", callback_data=f"edit_delete_{batch_id}_{idx}")])
    buttons.append([InlineKeyboardButton("➕ Add More Files", callback_data=f"edit_add_{batch_id}"),
                    InlineKeyboardButton("✅ Save Changes", callback_data=f"edit_save_{batch_id}")])
    buttons.append([InlineKeyboardButton("❌ Cancel Edit", callback_data=f"edit_cancel_{batch_id}")])
    return text, InlineKeyboardMarkup(buttons)

# -------------------------
# Message handlers
# -------------------------
@app.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    await add_user_to_db(message)
    user_id = getattr(message.from_user, "id", None)
    if not user_id:
        return

    # remove any old menu message for this user
    if user_sessions.get(user_id) and user_sessions[user_id].get("menu_msg_id"):
        try:
            await client.delete_messages(user_id, user_sessions[user_id]["menu_msg_id"])
        except Exception:
            pass
    user_sessions.pop(user_id, None)
    user_states.pop(user_id, None)

    # start param handling: /start <batch_id>
    if len(message.command) > 1:
        batch_id = message.command[1]

        # if UPDATE_CHANNEL is set, require join
        if not await is_user_member(client, user_id):
            join_url = f"https://t.me/{UPDATE_CHANNEL[1:]}" if UPDATE_CHANNEL.startswith("@") else f"https://t.me/{UPDATE_CHANNEL}"
            join_btn = InlineKeyboardButton("🔗 Join Channel", url=join_url)
            try:
                await message.reply("__👋 Hello! Please join our update channel to access content. Once you join, I will send your files automatically.__",
                                    reply_markup=InlineKeyboardMarkup([[join_btn]]))
            except Exception:
                try:
                    await client.send_message(user_id, "__👋 Hello! Please join our update channel to access content. Once you join, I will send your files automatically.__")
                except Exception:
                    pass

            # background auto-check task: check periodically for membership and send files immediately once they join
            async def auto_check_and_send():
                checks = 12  # number of checks
                interval = 5  # seconds between checks -> total wait = checks * interval (here 60s)
                try:
                    for _ in range(checks):
                        await asyncio.sleep(interval)
                        if await is_user_member(client, user_id):
                            try:
                                await client.send_message(user_id, "__✅ Thank you for joining! Preparing your files...__")
                            except Exception:
                                pass
                            try:
                                await process_link_click(client, user_id, batch_id)
                            except Exception as e:
                                logging.exception("auto_check_and_send: failed to process_link_click: %s", e)
                            return
                    # after checks expired; remind user to join
                    try:
                        await client.send_message(user_id, "__⏳ You didn't join the channel yet. Please join to access files, then send /start again or open the link.__")
                    except Exception:
                        pass
                except asyncio.CancelledError:
                    logging.debug("auto_check_and_send cancelled for user %s", user_id)
                except Exception as e:
                    logging.exception("auto_check_and_send unexpected error: %s", e)

            try:
                asyncio.create_task(auto_check_and_send())
            except Exception:
                try:
                    app.loop.create_task(auto_check_and_send())
                except Exception as e:
                    logging.warning("Could not create auto_check task: %s", e)
            return

        # user is already a member — send files directly
        await process_link_click(client, user_id, batch_id)
    else:
        await message.reply(get_start_text(), reply_markup=get_start_keyboard())

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
        else:
            await query.message.edit_text(get_start_text(), reply_markup=get_start_keyboard())
    except MessageNotModified:
        await query.answer()
    except Exception as e:
        logging.error("Navigation callback error: %s", e)
        await query.answer("__An error occurred.__", show_alert=True)

@app.on_callback_query(filters.regex("^close_msg$"))
async def close_message_callback(client: Client, query: CallbackQuery):
    try:
        await query.message.delete()
        user_sessions.pop(query.from_user.id, None)
    except Exception as e:
        logging.warning("Failed to delete message on close: %s", e)
        await query.answer("__Could not delete message.__", show_alert=True)

@app.on_message(filters.command("stats") & filters.private & filters.user(ADMINS))
async def stats_handler(client: Client, message: Message):
    total_users = users_collection.count_documents({})
    banned_users = users_collection.count_documents({"banned": True})
    total_batches = files_collection.count_documents({})
    paid_batches = files_collection.count_documents({"is_paid": True})
    await message.reply(
        f"__📊 **Bot Statistics**\n\n👤 **Users:**\n   - Total Users: `{total_users}`\n   - Banned Users: `{banned_users}`\n\n🔗 **Links (Batches):**\n   - Total Batches: `{total_batches}`\n   - Paid Batches: `{paid_batches}`\n   - Free Batches: `{total_batches - paid_batches}`__"
    )

@app.on_message(filters.command("ban") & filters.private & filters.user(ADMINS))
async def ban_handler(client: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("__Usage: `/ban <user_id>`__")
        return
    try:
        target = int(message.command[1])
        if target in ADMINS:
            await message.reply("__❌ You cannot ban an admin.__")
            return
        users_collection.update_one({"_id": target}, {"$set": {"banned": True}}, upsert=True)
        await message.reply(f"__✅ User `{target}` has been banned.__")
    except ValueError:
        await message.reply("__Invalid User ID provided.__")

@app.on_message(filters.command("unban") & filters.private & filters.user(ADMINS))
async def unban_handler(client: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("__Usage: `/unban <user_id>`__")
        return
    try:
        target = int(message.command[1])
        users_collection.update_one({"_id": target}, {"$set": {"banned": False}}, upsert=True)
        await message.reply(f"__✅ User `{target}` has been unbanned.__")
    except ValueError:
        await message.reply("__Invalid User ID provided.__")

@app.on_message(filters.command("linkinfo") & filters.private & filters.user(ADMINS))
async def linkinfo_handler(client: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("__Usage: `/linkinfo <batch_id>`__")
        return
    batch_id = message.command[1]
    batch = files_collection.find_one({"_id": batch_id})
    if not batch:
        await message.reply(f"__❌ No link found with Batch ID: `{batch_id}`__")
        return
    owner_id = batch.get("owner_id")
    owner_info = users_collection.find_one({"_id": owner_id}) or {}
    owner_details = f"__{owner_info.get('first_name','')} (@{owner_info.get('username','N/A')})__" if owner_info else "__Unknown (Not in DB)__"
    link_type = "Paid 💰" if batch.get("is_paid") else "Free 🆓"
    file_count = len(batch.get("message_ids", []))
    text = (
        f"__**🔗 Link Information**\n\n- **Batch ID:** `{batch_id}`\n"
        f"- **Link Type:** {link_type}\n- **File Count:** `{file_count}`\n\n"
        f"👤 **Owner Details:**\n- **User ID:** `{owner_id}`\n- **Name/Username:** {owner_details}__\n"
    )
    if batch.get("is_paid"):
        text += f"__- **Price:** `₹{batch.get('price', 0):.2f}`\n- **UPI ID:** `{batch.get('upi_id', 'N/A')}`__"
    await message.reply(text)

@app.on_message(filters.command("settings") & filters.private & filters.user(ADMINS))
async def settings_handler(client: Client, message: Message):
    current_mode = await get_bot_mode()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌍 Public (Anyone Can Upload)", callback_data="set_mode_public")],
        [InlineKeyboardButton("🔒 Private (Admins Only)", callback_data="set_mode_private")]
    ])
    await message.reply(f"__⚙️ **Bot Settings**\n\nCurrent Mode: **{current_mode.upper()}**.\nSelect A New Mode:__", reply_markup=keyboard)

@app.on_callback_query(filters.regex(r"^set_mode_") & filters.user(ADMINS))
async def set_mode_callback(client: Client, query: CallbackQuery):
    new_mode = query.data.split("_", 2)[2]
    settings_collection.update_one({"_id":"bot_mode"}, {"$set":{"mode": new_mode}}, upsert=True)
    await query.answer(f"__Mode set to {new_mode.upper()}!__", show_alert=True)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌍 Public (Anyone Can Upload)", callback_data="set_mode_public")],
        [InlineKeyboardButton("🔒 Private (Admins Only)", callback_data="set_mode_private")]
    ])
    await query.message.edit_text(f"__⚙️ **Bot Settings**\n\nMode Is Now **{new_mode.upper()}**.\nSelect A New Mode:__", reply_markup=keyboard)

# UPI admin-only commands
@app.on_message(filters.command(["setupi","myupi"]) & filters.private & filters.user(ADMINS))
async def upi_commands_handler(client: Client, message: Message):
    user_id = message.from_user.id
    cmd = message.command[0].lower()
    if cmd == "setupi":
        user_states[user_id] = {"state": "waiting_for_new_upi", "status_msgs": [message.id]}
        await message.reply("__Please Send Your UPI ID To Save Or Update It. Example: `yourname@upi`__")
    else:  # myupi
        user_doc = users_collection.find_one({"_id": user_id}) or {}
        if user_doc and user_doc.get("upi_id"):
            await message.reply(f"__Your saved UPI ID is: `{user_doc['upi_id']}`\n\nTo change it, use /setupi.__")
        else:
            await message.reply("__You Have Not Set A UPI ID Yet. Use /setupi To Save One.__")

# -------------------------
# Upload & batch creation flows
# -------------------------
@app.on_message(filters.private & (filters.document | filters.video | filters.photo | filters.audio), group=1)
async def file_handler(client: Client, message: Message):
    """
    Accept files from users, accumulate them in a session, show a menu to create free/paid links.
    """
    user_id = message.from_user.id
    await add_user_to_db(message)

    user_doc = users_collection.find_one({"_id": user_id}) or {}
    if user_doc.get("banned", False):
        await message.reply("__❌ You are banned.__")
        return

    bot_mode = await get_bot_mode()
    if bot_mode == "private" and user_id not in ADMINS:
        await message.reply("__😔 Sorry! Only Admins Are Allowed To Upload Files At The Moment.__")
        return

    if user_id in user_states:
        # user is in a different flow (e.g. pricing, editing), don't accept new batch
        return

    # create session
    sess = user_sessions.setdefault(user_id, {"files": [], "menu_msg_id": None, "job": None})
    sess["files"].append(message)

    # debounce update to reduce spam
    if sess.get("job") and not sess["job"].done():
        sess["job"].cancel()

    async def _update_batch_menu_job():
        await asyncio.sleep(0.75)
        await update_batch_menu(client, user_id, message)

    sess["job"] = asyncio.create_task(_update_batch_menu_job())

async def update_batch_menu(client: Client, user_id: int, last_message: Message):
    if user_id not in user_sessions:
        return
    file_count = len(user_sessions[user_id]["files"])
    text = f"__✅ **Batch Updated!** You Have **{file_count}** Files In The Queue. What's Next?__"
    buttons = [
        [InlineKeyboardButton("🔗 Get Free Link", callback_data="get_link")],
        [InlineKeyboardButton("➕ Add More Files", callback_data="add_more")],
        [InlineKeyboardButton("❌ Close", callback_data="close_msg")]
    ]
    if user_id in ADMINS:
        # append paid option
        buttons[0].append(InlineKeyboardButton("💰 Set Price & Sell", callback_data="set_price"))

    keyboard = InlineKeyboardMarkup(buttons)
    old_menu_id = user_sessions[user_id].get("menu_msg_id")
    if old_menu_id:
        try:
            await client.delete_messages(user_id, old_menu_id)
        except Exception:
            pass

    new_menu_msg = await last_message.reply_text(text, reply_markup=keyboard, quote=True)
    user_sessions[user_id]["menu_msg_id"] = new_menu_msg.id

@app.on_callback_query(filters.regex("^(get_link|add_more|set_price)$"))
async def batch_options_callback(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    if user_id not in user_sessions or not user_sessions[user_id].get("files"):
        await query.answer("__Your Session Has Expired. Please Send Files Again.__", show_alert=True)
        return

    if query.data == "set_price" and user_id not in ADMINS:
        await query.answer("❗️ This feature is available for Admins only.", show_alert=True)
        return

    if query.data == "add_more":
        await query.answer("__✅ OK. Send Me More Files To Add To This Batch. ✅__", show_alert=True)
        return

    # proceed to copy files to LOG_CHANNEL
    try:
        await query.message.edit_text("__⏳ `Step 1/2`: Copying Files To Secure Storage...__")
    except MessageNotModified:
        pass

    log_message_ids = []
    try:
        for msg in user_sessions[user_id]["files"]:
            # copy each message to LOG_CHANNEL to store the file
            copied = await msg.copy(chat_id=LOG_CHANNEL)
            log_message_ids.append(copied.id)
    except Exception as e:
        logging.exception("Error copying files to log channel: %s", e)
        await query.message.edit_text(f"__❌ Error Copying Files: `{e}`. Please Start Again.__")
        user_sessions.pop(user_id, None)
        return

    # generate batch_id and share link
    batch_id = generate_random_string(12)
    bot_me = await app.get_me()
    share_link = f"https://krpicture0.blogspot.com?start={batch_id}"

    if query.data == "get_link":
        # free link
        try:
            files_collection.insert_one({
                "_id": batch_id,
                "message_ids": log_message_ids,
                "owner_id": user_id,
                "is_paid": False,
                "created_at": datetime.now(timezone.utc)
            })
        except Exception as e:
            logging.exception("DB insert failed: %s", e)
            await query.message.edit_text("__❌ Database error. Try again later.__")
            user_sessions.pop(user_id, None)
            return

        try:
            await query.message.edit_text(f"__✅ **Free Link Generated for {len(log_message_ids)} file(s)!**\n\n`{share_link}`__", disable_web_page_preview=True)
        except MessageNotModified:
            pass
        user_sessions.pop(user_id, None)

    else:
        # paid flow
        try:
            status_msg = await query.message.edit_text("__💰 **Set A Price For File!**\n\n__Please Send The Price For This Batch In INR **(e.g., `10`)**.__")
        except MessageNotModified:
            # ignore
            status_msg = await query.message.reply_text("__💰 **Set A Price For File!**\n\n__Please Send The Price For This Batch In INR **(e.g., `10`)**.__")
        user_states[user_id] = {
            "state": "waiting_for_price",
            "log_ids": log_message_ids,
            "batch_id": batch_id,
            "status_msgs": [status_msg.id]
        }

# conversation handler for price, upi etc.
@app.on_message(filters.private & filters.text & ~filters.command(["start","help","setupi","myupi","stats","settings","ban","unban","linkinfo","editlink"]), group=1)
async def conversation_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in user_states:
        return

    state_info = user_states[user_id]
    state = state_info.get("state")
    state_info.setdefault("status_msgs", []).append(message.id)

    if state == "waiting_for_price":
        try:
            price = float(message.text.strip())
            if price <= 0:
                raise ValueError("price<=0")
            state_info["price"] = price
            user_doc = users_collection.find_one({"_id": user_id}) or {}
            if user_doc.get("upi_id"):
                upi_id = user_doc["upi_id"]
                status_msg = await message.reply(f"__✅ Price: `₹{price:.2f}` | UPI: `{upi_id}`\n⏳ Finalizing link...__")
                state_info["status_msgs"].append(status_msg.id)
                await create_paid_batch_in_db(client, message, state_info, upi_id)
            else:
                state_info["state"] = "waiting_for_upi"
                status_msg = await message.reply(f"__✅ Price: `₹{price:.2f}`.\n\nNow Send Your UPI ID (It Will Be Saved).__")
                state_info["status_msgs"].append(status_msg.id)
        except Exception:
            status_msg = await message.reply("__**Invalid Price.** Send A Number Like `10`.__")
            state_info["status_msgs"].append(status_msg.id)

    elif state == "waiting_for_upi":
        upi_id = message.text.strip()
        if not re.match(r"^[a-zA-Z0-9.\-_]{2,256}@[a-zA-Z]{2,64}$", upi_id):
            status_msg = await message.reply("__Invalid UPI ID format. Try again.__")
            state_info["status_msgs"].append(status_msg.id)
            return
        users_collection.update_one({"_id": user_id}, {"$set": {"upi_id": upi_id}}, upsert=True)
        status_msg = await message.reply(f"__✅ UPI ID `{upi_id}` Saved.\n⏳ Finalizing Link...__")
        state_info["status_msgs"].append(status_msg.id)
        await create_paid_batch_in_db(client, message, state_info, upi_id)

    elif state == "waiting_for_new_upi":
        upi_id = message.text.strip()
        if not re.match(r"^[a-zA-Z0-9.\-_]{2,256}@[a-zA-Z]{2,64}$", upi_id):
            await message.reply("__Invalid UPI ID Format. Try Again.__")
            return
        users_collection.update_one({"_id": user_id}, {"$set": {"upi_id": upi_id}}, upsert=True)
        await message.reply(f"__✅ Success! Your UPI ID Is Updated To: `{upi_id}`__")
        # cleanup previous messages that were stored in status_msgs
        try:
            await client.delete_messages(user_id, state_info.get("status_msgs", []))
        except Exception:
            pass
        user_states.pop(user_id, None)

# create paid batch DB entry
async def create_paid_batch_in_db(client: Client, message: Message, state_info: dict, upi_id: str):
    user_id = message.from_user.id
    try:
        batch_id = state_info["batch_id"]
        files_collection.insert_one({
            "_id": batch_id,
            "message_ids": state_info["log_ids"],
            "owner_id": user_id,
            "is_paid": True,
            "price": float(state_info["price"]),
            "upi_id": upi_id,
            "payee_name": message.from_user.first_name,
            "created_at": datetime.now(timezone.utc)
        })
        share_link = f"https://krpicture0.blogspot.com?start={batch_id}"
        await message.reply(f"__✅ **Paid Link Generated For {len(state_info['log_ids'])} file(s)!**\n\nPrice: `₹{state_info['price']:.2f}`\n\n`{share_link}`__", disable_web_page_preview=True)
        # delete status messages
        try:
            await client.delete_messages(user_id, state_info.get("status_msgs", []))
        except Exception:
            pass
    except Exception as e:
        logging.exception("Failed to create paid batch: %s", e)
        await message.reply(f"__❌ An error occurred: `{e}`__")
    finally:
        user_states.pop(user_id, None)
        user_sessions.pop(user_id, None)

# -------------------------
# Edit link feature
# -------------------------
@app.on_message(filters.command("editlink") & filters.private)
async def edit_link_command(client: Client, message: Message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        await message.reply("__**How to use:**__\n`/editlink <batch_id>`\n\n_You can only edit links that you have created._")
        return

    batch_id_input = message.command[1]
    if "http" in batch_id_input:
        try:
            parsed = urlparse(batch_id_input)
            batch_id = parse_qs(parsed.query).get("start", [None])[0]
            if not batch_id:
                raise ValueError("missing start")
        except Exception:
            await message.reply("__❗️ Invalid link format. Provide the Batch ID, not full URL.__")
            return
    else:
        batch_id = batch_id_input

    batch_record = files_collection.find_one({"_id": batch_id})
    if not batch_record:
        await message.reply(f"__❌ No link found with Batch ID: `{batch_id}`__")
        return
    if batch_record.get("owner_id") != user_id:
        await message.reply("__🔒 You can only edit links that you have created.__")
        return

    EDIT_SESSIONS[batch_id] = {
        "owner_id": user_id,
        "files": list(batch_record.get("message_ids", [])),
        "edit_msg_id": None
    }
    user_states[user_id] = {"state": "editing_link", "batch_id": batch_id}
    text, keyboard = await generate_edit_menu(batch_id)
    edit_msg = await message.reply(text, reply_markup=keyboard)
    EDIT_SESSIONS[batch_id]["edit_msg_id"] = edit_msg.id

@app.on_callback_query(filters.regex("^edit_"))
async def edit_link_callbacks(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    parts = query.data.split("_")
    # expected: edit_<action>_<batch_id>[_<index>]
    if len(parts) < 3:
        await query.answer("Invalid action.", show_alert=True)
        return
    action = parts[1]
    batch_id = parts[2]

    session = EDIT_SESSIONS.get(batch_id)
    if not session or session.get("owner_id") != user_id:
        await query.answer("This edit session is invalid or has expired.", show_alert=True)
        return

    if action == "delete":
        try:
            index = int(parts[3])
            del session["files"][index]
            await query.answer("✅ File removed.")
            text, keyboard = await generate_edit_menu(batch_id)
            await query.message.edit_text(text, reply_markup=keyboard)
        except Exception:
            await query.answer("Could not delete this file. It may have been removed.", show_alert=True)

    elif action == "add":
        await query.answer("✅ OK. Send me more files to add to this batch. When done, click 'Save Changes'.", show_alert=True)
        user_states[user_id] = {"state": "editing_adding_files", "batch_id": batch_id}

    elif action == "cancel":
        del EDIT_SESSIONS[batch_id]
        user_states.pop(user_id, None)
        await query.message.edit_text("__❌ Edit cancelled.__")

    elif action == "save":
        new_list = session.get("files", [])
        if not new_list:
            await query.answer("❗️ You cannot save an empty link. Add at least one file.", show_alert=True)
            return
        files_collection.update_one({"_id": batch_id}, {"$set": {"message_ids": new_list}})
        del EDIT_SESSIONS[batch_id]
        user_states.pop(user_id, None)
        await query.message.edit_text(f"__✅ **Link `{batch_id}` updated successfully!** It now contains **{len(new_list)}** files.__")

# When adding files to an edit session, group=2 to avoid intercepting the first handler
@app.on_message(filters.private & (filters.document | filters.video | filters.photo | filters.audio), group=2)
async def file_handler_for_editing(client: Client, message: Message):
    user_id = message.from_user.id
    state = user_states.get(user_id, {}).get("state")
    if state != "editing_adding_files":
        return
    batch_id = user_states[user_id].get("batch_id")
    if batch_id not in EDIT_SESSIONS:
        await message.reply("__Edit session expired. Start again with /editlink.__")
        user_states.pop(user_id, None)
        return
    try:
        copied = await message.copy(chat_id=LOG_CHANNEL)
        EDIT_SESSIONS[batch_id]["files"].append(copied.id)
        edit_msg_id = EDIT_SESSIONS[batch_id]["edit_msg_id"]
        text, keyboard = await generate_edit_menu(batch_id)
        # update the edit message in user's chat
        try:
            await client.edit_message_text(user_id, edit_msg_id, text, reply_markup=keyboard)
        except Exception:
            pass
        await message.reply_text("__✅ File added to the edit session.__", quote=True)
    except Exception as e:
        logging.exception("Could not add file to edit session: %s", e)
        await message.reply_text(f"__❌ Could not add file: {e}__")

# -------------------------
# Payment & link processing
# -------------------------
async def process_link_click(client: Client, user_id: int, batch_id: str):
    batch_record = files_collection.find_one({"_id": batch_id})
    if not batch_record:
        try:
            await client.send_message(user_id, "__🤔 **Link Expired or Invalid**__")
        except Exception:
            pass
        return

    if not batch_record.get("is_paid", False):
        await send_files_from_batch(client, user_id, batch_record, FREE_DELETE_DELAY_MINUTES, "Minutes")
        return

    # paid flow: allocate unique amount (base price + cents)
    base_price = float(batch_record.get("price", 0))
    pending_amounts = {p["unique_amount"] for p in payments_collection.find({}, {"unique_amount": 1})}
    unique_amount_str = None
    # try up to 500 variations
    for i in range(1, 500):
        temp_amount = f"{base_price + (i / 100.0):.2f}"
        if temp_amount not in pending_amounts:
            unique_amount_str = temp_amount
            break
    if not unique_amount_str:
        await client.send_message(user_id, "__🚦 Sorry, the server is busy. Please try again in a minute.__")
        return

    payment_id = generate_random_string(12)
    payments_collection.insert_one({
        "_id": payment_id,
        "batch_id": batch_id,
        "buyer_id": user_id,
        "unique_amount": unique_amount_str,
        "created_at": datetime.now(timezone.utc)
    })

    # schedule expiration
    run_time = datetime.now(IST) + timedelta(minutes=PAYMENT_EXPIRATION_MINUTES)
    try:
        scheduler.add_job(expire_payment_job, "date", run_date=run_time, args=[payment_id, user_id, batch_id], id=payment_id, replace_existing=True)
    except Exception as e:
        logging.warning("Could not schedule payment expiration job: %s", e)

    bot_username = (await client.get_me()).username
    payee_name = quote_plus(batch_record.get("payee_name", "Seller"))
    upi_id = batch_record.get("upi_id", "")
    payment_url = f"{PAYMENT_PAGE_URL}?amount={unique_amount_str}&upi={upi_id}&name={payee_name}&bot={bot_username}"
    pay_btn = InlineKeyboardButton("💰 Pay Now", url=payment_url)
    paid_btn = InlineKeyboardButton("✅ I Have Paid", callback_data=f"i_paid_{payment_id}")

    await client.send_message(
        user_id,
        f"**__🔒 This Is A Premium File Batch.__**\n\n**__IMPORTANT:__** __Pay the **EXACT AMOUNT** shown below. This session will expire in **{PAYMENT_EXPIRATION_MINUTES} minutes**.__\n\n__💰 **Amount To Pay:** `₹{unique_amount_str}`__\n\n__Click **'Pay Now'**, then click **'I Have Paid'** to request access.__",
        reply_markup=InlineKeyboardMarkup([[pay_btn], [paid_btn]])
    )

@app.on_callback_query(filters.regex(r"^i_paid_"))
async def i_have_paid_callback(client: Client, query: CallbackQuery):
    try:
        _, _, payment_id = query.data.split("_", 2)
    except Exception:
        await query.answer("__Invalid request.__", show_alert=True)
        return

    # cancel expiration job (if exists)
    try:
        scheduler.remove_job(payment_id)
    except JobLookupError:
        pass
    except Exception as e:
        logging.warning("Error removing payment expiration job: %s", e)

    payment_record = payments_collection.find_one({"_id": payment_id})
    if not payment_record:
        await query.answer("__This payment session has expired. Please generate a new payment link.__", show_alert=True)
        return

    batch_id = payment_record["batch_id"]
    batch_record = files_collection.find_one({"_id": batch_id})
    if not batch_record:
        await query.answer("__The File Batch Linked To This Payment Is No Longer Available.__", show_alert=True)
        return

    owner_id = batch_record["owner_id"]
    # schedule owner approval expiration
    run_time = datetime.now(IST) + timedelta(hours=APPROVAL_EXPIRATION_HOURS)
    try:
        scheduler.add_job(expire_approval_job, "date", run_date=run_time, args=[payment_id, query.from_user.id, owner_id], id=f"approve_{payment_id}", replace_existing=True)
    except Exception as e:
        logging.warning("Could not schedule approval expiration job: %s", e)

    await query.answer("__✅ Request Sent To The Seller. You Will Get The Files After Approval.__", show_alert=True)
    try:
        await query.message.edit_reply_markup(None)
    except Exception:
        pass

    approve_btn = InlineKeyboardButton("✅ Approve", callback_data=f"approve_{payment_id}")
    decline_btn = InlineKeyboardButton("❌ Decline", callback_data=f"decline_{payment_id}")

    try:
        buyer_user = query.from_user
        await client.send_message(
            owner_id,
            f"__🔔 **Payment Request**\n\n**User:** {buyer_user.mention} (`{buyer_user.id}`)\n**Batch ID:** `{batch_id}`\n\nThey Claim To Have Paid The Unique Amount Of **`₹{payment_record['unique_amount']}`**.\n\nPlease check your account for this **exact amount** and click **Approve**.__",
            reply_markup=InlineKeyboardMarkup([[approve_btn], [decline_btn]])
        )
    except Exception as e:
        logging.warning("Could Not Send Notification To Owner %s: %s", owner_id, e)

async def process_payment_approval(payment_id: str, approved_by: str = "Seller (Manual)"):
    # cancel approval expiration job
    try:
        scheduler.remove_job(f"approve_{payment_id}")
    except JobLookupError:
        pass
    except Exception as e:
        logging.warning("Error removing approval job: %s", e)

    payment_record = payments_collection.find_one({"_id": payment_id})
    if not payment_record:
        logging.warning("Approval failed: payment record not found %s", payment_id)
        return "__This Payment Request Has Expired Or Is Invalid.__"

    batch_id = payment_record["batch_id"]
    buyer_id = payment_record["buyer_id"]
    unique_amount = payment_record["unique_amount"]
    batch_record = files_collection.find_one({"_id": batch_id})
    if not batch_record:
        payments_collection.delete_one({"_id": payment_id})
        logging.error("Critical: Batch %s not found for payment %s. Deleted payment.", batch_id, payment_id)
        return f"__Error: The file batch `{batch_id}` no longer exists. Payment record deleted.__"

    owner_id = batch_record["owner_id"]
    delivered = await send_files_from_batch(app, buyer_id, batch_record, PAID_DELETE_DELAY_HOURS, "Hours")

    try:
        if delivered and approved_by.startswith("Automation"):
            await app.send_message(buyer_id, f"__✅ Your payment of `₹{unique_amount}` has been automatically approved! You are receiving the files.__")
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
            fail_message = f"__❌ **Delivery Failed!** The user `{buyer_id}` might have blocked the bot.__"
            await app.send_message(owner_id, fail_message)
            final_message_for_button = fail_message
    except Exception as e:
        logging.warning("Could not send notification to owner %s: %s", owner_id, e)
        final_message_for_button = "__An error occurred while notifying the owner.__"

    payments_collection.delete_one({"_id": payment_id})
    return final_message_for_button

@app.on_callback_query(filters.regex(r"^(approve|decline)_"))
async def payment_verification_callback(client: Client, query: CallbackQuery):
    try:
        action, payment_id = query.data.split("_", 1)
    except Exception:
        return
    owner_id = query.from_user.id
    payment_record = payments_collection.find_one({"_id": payment_id})
    if not payment_record:
        await query.answer("__This Payment Request Has Expired Or Is Invalid.__", show_alert=True)
        return
    batch_record = files_collection.find_one({"_id": payment_record["batch_id"]})
    if not batch_record or batch_record["owner_id"] != owner_id:
        await query.answer("__This Is Not For You.__", show_alert=True)
        return

    if action == "approve":
        result_message = await process_payment_approval(payment_id, approved_by="Seller (Manual)")
        try:
            await query.message.edit_text(result_message)
        except MessageNotModified:
            pass
    else:
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
        payments_collection.delete_one({"_id": payment_id})

async def send_files_from_batch(client, user_id: int, batch_record: dict, delay_amount: int, delay_unit: str):
    """Copies files from LOG_CHANNEL to the user, adds warning caption and schedules deletion."""
    try:
        await client.send_message(user_id, f"__✅ Access Granted! You Are Receiving **{len(batch_record.get('message_ids', []))}** Files.__")
    except Exception:
        pass

    all_sent_successfully = True
    for msg_id in batch_record.get("message_ids", []):
        try:
            sent_msg = await client.copy_message(chat_id=user_id, from_chat_id=LOG_CHANNEL, message_id=msg_id)
            if sent_msg is None:
                logging.warning("send_files_from_batch: log message %s returned None", msg_id)
                continue
            warning_text = f"\n\n\n__**⚠️ IMPORTANT!**\n\nThese Files Will Be **Automatically Deleted In {delay_amount} {delay_unit}**. Please Forward Them To Your **Saved Messages** Immediately.__"
            try:
                if getattr(sent_msg, "caption", None) is not None:
                    await sent_msg.edit_caption((sent_msg.caption or "") + warning_text)
                else:
                    await sent_msg.reply(warning_text, quote=True)
            except Exception:
                try:
                    await client.send_message(user_id, warning_text)
                except Exception:
                    pass

            # schedule deletion
            run_time = datetime.now(IST) + (timedelta(minutes=delay_amount) if delay_unit == "Minutes" else timedelta(hours=delay_amount))
            try:
                scheduler.add_job(delete_message_job, "date", run_date=run_time, args=[sent_msg.chat.id, [sent_msg.id]], misfire_grace_time=300)
            except Exception as e:
                logging.warning("Failed to schedule delete job for message %s: %s", getattr(sent_msg, "id", None), e)

        except (UserIsBlocked, InputUserDeactivated):
            all_sent_successfully = False
            logging.warning("Failed to send file to %s: blocked/deactivated", user_id)
            break
        except Exception as e:
            all_sent_successfully = False
            logging.exception("Error sending file %s to %s: %s", msg_id, user_id, e)
            try:
                await client.send_message(user_id, "__❌ Could Not Send One Of The Files. It Might Have Been Deleted From The Source.__")
            except Exception:
                pass
    return all_sent_successfully

# -------------------------
# Startup & shutdown with asyncio-safe main()
# -------------------------
async def start_services():
    """Start scheduler and Flask server (Flask runs in a background thread)."""
    try:
        scheduler.start()
        logging.info("Scheduler started.")
    except Exception as e:
        logging.exception("Failed to start scheduler: %s", e)

    # start flask in thread (health checks)
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logging.info("Flask webserver started.")

async def stop_services():
    """Stop scheduler and any background tasks cleanly."""
    try:
        scheduler.shutdown(wait=False)
        logging.info("Scheduler shutdown requested.")
    except Exception as e:
        logging.warning("Error shutting down scheduler: %s", e)

async def shutdown(loop, stop_event: asyncio.Event):
    """Trigger shutdown when called from signal handler."""
    logging.info("Shutdown initiated.")
    stop_event.set()

async def main():
    # stop_event is used to keep the app running until a signal is received
    stop_event = asyncio.Event()

    # register signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(loop, stop_event)))
        except NotImplementedError:
            # some platforms (Windows) may not support add_signal_handler
            pass

    # start services
    await start_services()

    # start the pyrogram client
    try:
        await app.start()
        logging.info("Pyrogram client started.")
    except Exception as e:
        logging.exception("Failed to start Pyrogram client: %s", e)
        # try to stop scheduler/flask and exit
        await stop_services()
        return

    # wait until stop_event is set (triggered by signal handler)
    try:
        logging.info("Bot is running. Waiting for stop signal...")
        await stop_event.wait()
    finally:
        logging.info("Stop signal received. Shutting down...")

    # graceful shutdown
    try:
        await app.stop()
        logging.info("Pyrogram client stopped.")
    except RuntimeError:
        logging.warning("Event loop already closed during app.stop().")
    except Exception as e:
        logging.warning("Error stopping Pyrogram client: %s", e)

    # stop other services
    try:
        await stop_services()
    except Exception as e:
        logging.warning("Error during stop_services: %s", e)

if __name__ == "__main__":
    logging.info("Starting services...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received. Exiting.")
    except Exception as e:
        logging.exception("Unhandled exception in main: %s", e)
