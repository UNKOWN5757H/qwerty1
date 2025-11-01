# --- Files-Store-main/bot.py (VERSION 12.0 - EDIT LINK FEATURE & FINAL FIXES) ---

import os
import logging
import random
import string
import asyncio
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import pytz
import re
from urllib.parse import urlparse, parse_qs

from pyrogram import Client, filters, enums
from pyrogram.errors import UserNotParticipant, FloodWait, UserIsBlocked, InputUserDeactivated, MessageNotModified
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery

from pymongo import MongoClient
from flask import Flask, request, jsonify
from threading import Thread
from urllib.parse import quote_plus

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.base import JobLookupError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('apscheduler').setLevel(logging.WARNING)

load_dotenv()
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")
LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL"))
UPDATE_CHANNEL = os.environ.get("UPDATE_CHANNEL")
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "")
ADMINS = [int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(',') if admin_id]
PAYMENT_PAGE_URL = os.environ.get("PAYMENT_PAGE_URL", "https://t.me/Nikhil5757h")
AUTOMATION_SECRET = os.environ.get("AUTOMATION_SECRET")

flask_app = Flask(__name__)
@flask_app.route('/')
def index(): return "Bot is alive!", 200
@flask_app.route('/api/shortcut', methods=['POST'])
def shortcut_webhook():
    provided_secret = request.headers.get('X-Shortcut-Secret')
    if not AUTOMATION_SECRET or provided_secret != AUTOMATION_SECRET: return jsonify({"status": "error", "message": "Unauthorized"}), 403
    sms_text = request.get_data(as_text=True)
    if not sms_text: return jsonify({"status": "error", "message": "Bad Request: SMS text is missing"}), 400
    amount_match = re.search(r'(?:Rs\.?|₹|INR)\s*([\d,]+\.\d{2})', sms_text)
    if not amount_match: return jsonify({"status": "info", "message": "No valid amount found in SMS"}), 200
    unique_amount = amount_match.group(1).replace(',', '')
    payment_record = payments_collection.find_one({"unique_amount": unique_amount})
    if payment_record:
        batch_record = files_collection.find_one({"_id": payment_record.get('batch_id')})
        if batch_record and batch_record.get('owner_id') in ADMINS:
            payment_id = payment_record["_id"]
            future = asyncio.run_coroutine_threadsafe(process_payment_approval(payment_id, approved_by="Automation 🤖"), app.loop)
            try: future.result(timeout=15); return jsonify({"status": "success", "message": f"Admin payment {unique_amount} approved."}), 200
            except Exception as e: return jsonify({"status": "error", "message": f"Async task failed: {e}"}), 500
        else: return jsonify({"status": "info", "message": "Payment is for a normal user, manual approval required."}), 200
    else: return jsonify({"status": "info", "message": "No pending user for this amount."}), 200
def run_flask():
    port = int(os.environ.get('PORT', 8080)); flask_app.run(host='0.0.0.0', port=port)

FREE_DELETE_DELAY_MINUTES, PAID_DELETE_DELAY_HOURS = 30, 24
PAYMENT_EXPIRATION_MINUTES, APPROVAL_EXPIRATION_HOURS = 30, 24
IST = pytz.timezone("Asia/Kolkata")

try:
    client = MongoClient(MONGO_URI); db = client['file_link_bot']
    files_collection, users_collection, settings_collection, payments_collection = db['file_batches'], db['users'], db['settings'], db['pending_payments']
    logging.info("MongoDB Connected Successfully!")
except Exception as e: logging.error(f"Error connecting to MongoDB: {e}"); exit()

jobstores = {'default': MongoDBJobStore(database="file_link_bot", collection="scheduler_jobs", client=client)}
scheduler = BackgroundScheduler(jobstores=jobstores, timezone="Asia/Kolkata")
app = Client("FileLinkBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_sessions, user_states = {}, {}

def delete_message_job(chat_id: int, message_ids: list):
    async def task():
        try: await app.delete_messages(chat_id=chat_id, message_ids=message_ids)
        except Exception as e: logging.warning(f"Failed to delete messages {message_ids} in {chat_id}: {e}")
    if app.is_connected: asyncio.run_coroutine_threadsafe(task(), app.loop)

def expire_payment_job(payment_id: str, user_id: int, batch_id: str):
    async def task():
        if payments_collection.find_one_and_delete({"_id": payment_id}):
            logging.info(f"Payment session {payment_id} expired for user {user_id} (user inactivity).")
            share_link = f"https://t.me/{(await app.get_me()).username}?start={batch_id}"
            try: await app.send_message(user_id, f"__⏳ Your payment session has expired because you did not confirm the payment within {PAYMENT_EXPIRATION_MINUTES} minutes.__\n\n__Please generate a new payment link by clicking here:__ {share_link}")
            except Exception: pass
    if app.is_connected: asyncio.run_coroutine_threadsafe(task(), app.loop)

def expire_approval_job(payment_id: str, user_id: int, owner_id: int):
    async def task():
        if payments_collection.find_one_and_delete({"_id": payment_id}):
            logging.info(f"Approval request {payment_id} for user {user_id} expired (admin inactivity).")
            try:
                await app.send_message(user_id, f"__😔 We are sorry, but the seller did not respond to your payment confirmation within {APPROVAL_EXPIRATION_HOURS} hours. Your request has been cancelled. Please contact the seller or support directly.__")
                await app.send_message(owner_id, f"__⚠️ The payment approval request for user `{user_id}` has expired because you did not take any action for {APPROVAL_EXPIRATION_HOURS} hours. The request has been cancelled.__")
            except Exception: pass
    if app.is_connected: asyncio.run_coroutine_threadsafe(task(), app.loop)

def generate_random_string(length=10): return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
async def is_user_member(client: Client, user_id: int) -> bool:
    if not UPDATE_CHANNEL: return True
    try: await client.get_chat_member(chat_id=f"@{UPDATE_CHANNEL}", user_id=user_id); return True
    except UserNotParticipant: return False
    except Exception: return False
async def add_user_to_db(message: Message):
    user_data = {"first_name": message.from_user.first_name, "last_name": message.from_user.last_name, "username": message.from_user.username}
    users_collection.update_one({"_id": message.from_user.id}, {"$set": user_data, "$setOnInsert": {"banned": False, "joined_date": datetime.now(timezone.utc)}}, upsert=True)
async def get_bot_mode() -> str:
    setting = settings_collection.find_one({"_id": "bot_mode"}); return setting.get("mode", "public") if setting else "public"

def get_start_text(): return "__**Hey! I am PermaStore Bot 🤖**\n\nSend Me Any File! And I'll Gives You A **Permanent** Shareable Link! Which **Never Expired.**__"
def get_start_keyboard(): return InlineKeyboardMarkup([[InlineKeyboardButton("❓ How to Use / Help", callback_data="show_help")]])

# --- UPDATED: Help message with /editlink and admin-only UPI commands ---
def get_help_text_and_keyboard(user_id: int):
    help_text = ("__**Here's how to use me:**__\n\n"
                 "__1. **Send Files:** Send me any file, or forward multiple files at once.__\n\n"
                 "__2. **Use the Menu:** After you send a file, a menu will appear:__\n\n"
                 "   __- **🔗 Get Free Link:** Creates a permanent link for all files in your batch.__\n\n"
                 "   __- **➕ Add More Files:** Allows you to send more files to the current batch.__\n\n"
                 "__**Available Commands:**__\n"
                 "__/start__ `- Restart the bot and clear any session.`\n\n"
                 "__/editlink <batch_id>__ `- Edit an existing link you created.`\n\n"
                 "__/help__ `- Show this help message.`\n\n")
    if user_id in ADMINS:
        help_text = ("__**Here's how to use me:**__\n\n"
                     "__1. **Send Files:** Send me any file, or forward multiple files at once.__\n\n"
                     "__2. **Use the Menu:** After you send a file, a menu will appear:__\n\n"
                     "   __- **🔗 Get Free Link:** Creates a permanent link for all files.__\n\n"
                     "   __- **💰 Set Price & Sell:** Lets you set a price and get a paid link.__\n\n\n"
                     "   __- **➕ Add More Files:** Allows you to add more files to the current batch.__\n\n"
                     "__**Available Commands:**__\n\n"
                     "__/start__ `- Restart the bot and clear any session.`\n\n"
                     "__/editlink <batch_id>__ `- Edit an existing link you created.`\n\n"
                     "__/setupi__ `- Save or update your UPI ID for receiving payments.`\n\n"
                     "__/myupi__ `- Check your currently saved UPI ID.`\n\n"
                     "__/help__ `- Show this help message.`\n\n"
                     "__**👑 Admin Commands:**__\n\n"
                     "__/stats__ `- Get bot statistics.`\n\n"
                     "__/settings__ `- Change bot mode (Public/Private).`\n\n"
                     "__/ban <user_id>__ `- Ban a user.`\n\n"
                     "__/unban <user_id>__ `- Unban a user.`\n\n"
                     "__/linkinfo <batch_id>__ `- Get details of a specific link.`\n\n")
    return help_text, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start")]])

@app.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    await add_user_to_db(message); user_id = message.from_user.id
    if user_id in user_sessions:
        try:
            if user_sessions[user_id].get('menu_msg_id'): await client.delete_messages(user_id, user_sessions[user_id]['menu_msg_id'])
        except Exception: pass
    user_sessions.pop(user_id, None); user_states.pop(user_id, None)
    if len(message.command) > 1:
        batch_id = message.command[1]
        if not await is_user_member(client, user_id):
            join_btn, joined_btn = InlineKeyboardButton("🔗 Join Channel", url=f"https://t.me/{UPDATE_CHANNEL}"), InlineKeyboardButton("✅ I Have Joined", callback_data=f"check_join_{batch_id}")
            await message.reply(f"__👋 **Hello!**\n\nJoin Our Update Channel To Access The Content.__", reply_markup=InlineKeyboardMarkup([[join_btn], [joined_btn]])); return
        await process_link_click(client, user_id, batch_id)
    else: await message.reply(get_start_text(), reply_markup=get_start_keyboard())
@app.on_message(filters.command("help") & filters.private)
async def help_handler(client: Client, message: Message): help_text, help_keyboard = get_help_text_and_keyboard(message.from_user.id); await message.reply(help_text, reply_markup=help_keyboard, disable_web_page_preview=True)
@app.on_callback_query(filters.regex("^(show_help|back_to_start)$"))
async def navigation_callbacks(client: Client, query: CallbackQuery):
    action = query.data
    try:
        if action == "show_help": help_text, help_keyboard = get_help_text_and_keyboard(query.from_user.id); await query.message.edit_text(help_text, reply_markup=help_keyboard, disable_web_page_preview=True)
        elif action == "back_to_start": await query.message.edit_text(get_start_text(), reply_markup=get_start_keyboard())
    except MessageNotModified: await query.answer()
    except Exception as e: logging.error(f"Navigation callback error: {e}"); await query.answer("__An error occurred.__", show_alert=True)
@app.on_callback_query(filters.regex("^close_msg$"))
async def close_message_callback(client: Client, query: CallbackQuery):
    try: await query.message.delete(); user_sessions.pop(query.from_user.id, None)
    except Exception as e: await query.answer("__Could not delete message.__", show_alert=True); logging.warning(f"Failed to delete message on close command: {e}")
@app.on_message(filters.command("stats") & filters.private & filters.user(ADMINS))
async def stats_handler(client: Client, message: Message):
    total_users, banned_users, total_batches, paid_batches = users_collection.count_documents({}), users_collection.count_documents({"banned": True}), files_collection.count_documents({}), files_collection.count_documents({"is_paid": True})
    await message.reply(f"__📊 **Bot Statistics**\n\n👤 **Users:**\n   - Total Users: `{total_users}`\n   - Banned Users: `{banned_users}`\n\n🔗 **Links (Batches):**\n   - Total Batches: `{total_batches}`\n   - Paid Batches: `{paid_batches}`\n   - Free Batches: `{total_batches - paid_batches}`__")
@app.on_message(filters.command("ban") & filters.private & filters.user(ADMINS))
async def ban_handler(client: Client, message: Message):
    if len(message.command) < 2: await message.reply("__Usage: `/ban <user_id>`__"); return
    try:
        user_id_to_ban = int(message.command[1])
        if user_id_to_ban in ADMINS: await message.reply("__❌ You cannot ban an admin.__"); return
        users_collection.update_one({"_id": user_id_to_ban}, {"$set": {"banned": True}}, upsert=True); await message.reply(f"__✅ User `{user_id_to_ban}` has been banned successfully.__")
    except ValueError: await message.reply("__Invalid User ID provided.__")
@app.on_message(filters.command("unban") & filters.private & filters.user(ADMINS))
async def unban_handler(client: Client, message: Message):
    if len(message.command) < 2: await message.reply("__Usage: `/unban <user_id>`__"); return
    try:
        user_id_to_unban = int(message.command[1])
        users_collection.update_one({"_id": user_id_to_unban}, {"$set": {"banned": False}}); await message.reply(f"__✅ User `{user_id_to_unban}` has been unbanned.__")
    except ValueError: await message.reply("__Invalid User ID provided.__")
@app.on_message(filters.command("linkinfo") & filters.private & filters.user(ADMINS))
async def linkinfo_handler(client: Client, message: Message):
    if len(message.command) < 2: await message.reply("__Usage: `/linkinfo <batch_id>`__"); return
    batch_id, batch_record = message.command[1], files_collection.find_one({"_id": message.command[1]})
    if not batch_record: await message.reply(f"__❌ No link found with Batch ID: `{batch_id}`__"); return
    owner_id, owner_info = batch_record.get('owner_id'), users_collection.find_one({"_id": batch_record.get('owner_id')})
    owner_details = f"__{owner_info.get('first_name', '')} (@{owner_info.get('username', 'N/A')})__" if owner_info else "__Unknown (Not in DB)__"
    link_type, file_count = "Paid 💰" if batch_record.get('is_paid') else "Free 🆓", len(batch_record.get('message_ids', []))
    text = f"__**🔗 Link Information**\n\n- **Batch ID:** `{batch_id}`\n- **Link Type:** {link_type}\n- **File Count:** `{file_count}`\n\n👤 **Owner Details:**\n- **User ID:** `{owner_id}`\n- **Name/Username:** {owner_details}__\n"
    if batch_record.get('is_paid'): text += f"__- **Price:** `₹{batch_record.get('price', 0):.2f}`\n__- **UPI ID:** `{batch_record.get('upi_id', 'N/A')}`__"
    await message.reply(text)
@app.on_message(filters.command("settings") & filters.private & filters.user(ADMINS))
async def settings_handler(client: Client, message: Message):
    current_mode = await get_bot_mode()
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🌍 Public (Anyone Can Upload)", callback_data="set_mode_public")], [InlineKeyboardButton("🔒 Private (Admins Only)", callback_data="set_mode_private")]])
    await message.reply(f"__⚙️ **Bot Settings**\n\nCurrent Mode: **{current_mode.upper()}**.\nSelect A New Mode:__", reply_markup=keyboard)
@app.on_callback_query(filters.regex(r"^set_mode_") & filters.user(ADMINS))
async def set_mode_callback(client: Client, query: CallbackQuery):
    new_mode = query.data.split("_")[2]
    settings_collection.update_one({"_id": "bot_mode"}, {"$set": {"mode": new_mode}}, upsert=True)
    await query.answer(f"__Mode set to {new_mode.upper()}!__", show_alert=True)
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🌍 Public (Anyone Can Upload)", callback_data="set_mode_public")], [InlineKeyboardButton("🔒 Private (Admins Only)", callback_data="set_mode_private")]])
    await query.message.edit_text(f"__⚙️ **Bot Settings**\n\nMode Is Now **{new_mode.upper()}**.\nSelect A New Mode:__", reply_markup=keyboard)

# --- MODIFIED: UPI commands are now Admin-Only ---
@app.on_message(filters.command(["setupi", "myupi"]) & filters.private & filters.user(ADMINS))
async def upi_commands_handler(client: Client, message: Message):
    user_id, command = message.from_user.id, message.command[0].lower()
    if command == "setupi": user_states[user_id] = {'state': 'waiting_for_new_upi', 'status_msgs': [message.id]}; await message.reply("__Please Send Your Upi Id To Save Or Update It.\nExample: `yourname@upi`__")
    elif command == "myupi":
        user_doc = users_collection.find_one({"_id": user_id})
        if user_doc and user_doc.get("upi_id"): await message.reply(f"__Your saved UPI ID is: `{user_doc['upi_id']}`\n\nTo change it, use /setupi.__")
        else: await message.reply("__You Have Not Set A UPI ID Yet. Use /setupi To Save One.__")

# --- All handlers from file_handler to create_paid_batch_in_db are the same ---
# --- (The logic inside them is now protected by the UI changes) ---
@app.on_message(filters.private & (filters.document | filters.video | filters.photo | filters.audio), group=1)
async def file_handler(client: Client, message: Message):
    user_id, user_doc, bot_mode = message.from_user.id, users_collection.find_one({"_id": message.from_user.id}), await get_bot_mode()
    if user_doc and user_doc.get("banned", False): await message.reply("__❌ **You are banned.**\nYou are no longer allowed to use this bot.__"); return
    if bot_mode == "private" and user_id not in ADMINS: await message.reply("__😔 Sorry! Only Admins Are Allowed To Upload Files At The Moment.__"); return
    if user_id in user_states: return
    if user_id not in user_sessions: user_sessions[user_id] = {'files': [], 'menu_msg_id': None, 'job': None}
    user_sessions[user_id]['files'].append(message)
    if user_sessions[user_id]['job']: user_sessions[user_id]['job'].cancel()
    async def _update_batch_menu_job(): await asyncio.sleep(0.75); await update_batch_menu(client, user_id, message)
    user_sessions[user_id]['job'] = asyncio.create_task(_update_batch_menu_job())

async def update_batch_menu(client: Client, user_id: int, last_message: Message):
    if user_id not in user_sessions: return
    file_count = len(user_sessions[user_id]['files'])
    text = f"__✅ **Batch Updated!** You Have **{file_count}** Files In The Queue. What's Next?__"
    buttons = [[InlineKeyboardButton("🔗 Get Free Link", callback_data="get_link")], [InlineKeyboardButton("➕ Add More Files", callback_data="add_more")], [InlineKeyboardButton("❌ Close", callback_data="close_msg")]]
    if user_id in ADMINS: buttons[0].append(InlineKeyboardButton("💰 Set Price & Sell", callback_data="set_price"))
    keyboard = InlineKeyboardMarkup(buttons)
    if old_menu_id := user_sessions[user_id].get('menu_msg_id'):
        try: await client.delete_messages(user_id, old_menu_id)
        except Exception: pass
    new_menu_msg = await last_message.reply_text(text, reply_markup=keyboard, quote=True); user_sessions[user_id]['menu_msg_id'] = new_menu_msg.id

@app.on_callback_query(filters.regex("^(get_link|add_more|set_price)$"))
async def batch_options_callback(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    if user_id not in user_sessions or not user_sessions[user_id]['files']: await query.answer("__Your Session Has Expired. Please Send Files Again.__", show_alert=True); return
    if query.data == "set_price" and user_id not in ADMINS: await query.answer("❗️ This feature is available for Admins only.", show_alert=True); return
    if query.data == "add_more": await query.answer("__✅ OK. Send Me More Files To Add To This Batch. ✅__", show_alert=True); return
    await query.message.edit_text("__⏳ `Step 1/2`: Copying Files To Secure Storage...__")
    log_message_ids = []
    try:
        for msg in user_sessions[user_id]['files']: log_message_ids.append((await msg.copy(LOG_CHANNEL)).id)
    except Exception as e: await query.message.edit_text(f"__❌ Error Copying Files: `{e}`. Please Start Again.__"); user_sessions.pop(user_id, None); return
    batch_id, share_link = generate_random_string(), f"https://krpicture0.blogspot.com?start={generate_random_string()}" # Bugfix, use actual batch_id
    if query.data == "get_link":
        await query.message.edit_text("__⏳ `Step 2/2`: Generating Your Link...__")
        files_collection.insert_one({'_id': batch_id, 'message_ids': log_message_ids, 'owner_id': user_id, 'is_paid': False})
        await query.message.edit_text(f"__✅ **Free Link Generated for {len(log_message_ids)} file(s)!**\n\n`{share_link.replace(share_link.split('=')[-1], batch_id)}`__", disable_web_page_preview=True)
        user_sessions.pop(user_id, None)
    elif query.data == "set_price":
        status_msg = await query.message.edit_text("__💰 **Set A Price For File!**\n\n__Please Send The Price For This Batch In INR **(e.g., `10`)**.__")
        user_states[user_id] = {'state': 'waiting_for_price', 'log_ids': log_message_ids, 'batch_id': batch_id, 'status_msgs': [status_msg.id]}

@app.on_message(filters.private & filters.text & ~filters.command(["start", "help", "setupi", "myupi", "stats", "settings", "ban", "unban", "linkinfo", "editlink"]), group=1)
async def conversation_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in user_states: return
    state_info, state = user_states[user_id], user_states[user_id].get('state')
    state_info['status_msgs'].append(message.id)
    if state == 'waiting_for_price':
        try:
            price = float(message.text)
            if price <= 0: raise ValueError
            state_info['price'] = price
            user_doc = users_collection.find_one({"_id": user_id})
            if user_doc and user_doc.get("upi_id"):
                upi_id = user_doc["upi_id"]
                status_msg = await message.reply(f"__✅ Price: `₹{price:.2f}` | UPI: `{upi_id}`\n⏳ Finalizing link...__"); state_info['status_msgs'].append(status_msg.id)
                await create_paid_batch_in_db(client, message, state_info, upi_id)
            else:
                state_info['state'] = 'waiting_for_upi'
                status_msg = await message.reply(f"__✅ Price: `₹{price:.2f}`.\n\nNow Send Your UPI ID (It Will Be Saved).__"); state_info['status_msgs'].append(status_msg.id)
        except (ValueError, TypeError): status_msg = await message.reply("__**Invalid Price.** Send A Number Like `10`.__"); state_info['status_msgs'].append(status_msg.id)
    elif state == 'waiting_for_upi':
        upi_id = message.text.strip()
        if not re.match(r"^[a-zA-Z0-9.\-_]{2,256}@[a-zA-Z]{2,64}$", upi_id): status_msg = await message.reply("__Invalid UPI ID format. Try again.__"); state_info['status_msgs'].append(status_msg.id); return
        users_collection.update_one({"_id": user_id}, {"$set": {"upi_id": upi_id}}, upsert=True)
        status_msg = await message.reply(f"__✅ UPI ID `{upi_id}` Saved.\n⏳ Finalizing Link...__"); state_info['status_msgs'].append(status_msg.id)
        await create_paid_batch_in_db(client, message, state_info, upi_id)
    elif state == 'waiting_for_new_upi':
        upi_id = message.text.strip()
        if not re.match(r"^[a-zA-Z0-9.\-_]{2,256}@[a-zA-Z]{2,64}$", upi_id): await message.reply("__Invalid UPI ID Format. Try Again.__"); return
        users_collection.update_one({"_id": user_id}, {"$set": {"upi_id": upi_id}}, upsert=True)
        await message.reply(f"__✅ Success! Your UPI ID Is Updated To: `{upi_id}`__")
        await client.delete_messages(user_id, state_info['status_msgs']); user_states.pop(user_id, None)

# --- All other handlers from create_paid_batch_in_db to the end are the same ---
# ... (No changes are needed for the payment processing logic, it's now just admin-gated) ...

async def create_paid_batch_in_db(client: Client, message: Message, state_info: dict, upi_id: str):
    user_id = message.from_user.id
    try:
        batch_id, share_link = state_info['batch_id'], f"https://krpicture0.blogspot.com?start={state_info['batch_id']}"
        files_collection.insert_one({'_id': batch_id, 'message_ids': state_info['log_ids'], 'owner_id': user_id, 'is_paid': True, 'price': state_info['price'], 'upi_id': upi_id, 'payee_name': message.from_user.first_name})
        await message.reply(f"__✅ **Paid Link Generated For {len(state_info['log_ids'])} file(s)!**\n\nPrice: `₹{state_info['price']:.2f}`\n\n`{share_link}`__", disable_web_page_preview=True)
        await client.delete_messages(user_id, state_info['status_msgs'])
    except Exception as e: await message.reply(f"__❌ An error occurred: `{e}`__")
    finally: user_states.pop(user_id, None); user_sessions.pop(user_id, None)

@app.on_callback_query(filters.regex(r"^check_join_"))
async def check_join_callback(client: Client, query: CallbackQuery):
    user_id, _, _, batch_id = query.from_user.id, *query.data.split("_", 2)
    if await is_user_member(client, user_id): await query.message.delete(); await process_link_click(client, user_id, batch_id)
    else: await query.answer("__Please Join The Channel And Click Again.__", show_alert=True)

# ... (The rest of the code is unchanged from the version I sent before, including the new /editlink feature and its helpers) ...

# ---------------------------------------------------------------------------------
# --- NEW: EDIT LINK FEATURE ---
# ---------------------------------------------------------------------------------

EDIT_SESSIONS = {} # To store temporary data for editing sessions

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
        logging.warning(f"Could not get details for msg_id {msg_id}: {e}")
        return "DELETED/UNAVAILABLE FILE", "N/A"

async def generate_edit_menu(batch_id):
    batch_data = EDIT_SESSIONS.get(batch_id, {}).get('files', [])
    text = f"__✍️ **Editing Link:** `{batch_id}`__\n\n__You have **{len(batch_data)}** files in this batch. You can delete files or add more.__"
    buttons = []
    for i, msg_id in enumerate(batch_data):
        file_name, file_size = await get_file_details(msg_id)
        buttons.append([InlineKeyboardButton(f"❌ {file_name} ({file_size})", callback_data=f"edit_delete_{batch_id}_{i}")])
    
    buttons.append([
        InlineKeyboardButton("➕ Add More Files", callback_data=f"edit_add_{batch_id}"),
        InlineKeyboardButton("✅ Save Changes", callback_data=f"edit_save_{batch_id}")
    ])
    buttons.append([InlineKeyboardButton("❌ Cancel Edit", callback_data=f"edit_cancel_{batch_id}")])
    return text, InlineKeyboardMarkup(buttons)

@app.on_message(filters.command("editlink") & filters.private)
async def edit_link_command(client: Client, message: Message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        await message.reply("__**How to use:**__\n`/editlink <batch_id>`\n\n_You can only edit links that you have created._"); return
    
    batch_id_input = message.command[1]
    if 'http' in batch_id_input:
        try:
            parsed_url = urlparse(batch_id_input)
            batch_id = parse_qs(parsed_url.query)['start'][0]
        except Exception:
            await message.reply(f"__❗️ **Invalid Link format.**__\n\nPlease provide just the Batch ID, not the full URL.\n**Example:** `/editlink {generate_random_string()}`"); return
    else:
        batch_id = batch_id_input

    batch_record = files_collection.find_one({"_id": batch_id})
    if not batch_record:
        await message.reply(f"__❌ No link found with Batch ID: `{batch_id}`__"); return
    
    if batch_record.get('owner_id') != user_id:
        await message.reply("__🔒 You can only edit links that you have created.__"); return
    
    # Start the edit session
    EDIT_SESSIONS[batch_id] = {
        'owner_id': user_id,
        'files': list(batch_record.get('message_ids', [])),
        'edit_msg_id': None
    }
    user_states[user_id] = {'state': 'editing_link', 'batch_id': batch_id}
    
    text, keyboard = await generate_edit_menu(batch_id)
    edit_msg = await message.reply(text, reply_markup=keyboard)
    EDIT_SESSIONS[batch_id]['edit_msg_id'] = edit_msg.id

@app.on_callback_query(filters.regex("^edit_"))
async def edit_link_callbacks(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    parts = query.data.split("_")
    action = parts[1]
    batch_id = parts[2]
    
    if not EDIT_SESSIONS.get(batch_id) or EDIT_SESSIONS.get(batch_id, {}).get('owner_id') != user_id:
        await query.answer("This edit session is invalid or has expired.", show_alert=True); return

    if action == "delete":
        try:
            file_index = int(parts[3])
            del EDIT_SESSIONS[batch_id]['files'][file_index]
            await query.answer("✅ File removed.")
            text, keyboard = await generate_edit_menu(batch_id)
            await query.message.edit_text(text, reply_markup=keyboard)
        except (ValueError, IndexError):
            await query.answer("Could not delete this file. It may have already been removed.", show_alert=True)
    
    elif action == "add":
        await query.answer("✅ OK. Send me more files to add to this batch. When you are done, come back and click 'Save Changes'.", show_alert=True)
        user_states[user_id] = {'state': 'editing_adding_files', 'batch_id': batch_id}

    elif action == "cancel":
        del EDIT_SESSIONS[batch_id]
        user_states.pop(user_id, None)
        await query.message.edit_text("__❌ Edit cancelled.__")

    elif action == "save":
        new_file_list = EDIT_SESSIONS[batch_id]['files']
        if not new_file_list:
            await query.answer("❗️ You cannot save an empty link. Add at least one file.", show_alert=True); return
        
        files_collection.update_one({"_id": batch_id}, {"$set": {"message_ids": new_file_list}})
        del EDIT_SESSIONS[batch_id]
        user_states.pop(user_id, None)
        await query.message.edit_text(f"__✅ **Link `{batch_id}` updated successfully!** It now contains **{len(new_file_list)}** files.__")

# --- All other handlers from this point on are the same ---
# (They correctly handle the updated logic)
@app.on_message(filters.private & (filters.document | filters.video | filters.photo | filters.audio), group=2)
async def file_handler_for_editing(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id in user_states and user_states[user_id].get('state') == 'editing_adding_files':
        batch_id = user_states[user_id]['batch_id']
        if batch_id in EDIT_SESSIONS:
            try:
                log_message = await message.copy(LOG_CHANNEL)
                EDIT_SESSIONS[batch_id]['files'].append(log_message.id)
                edit_msg_id = EDIT_SESSIONS[batch_id]['edit_msg_id']
                text, keyboard = await generate_edit_menu(batch_id)
                await client.edit_message_text(user_id, edit_msg_id, text, reply_markup=keyboard)
                await message.reply_text("__✅ File added to the edit session.__", quote=True)
            except Exception as e:
                await message.reply_text(f"__❌ Could not add file: {e}__")

# --- Payment Processing Logic (Unchanged, as it's gated by admin features) ---
async def process_link_click(client: Client, user_id: int, batch_id: str):
    # ... This entire function remains the same as the final version
    batch_record = files_collection.find_one({"_id": batch_id})
    if not batch_record: await client.send_message(user_id, "__🤔 **Link Expired or Invalid**__"); return
    if not batch_record.get('is_paid', False):
        await send_files_from_batch(client, user_id, batch_record, FREE_DELETE_DELAY_MINUTES, 'Minutes')
    else:
        base_price, unique_amount_str = float(batch_record.get('price')), None
        pending_amounts = {p['unique_amount'] for p in payments_collection.find({}, {'unique_amount': 1})}
        for i in range(1, 100):
            temp_amount = f"{base_price + (i / 100.0):.2f}"
            if temp_amount not in pending_amounts: unique_amount_str = temp_amount; break
        if not unique_amount_str:
            logging.warning(f"All 99 standard slots for base price {base_price} are taken. Searching.")
            for i in range(100, 500):
                temp_amount = f"{base_price + (i / 100.0):.2f}"
                if temp_amount not in pending_amounts: unique_amount_str = temp_amount; break
        if not unique_amount_str: await client.send_message(user_id, "__🚦 Sorry, the server is extremely busy. Please try again in a minute.__"); return
        payment_id = generate_random_string(12)
        payments_collection.insert_one({"_id": payment_id, "batch_id": batch_id, "buyer_id": user_id, "unique_amount": unique_amount_str, "created_at": datetime.now(timezone.utc)})
        run_time = datetime.now(IST) + timedelta(minutes=PAYMENT_EXPIRATION_MINUTES)
        scheduler.add_job(expire_payment_job, "date", run_date=run_time, args=[payment_id, user_id, batch_id], id=payment_id, replace_existing=True)
        bot_username, payee_name, upi_id = (await client.get_me()).username, quote_plus(batch_record.get('payee_name', 'Seller')), batch_record.get('upi_id')
        payment_url = f"{PAYMENT_PAGE_URL}?amount={unique_amount_str}&upi={upi_id}&name={payee_name}&bot={bot_username}"
        pay_btn, paid_btn = InlineKeyboardButton("💰 Pay Now", url=payment_url), InlineKeyboardButton("✅ I Have Paid", callback_data=f"i_paid_{payment_id}")
        await client.send_message(user_id, f"**__🔒 This Is A Premium File Batch.__**\n\n**__IMPORTANT:__** __Pay the **EXACT AMOUNT** shown below. This session will expire in **{PAYMENT_EXPIRATION_MINUTES} minutes**.__\n\n__💰 **Amount To Pay:** `₹{unique_amount_str}`__\n\n__Click **'Pay Now'**, then click **'I Have Paid'** to request access.__", reply_markup=InlineKeyboardMarkup([[pay_btn], [paid_btn]]))

@app.on_callback_query(filters.regex(r"^i_paid_"))
async def i_have_paid_callback(client: Client, query: CallbackQuery):
    try: _, _, payment_id = query.data.split("_", 2)
    except: await query.answer("__Invalid request.__", show_alert=True); return
    try: scheduler.remove_job(payment_id); logging.info(f"User clicked 'I have paid'. Expiration job for {payment_id} cancelled.")
    except JobLookupError: pass
    except Exception as e: logging.error(f"Error removing job on 'I have paid' click for {payment_id}: {e}")
    payment_record = payments_collection.find_one({"_id": payment_id})
    if not payment_record: await query.answer(f"__This payment session has expired. Please generate a new payment link.__", show_alert=True); return
    batch_id, unique_amount, buyer_user = payment_record['batch_id'], payment_record['unique_amount'], query.from_user
    batch_record = files_collection.find_one({"_id": batch_id})
    if not batch_record: await query.answer("__The File Batch Linked To This Payment Is No Longer Available.__", show_alert=True); return
    owner_id = batch_record['owner_id']
    run_time = datetime.now(IST) + timedelta(hours=APPROVAL_EXPIRATION_HOURS)
    scheduler.add_job(expire_approval_job, "date", run_date=run_time, args=[payment_id, query.from_user.id, owner_id], id=f"approve_{payment_id}", replace_existing=True)
    await query.answer("__✅ Request Sent To The Seller. You Will Get The Files After Approval.__", show_alert=True)
    await query.message.edit_reply_markup(None)
    approve_btn, decline_btn = InlineKeyboardButton("✅ Approve", callback_data=f"approve_{payment_id}"), InlineKeyboardButton("❌ Decline", callback_data=f"decline_{payment_id}")
    try:
        await client.send_message(owner_id, f"__🔔 **Payment Request**\n\n**User:** {buyer_user.mention} (`{buyer_user.id}`)\n**Batch ID:** `{batch_id}`\n\nThey Claim To Have Paid The Unique Amount Of **`₹{unique_amount}`**.\n\nPlease check your account for this **exact amount** and click **Approve**.__", reply_markup=InlineKeyboardMarkup([[approve_btn], [decline_btn]]))
    except Exception as e: logging.warning(f"Could Not Send Notification To Owner {owner_id}: {e}")

async def process_payment_approval(payment_id: str, approved_by: str = "Seller (Manual)"):
    try: scheduler.remove_job(f"approve_{payment_id}"); logging.info(f"Successfully removed approval expiration job for ID: {payment_id}")
    except JobLookupError: pass
    except Exception as e: logging.error(f"Error removing approval job {payment_id}: {e}")
    payment_record = payments_collection.find_one({"_id": payment_id})
    if not payment_record: logging.warning(f"Approval failed: Payment record {payment_id} not found."); return "__This Payment Request Has Expired Or Is Invalid.__"
    batch_id, buyer_id, unique_amount = payment_record['batch_id'], payment_record['buyer_id'], payment_record['unique_amount']
    batch_record = files_collection.find_one({"_id": batch_id})
    if not batch_record:
        payments_collection.delete_one({"_id": payment_id}); logging.error(f"Critical Error: Batch {batch_id} not found for payment {payment_id}. Deleting orphan payment.")
        return f"__Error: The file batch `{batch_id}` no longer exists. Payment record deleted.__"
    owner_id = batch_record['owner_id']
    delivered = await send_files_from_batch(app, buyer_id, batch_record, PAID_DELETE_DELAY_HOURS, 'Hours')
    try:
        if delivered and approved_by.startswith("Automation"): await app.send_message(buyer_id, f"__✅ Your payment of `₹{unique_amount}` has been **automatically approved**! You are receiving the files.__")
    except Exception: pass
    final_message_for_button = ""
    try:
        if delivered:
            success_message = (f"__**✅ Files Delivered Successfully!**\n\n**Approved By:** {approved_by}\n**Buyer:** `{buyer_id}`\n**Batch ID:** `{batch_id}`\n**Amount:** `₹{unique_amount}`__")
            await app.send_message(owner_id, success_message)
            final_message_for_button = f"__✅ Payment Of `₹{unique_amount}` Approved For User `{buyer_id}`. Files have been sent.__"
        else:
            fail_message = f"__❌ **Delivery Failed!**\n\nThe user `{buyer_id}` might have blocked the bot.__"
            await app.send_message(owner_id, fail_message)
            final_message_for_button = fail_message
    except Exception as e: logging.warning(f"Could not send notification to owner {owner_id}: {e}"); final_message_for_button = "__An error occurred while notifying the owner.__"
    payments_collection.delete_one({"_id": payment_id})
    return final_message_for_button

@app.on_callback_query(filters.regex(r"^(approve|decline)_"))
async def payment_verification_callback(client: Client, query: CallbackQuery):
    try: action, payment_id = query.data.split("_", 1)
    except: return
    owner_id, payment_record = query.from_user.id, payments_collection.find_one({"_id": payment_id})
    if not payment_record: await query.answer("__This Payment Request Has Expired Or Is Invalid.__", show_alert=True); return
    batch_record = files_collection.find_one({"_id": payment_record['batch_id']})
    if not batch_record or batch_record['owner_id'] != owner_id: await query.answer("__This Is Not For You.__", show_alert=True); return
    if action == "approve":
        result_message = await process_payment_approval(payment_id, approved_by="Seller (Manual)")
        try: await query.message.edit_text(result_message)
        except MessageNotModified: pass
    elif action == "decline":
        try: scheduler.remove_job(f"approve_{payment_id}"); logging.info(f"Approval for {payment_id} declined. Removing expiration job.")
        except JobLookupError: pass
        buyer_id, unique_amount = payment_record['buyer_id'], payment_record['unique_amount']
        await query.message.edit_text(f"__❌ Payment Of `₹{unique_amount}` Declined For User `{buyer_id}`.__")
        try: await client.send_message(buyer_id, "__😔 **Payment Declined**\nThe Seller Could Not Verify Your Payment.__")
        except: pass
        payments_collection.delete_one({"_id": payment_id})

async def send_files_from_batch(client, user_id, batch_record, delay_amount, delay_unit):
    await client.send_message(user_id, f"__✅ Access Granted! You Are Receiving **{len(batch_record['message_ids'])}** Files.__")
    all_sent_successfully = True
    for msg_id in batch_record['message_ids']:
        try:
            sent_file_msg = await client.copy_message(chat_id=user_id, from_chat_id=LOG_CHANNEL, message_id=msg_id)
            warning_text = f"\n\n\n__**⚠️ IMPORTANT!**\n\nThese Files Will Be **Automatically Deleted In {delay_amount} {delay_unit}**. Please Forward Them To Your **Saved Messages** Immediately.__"
            captionable_media = (enums.MessageMediaType.VIDEO, enums.MessageMediaType.DOCUMENT, enums.MessageMediaType.PHOTO, enums.MessageMediaType.AUDIO)
            if sent_file_msg.media in captionable_media: await sent_file_msg.edit_caption((sent_file_msg.caption or "") + warning_text)
            else: await sent_file_msg.reply(warning_text, quote=True)
            run_time = datetime.now(IST) + (timedelta(minutes=delay_amount) if delay_unit == 'Minutes' else timedelta(hours=delay_amount))
            scheduler.add_job(delete_message_job, "date", run_date=run_time, args=[sent_file_msg.chat.id, [sent_file_msg.id]], misfire_grace_time=300)
        except (UserIsBlocked, InputUserDeactivated): all_sent_successfully = False; logging.warning(f"Failed to send file to {user_id}: User has blocked the bot."); break 
        except Exception as e: all_sent_successfully = False; logging.error(f"Error sending file {msg_id} to {user_id}: {e}"); await client.send_message(user_id, f"__❌ Could Not Send One Of The Files. It Might Have Been Deleted From The Source.__")
    return all_sent_successfully

if __name__ == "__main__":
    logging.info("Starting Flask web server...")
    flask_thread = Thread(target=run_flask); flask_thread.start()
    logging.info("Starting Scheduler...")
    scheduler.start()
    logging.info("Bot is starting...")
    app.run()
