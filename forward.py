#!/usr/bin/env python3
import os
import asyncio
import logging
import functools
import gc
import re
from typing import Dict, List, Optional, Tuple, Set, Callable
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
)
from database import Database
from webserver import start_server_thread, register_monitoring

# Optimized logging to reduce I/O
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("forward")

# Environment variables with optimized defaults for Render free tier
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# Support multiple owners / admins via OWNER_IDS (comma-separated)
OWNER_IDS: Set[int] = set()
owner_env = os.getenv("OWNER_IDS", "").strip()
if owner_env:
    for part in owner_env.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            OWNER_IDS.add(int(part))
        except ValueError:
            logger.warning("Invalid OWNER_IDS value skipped: %s", part)

# Support additional allowed users via ALLOWED_USERS (comma-separated)
ALLOWED_USERS: Set[int] = set()
allowed_env = os.getenv("ALLOWED_USERS", "").strip()
if allowed_env:
    for part in allowed_env.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ALLOWED_USERS.add(int(part))
        except ValueError:
            logger.warning("Invalid ALLOWED_USERS value skipped: %s", part)

# OPTIMIZED Tuning parameters for Render free tier (25+ users, unlimited forwarding)
SEND_WORKER_COUNT = int(os.getenv("SEND_WORKER_COUNT", "15"))  # Reduced workers to save memory
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "10000"))  # Reduced queue size
TARGET_RESOLVE_RETRY_SECONDS = int(os.getenv("TARGET_RESOLVE_RETRY_SECONDS", "30"))  # Faster retry
MAX_CONCURRENT_USERS = int(os.getenv("MAX_CONCURRENT_USERS", "50"))  # Increased user limit
MESSAGE_PROCESS_BATCH_SIZE = int(os.getenv("MESSAGE_PROCESS_BATCH_SIZE", "5"))  # Batch processing

db = Database()

# Conversation states
TASK_NAME, SOURCE_IDS, TARGET_IDS, PREFIX_SUFFIX = range(4)

# OPTIMIZED: Use weak references and smaller data structures
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}

# OPTIMIZED: Hot-path caches with memory limits
tasks_cache: Dict[int, List[Dict]] = {}  # user_id -> list of task dicts
target_entity_cache: Dict[int, Dict[int, object]] = {}  # user_id -> {target_id: resolved_entity}
# handler_registered maps user_id -> handler callable (so we can remove it)
handler_registered: Dict[int, Callable] = {}

# Global send queue is created later on the running event loop (in post_init/start_send_workers)
send_queue: Optional[asyncio.Queue] = None

UNAUTHORIZED_MESSAGE = """🚫 **Access Denied!** 

You are not authorized to use this bot.

📞 **Call this number:** `07089430305`

Or

🗨️ **Message Developer:** [HEMMY](https://t.me/justmemmy)
"""

# Track worker tasks so we can cancel them on shutdown
worker_tasks: List[asyncio.Task] = []
_send_workers_started = False

# MAIN loop reference for cross-thread metrics collection
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

# OPTIMIZED: Memory management
_last_gc_run = 0
GC_INTERVAL = 300  # Run GC every 5 minutes


# Generic helper to run DB calls in a thread so the event loop isn't blocked
async def db_call(func, *args, **kwargs):
    return await asyncio.to_thread(functools.partial(func, *args, **kwargs))


# OPTIMIZED: Memory management helper
async def optimized_gc():
    """Run garbage collection periodically to free memory"""
    global _last_gc_run
    current_time = asyncio.get_event_loop().time()
    if current_time - _last_gc_run > GC_INTERVAL:
        collected = gc.collect()
        logger.debug(f"Garbage collection freed {collected} objects")
        _last_gc_run = current_time


# ---------- Authorization helpers ----------
async def check_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id

    # Allow if user present in DB allowed list OR configured via env lists (ALLOWED_USERS or OWNER_IDS)
    try:
        is_allowed_db = await db_call(db.is_user_allowed, user_id)
    except Exception:
        logger.exception("Error checking DB allowed users for %s", user_id)
        is_allowed_db = False

    is_allowed_env = (user_id in ALLOWED_USERS) or (user_id in OWNER_IDS)

    if not (is_allowed_db or is_allowed_env):
        if update.message:
            await update.message.reply_text(
                UNAUTHORIZED_MESSAGE,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_text(
                UNAUTHORIZED_MESSAGE,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        return False

    return True


# ---------- Simple UI handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    user = await db_call(db.get_user, user_id)

    user_name = update.effective_user.first_name or "User"
    user_phone = user["phone"] if user and user["phone"] else "Not connected"
    is_logged_in = user and user["is_logged_in"]

    status_emoji = "🟢" if is_logged_in else "🔴"
    status_text = "Online" if is_logged_in else "Offline"

    message_text = f"""
╔═══════════════════════════╗
║   📨 FORWARDER BOT 📨   ║
║  TELEGRAM MESSAGE FORWARDER  ║
╚═══════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 **User:** {user_name}
📱 **Phone:** `{user_phone}`
{status_emoji} **Status:** {status_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 **COMMANDS:**

🔐 **Account Management:**
  • /login - Connect your Telegram account
  • /logout - Disconnect your account

📨 **Forwarding Tasks:**
  • /forwadd - Add a task
  • /fortasks - List & Manage all your tasks

🆔 **Utilities:**
  • /getallid - Get all your chat IDs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ **How it works:**
1. Connect your account with /login
2. Create a forwarding task with /forwadd
3. Configure filters in /fortasks
4. Bot forwards messages based on your filters!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("📋 My Tasks", callback_data="show_tasks")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])

    await update.message.reply_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
        parse_mode="Markdown",
    )


# ---------- Fixed Button Handler ----------
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not await check_authorization(update, context):
        return

    data = query.data
    logger.info(f"Button pressed: {data}")

    try:
        if data == "login":
            await query.message.delete()
            await login_command(update, context)
        elif data == "logout":
            await query.message.delete()
            await logout_command(update, context)
        elif data == "show_tasks":
            await query.message.delete()
            await fortasks_command(update, context)
        elif data.startswith("manage_task_"):
            await manage_task_handler(update, context)
        elif data.startswith("filters_"):
            await filters_handler(update, context)
        elif data.startswith("filter_"):
            await toggle_filter_handler(update, context)
        elif data.startswith("setting_"):
            await toggle_setting_handler(update, context)
        elif data.startswith("delete_"):
            await delete_task_handler(update, context)
        elif data.startswith("prefix_"):
            await set_prefix_handler(update, context)
        elif data.startswith("suffix_"):
            await set_suffix_handler(update, context)
        elif data.startswith("chatids_"):
            user_id = query.from_user.id
            if data == "chatids_back":
                await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
            else:
                parts = data.split("_")
                category = parts[1]
                page = int(parts[2])
                await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context)
        else:
            await query.answer("❌ Unknown button action!")
    except Exception as e:
        logger.exception(f"Error in button handler for data {data}: {e}")
        await query.answer("❌ Error processing request!")


# ---------- Login/logout commands ----------
async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    # Check current user count
    if len(user_clients) >= MAX_CONCURRENT_USERS:
        await message.reply_text(
            "❌ **Server at capacity!**\n\n"
            "Too many users are currently connected. Please try again later.",
            parse_mode="Markdown",
        )
        return

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    login_states[user_id] = {"client": client, "step": "waiting_phone"}

    await message.reply_text(
        "📱 **Enter your phone number** (with country code):\n\n"
        "Example: `+1234567890`\n\n"
        "⚠️ Make sure to include the + sign!",
        parse_mode="Markdown",
    )


async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return

    if user_id not in login_states:
        return

    state = login_states[user_id]
    text = update.message.text.strip()
    client = state["client"]

    try:
        if state["step"] == "waiting_phone":
            processing_msg = await update.message.reply_text(
                "⏳ **Processing...**\n\n"
                "Requesting verification code from Telegram...",
                parse_mode="Markdown",
            )

            result = await client.send_code_request(text)
            state["phone"] = text
            state["phone_code_hash"] = result.phone_code_hash
            state["step"] = "waiting_code"

            await processing_msg.edit_text(
                "✅ **Code sent!**\n\n"
                "🔑 **Enter the verification code in this format:**\n\n"
                "`verify12345`\n\n"
                "⚠️ Type 'verify' followed immediately by your code (no spaces, no brackets).\n"
                "Example: If your code is 54321, type: `verify54321`",
                parse_mode="Markdown",
            )

        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please use this format:\n"
                    "`verify12345`\n\n"
                    "Type 'verify' followed immediately by your code.\n"
                    "Example: If your code is 54321, type: `verify54321`",
                    parse_mode="Markdown",
                )
                return

            code = text[6:]

            if not code or not code.isdigit():
                await update.message.reply_text(
                    "❌ **Invalid code!**\n\n"
                    "Please type 'verify' followed by your verification code.\n"
                    "Example: `verify12345`",
                    parse_mode="Markdown",
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying...**\n\n" "Checking your verification code...",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(state["phone"], code, phone_code_hash=state["phone_code_hash"])

                me = await client.get_me()
                session_string = client.session.save()

                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                user_clients[user_id] = client
                # ensure caches exist
                tasks_cache.setdefault(user_id, [])
                target_entity_cache.setdefault(user_id, {})
                await start_forwarding_for_user(user_id)

                del login_states[user_id]

                await verifying_msg.edit_text(
                    "✅ **Successfully connected!**\n\n"
                    f"👤 Name: {me.first_name}\n"
                    f"📱 Phone: {state['phone']}\n\n"
                    "You can now create forwarding tasks with:\n"
                    "`/forwadd`",
                    parse_mode="Markdown",
                )

            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await verifying_msg.edit_text(
                    "🔐 **2FA Password Required**\n\n"
                    "**Enter your 2-step verification password in this format:**\n\n"
                    "`passwordYourPassword123`\n\n"
                    "⚠️ Type 'password' followed immediately by your 2FA password (no spaces, no brackets).\n"
                    "Example: If your password is 'mypass123', type: `passwordmypass123`",
                    parse_mode="Markdown",
                )

        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please use this format:\n"
                    "`passwordYourPassword123`\n\n"
                    "Type 'password' followed immediately by your 2FA password.\n"
                    "Example: If your password is 'mypass123', type: `passwordmypass123`",
                    parse_mode="Markdown",
                )
                return

            password = text[8:]

            if not password:
                await update.message.reply_text(
                    "❌ **No password provided!**\n\n"
                    "Please type 'password' followed by your 2FA password.\n"
                    "Example: `passwordmypass123`",
                    parse_mode="Markdown",
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying 2FA...**\n\n" "Checking your password...", parse_mode="Markdown"
            )

            await client.sign_in(password=password)

            me = await client.get_me()
            session_string = client.session.save()

            await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

            user_clients[user_id] = client
            tasks_cache.setdefault(user_id, [])
            target_entity_cache.setdefault(user_id, {})
            await start_forwarding_for_user(user_id)

            del login_states[user_id]

            await verifying_msg.edit_text(
                "✅ **Successfully connected!**\n\n"
                f"👤 Name: {me.first_name}\n"
                f"📱 Phone: {state['phone']}\n\n"
                "You can now create forwarding tasks!",
                parse_mode="Markdown",
            )

    except Exception as e:
        logger.exception("Error during login process for %s", user_id)
        await update.message.reply_text(
            f"❌ **Error:** {str(e)}\n\n" "Please try /login again.",
            parse_mode="Markdown",
        )
        if user_id in login_states:
            try:
                # ensure Telethon client disconnected if login failed
                c = login_states[user_id].get("client")
                if c:
                    await c.disconnect()
            except Exception:
                logger.exception("Error disconnecting client after failed login for %s", user_id)
            del login_states[user_id]


async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await message.reply_text(
            "❌ **You're not connected!**\n\n" "Use /login to connect your account.", parse_mode="Markdown"
        )
        return

    logout_states[user_id] = {"phone": user["phone"]}

    await message.reply_text(
        "⚠️ **Confirm Logout**\n\n"
        f"📱 **Enter your phone number to confirm disconnection:**\n\n"
        f"Your connected phone: `{user['phone']}`\n\n"
        "Type your phone number exactly to confirm logout.",
        parse_mode="Markdown",
    )


async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id

    if user_id not in logout_states:
        return False

    text = update.message.text.strip()
    stored_phone = logout_states[user_id]["phone"]

    if text != stored_phone:
        await update.message.reply_text(
            "❌ **Phone number doesn't match!**\n\n"
            f"Expected: `{stored_phone}`\n"
            f"You entered: `{text}`\n\n"
            "Please try again or use /start to cancel.",
            parse_mode="Markdown",
        )
        return True

    # Disconnect client's telethon session (if present)
    if user_id in user_clients:
        client = user_clients[user_id]
        try:
            # remove handler if present
            handler = handler_registered.get(user_id)
            if handler:
                try:
                    client.remove_event_handler(handler)
                except Exception:
                    logger.exception("Error removing event handler during logout for user %s", user_id)
                handler_registered.pop(user_id, None)

            await client.disconnect()
        except Exception:
            logger.exception("Error disconnecting client for user %s", user_id)
        finally:
            user_clients.pop(user_id, None)

    # mark as logged out in DB
    try:
        await db_call(db.save_user, user_id, None, None, None, False)
    except Exception:
        logger.exception("Error saving user logout state for %s", user_id)
    # clear caches for this user
    tasks_cache.pop(user_id, None)
    target_entity_cache.pop(user_id, None)
    logout_states.pop(user_id, None)

    await update.message.reply_text(
        "👋 **Account disconnected successfully!**\n\n"
        "✅ All your forwarding tasks have been stopped.\n"
        "🔄 Use /login to connect again.",
        parse_mode="Markdown",
    )
    return True


# ---------- Task creation with conversation handler ----------
async def forwadd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return ConversationHandler.END

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\n" "Use /login to connect your Telegram account.", parse_mode="Markdown"
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "📝 **Let's create a new forwarding task!**\n\n"
        "🔤 **Please enter a name for your task:**\n\n"
        "💡 *Example: `my_forward_task`*",
        parse_mode="Markdown"
    )
    return TASK_NAME


async def task_name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    task_name = update.message.text.strip()
    context.user_data['task_name'] = task_name
    
    await update.message.reply_text(
        "📥 **Great! Now enter the source chat ID(s):**\n\n"
        "🔢 **Multiple IDs should be separated by commas**\n\n"
        "💡 *Example: `-100123456789, -100987654321`*",
        parse_mode="Markdown"
    )
    return SOURCE_IDS


async def source_ids_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        source_ids = [int(x.strip()) for x in update.message.text.split(',')]
        context.user_data['source_ids'] = source_ids
        
        await update.message.reply_text(
            "📤 **Perfect! Now enter the target chat ID(s):**\n\n"
            "🔢 **Multiple IDs should be separated by commas**\n\n"
            "💡 *Example: `-100555666777, -100888999000`*",
            parse_mode="Markdown"
        )
        return TARGET_IDS
    except ValueError:
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n"
            "Please enter only numbers separated by commas.\n\n"
            "💡 *Example: `-100123456789, -100987654321`*",
            parse_mode="Markdown"
        )
        return SOURCE_IDS


async def target_ids_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    try:
        target_ids = [int(x.strip()) for x in update.message.text.split(',')]
        task_name = context.user_data['task_name']
        source_ids = context.user_data['source_ids']
        
        task_id = await db_call(db.add_forwarding_task, user_id, task_name, source_ids, target_ids)
        if task_id:
            # Update in-memory cache
            tasks_cache.setdefault(user_id, [])
            tasks_cache[user_id].append({
                "id": task_id, 
                "label": task_name, 
                "source_ids": source_ids, 
                "target_ids": target_ids, 
                "is_active": 1
            })
            
            await update.message.reply_text(
                f"✅ **Task '{task_name}' created successfully!** 🎉\n\n"
                f"📥 **Sources:** {', '.join(map(str, source_ids))}\n"
                f"📤 **Targets:** {', '.join(map(str, target_ids))}\n\n"
                "⚙️ **Use** `/fortasks` **to manage your tasks and configure filters!**",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                f"❌ **Task '{task_name}' already exists!**\n\n"
                "Please choose a different name.",
                parse_mode="Markdown"
            )
    except ValueError:
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n"
            "Please enter only numbers separated by commas.\n\n"
            "💡 *Example: `-100555666777, -100888999000`*",
            parse_mode="Markdown"
        )
        return TARGET_IDS
    
    return ConversationHandler.END


async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "❌ **Task creation cancelled.**",
        parse_mode="Markdown"
    )
    return ConversationHandler.END


# ---------- Task management with instant updates ----------
async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    # Refresh tasks from database
    try:
        user_tasks = await db_call(db.get_user_tasks, user_id)
        tasks_cache[user_id] = user_tasks
    except Exception as e:
        logger.exception("Error refreshing tasks from DB for user %s", user_id)
        user_tasks = tasks_cache.get(user_id) or []

    if not user_tasks:
        if update.message:
            await update.message.reply_text(
                "📋 **No Active Tasks**\n\n" 
                "You don't have any forwarding tasks yet.\n\n" 
                "Create one with:\n" 
                "`/forwadd`",
                parse_mode="Markdown",
            )
        else:
            await update.callback_query.message.edit_text(
                "📋 **No Active Tasks**\n\n" 
                "You don't have any forwarding tasks yet.\n\n" 
                "Create one with:\n" 
                "`/forwadd`",
                parse_mode="Markdown",
            )
        return

    task_list = "📋 **Your Forwarding Tasks**\n\n"
    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    keyboard = []
    for task in user_tasks:
        task_list += f"• **{task['label']}**\n"
        task_list += f"  📥 Sources: {', '.join(map(str, task['source_ids']))}\n"
        task_list += f"  📤 Targets: {', '.join(map(str, task['target_ids']))}\n\n"
        
        keyboard.append([InlineKeyboardButton(f"⚙️ {task['label']}", callback_data=f"manage_task_{task['id']}")])

    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    task_list += f"Total: **{len(user_tasks)} task(s)**\n\n"
    task_list += "💡 **Click a task below to manage it!**"

    if update.message:
        await update.message.reply_text(
            task_list, 
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.callback_query.message.edit_text(
            task_list,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


async def manage_task_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    task_id = int(query.data.split('_')[2])
    
    # Get task details
    task = await get_task_by_id(task_id)
    if not task:
        await query.message.edit_text("❌ Task not found!")
        return
    
    settings = await db_call(db.get_task_settings, task_id)
    
    outgoing_emoji = "✅" if settings["outgoing_enabled"] else "❌"
    forward_tag_emoji = "✅" if settings["forward_tag_enabled"] else "❌"
    control_emoji = "✅" if settings["control_enabled"] else "❌"
    
    message_text = f"""
🔧 **Managing Task: {task['label']}**

📊 **Current Settings:**
{outgoing_emoji} Outgoing: {'On' if settings['outgoing_enabled'] else 'Off'}
{forward_tag_emoji} Forward Tag: {'On' if settings['forward_tag_enabled'] else 'Off'}  
{control_emoji} Control: {'On' if settings['control_enabled'] else 'Off'}

💡 **Choose an option to configure:**
"""
    
    keyboard = [
        [InlineKeyboardButton("🔍 Filters", callback_data=f"filters_{task_id}")],
        [InlineKeyboardButton(f"{outgoing_emoji} Outgoing", callback_data=f"setting_outgoing_{task_id}")],
        [InlineKeyboardButton(f"{forward_tag_emoji} Forward Tag", callback_data=f"setting_forward_tag_{task_id}")],
        [InlineKeyboardButton(f"{control_emoji} Control", callback_data=f"setting_control_{task_id}")],
        [InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{task_id}")],
        [InlineKeyboardButton("🔙 Back to Tasks", callback_data="show_tasks")]
    ]
    
    await query.message.edit_text(
        message_text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def filters_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    task_id = int(query.data.split('_')[1])
    
    # Build filters menu with current state
    message_text, keyboard = await build_filters_menu(task_id)
    
    await query.message.edit_text(
        message_text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def build_filters_menu(task_id: int):
    """Build the filters menu with current state"""
    filters_list = await db_call(db.get_task_filters, task_id)
    filters_dict = {f["filter_type"]: f for f in filters_list}
    
    raw_emoji = "✅" if filters_dict.get("raw_text", {}).get("is_active") else "❌"
    numbers_emoji = "✅" if filters_dict.get("numbers_only", {}).get("is_active") else "❌"
    alphabets_emoji = "✅" if filters_dict.get("alphabets_only", {}).get("is_active") else "❌"
    removed_numbers_emoji = "✅" if filters_dict.get("removed_numbers", {}).get("is_active") else "❌"
    removed_alphabets_emoji = "✅" if filters_dict.get("removed_alphabets", {}).get("is_active") else "❌"
    
    prefix_value = filters_dict.get("prefix", {}).get("value", "Not set")
    suffix_value = filters_dict.get("suffix", {}).get("value", "Not set")
    prefix_emoji = "✅" if filters_dict.get("prefix", {}).get("is_active") else "❌"
    suffix_emoji = "✅" if filters_dict.get("suffix", {}).get("is_active") else "❌"
    
    message_text = f"""
🔍 **Filters for Task**

📊 **Active Filters:**
{raw_emoji} Raw Text
{numbers_emoji} Numbers Only  
{alphabets_emoji} Alphabets Only
{removed_numbers_emoji} Removed Numbers Only
{removed_alphabets_emoji} Removed Alphabets Only
{prefix_emoji} Prefix: {prefix_value}
{suffix_emoji} Suffix: {suffix_value}

💡 **Multiple filters can be active at once!**
"""
    
    keyboard = [
        [InlineKeyboardButton(f"{raw_emoji} Raw Text", callback_data=f"filter_raw_text_{task_id}")],
        [InlineKeyboardButton(f"{numbers_emoji} Numbers Only", callback_data=f"filter_numbers_only_{task_id}")],
        [InlineKeyboardButton(f"{alphabets_emoji} Alphabets Only", callback_data=f"filter_alphabets_only_{task_id}")],
        [InlineKeyboardButton(f"{removed_numbers_emoji} Removed Numbers", callback_data=f"filter_removed_numbers_{task_id}")],
        [InlineKeyboardButton(f"{removed_alphabets_emoji} Removed Alphabets", callback_data=f"filter_removed_alphabets_{task_id}")],
        [InlineKeyboardButton(f"{prefix_emoji} Add Prefix", callback_data=f"prefix_{task_id}")],
        [InlineKeyboardButton(f"{suffix_emoji} Add Suffix", callback_data=f"suffix_{task_id}")],
        [InlineKeyboardButton("🔙 Back to Task", callback_data=f"manage_task_{task_id}")]
    ]
    
    return message_text, keyboard


async def toggle_filter_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split('_')
    filter_type = parts[1] + "_" + parts[2]  # e.g., "raw_text", "numbers_only"
    task_id = int(parts[3])
    
    # Get current filter state
    filters = await db_call(db.get_task_filters, task_id)
    current_state = any(f["filter_type"] == filter_type and f["is_active"] for f in filters)
    
    # Toggle the state
    await db_call(db.update_task_filter, task_id, filter_type, None, not current_state)
    
    # Show confirmation
    status = "enabled" if not current_state else "disabled"
    filter_name = filter_type.replace('_', ' ').title()
    await query.answer(f"✅ {filter_name} {status}!")
    
    # Update the current message instantly
    message_text, keyboard = await build_filters_menu(task_id)
    await query.message.edit_text(
        message_text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def toggle_setting_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split('_')
    setting_type = parts[1]  # e.g., "outgoing", "forward_tag", "control"
    task_id = int(parts[2])
    
    # Get current settings
    settings = await db_call(db.get_task_settings, task_id)
    current_value = settings.get(f"{setting_type}_enabled", True)
    
    # Toggle the setting
    await db_call(db.update_task_setting, task_id, f"{setting_type}_enabled", not current_value)
    
    # Show confirmation
    setting_name = setting_type.replace('_', ' ').title()
    status = "enabled" if not current_value else "disabled"
    await query.answer(f"✅ {setting_name} {status}!")
    
    # Refresh the task management menu
    await manage_task_handler(update, context)


# ---------- Fixed Delete Functionality ----------
async def delete_task_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    task_id = int(query.data.split('_')[1])
    
    # Get task details for confirmation
    task = await get_task_by_id(task_id)
    if not task:
        await query.message.edit_text("❌ Task not found!")
        return
    
    context.user_data['delete_task_id'] = task_id
    context.user_data['delete_task_name'] = task['label']
    
    await query.message.edit_text(
        f"🗑️ **Delete Task: {task['label']}**\n\n"
        f"⚠️ **This action cannot be undone!**\n\n"
        f"📝 **Please type the task name to confirm deletion:**\n\n"
        f"`{task['label']}`\n\n"
        "Type /cancel to cancel.",
        parse_mode="Markdown"
    )


async def confirm_delete_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    if 'delete_task_id' not in context.user_data:
        await update.message.reply_text("❌ No pending deletion!")
        return
    
    task_id = context.user_data['delete_task_id']
    task_name = context.user_data['delete_task_name']
    
    if text != task_name:
        await update.message.reply_text(
            f"❌ **Task name doesn't match!**\n\n"
            f"Expected: `{task_name}`\n"
            f"You entered: `{text}`\n\n"
            "Please try again or type /cancel to cancel.",
            parse_mode="Markdown"
        )
        return
    
    # Delete the task using task_id instead of label
    deleted = await db_call(db.remove_forwarding_task_by_id, task_id)
    if deleted:
        # Update cache
        if user_id in tasks_cache:
            tasks_cache[user_id] = [t for t in tasks_cache[user_id] if t.get('id') != task_id]
        
        # Send success confirmation
        await update.message.reply_text(
            f"✅ **Task '{task_name}' deleted successfully!**\n\n"
            f"🗑️ *The task has been completely removed from your list.*",
            parse_mode="Markdown"
        )
        
        # Go back to tasks list
        await fortasks_command(update, context)
    else:
        await update.message.reply_text(
            f"❌ **Failed to delete task '{task_name}'!**\n\n"
            "Please try again.",
            parse_mode="Markdown"
        )
    
    # Clean up context
    for key in ['delete_task_id', 'delete_task_name']:
        if key in context.user_data:
            del context.user_data[key]


# ---------- Fixed Prefix/Suffix Functionality ----------
async def set_prefix_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    task_id = int(query.data.split('_')[1])
    context.user_data['prefix_task_id'] = task_id
    
    await query.message.edit_text(
        "🔤 **Please enter the prefix text:**\n\n"
        "💡 *This will be added before each forwarded message*\n\n"
        "Type /cancel to cancel.",
        parse_mode="Markdown"
    )
    return PREFIX_SUFFIX


async def set_suffix_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    task_id = int(query.data.split('_')[1])
    context.user_data['suffix_task_id'] = task_id
    
    await query.message.edit_text(
        "🔤 **Please enter the suffix text:**\n\n"
        "💡 *This will be added after each forwarded message*\n\n"
        "Type /cancel to cancel.",
        parse_mode="Markdown"
    )
    return PREFIX_SUFFIX


async def prefix_suffix_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    if 'prefix_task_id' in context.user_data:
        task_id = context.user_data['prefix_task_id']
        await db_call(db.update_task_filter, task_id, "prefix", text, True)
        
        # Send confirmation message
        await update.message.reply_text(
            f"✅ **Prefix added successfully!**\n\n"
            f"🔤 **Your prefix:** `{text}`\n\n"
            f"💡 *This will be added before each forwarded message*",
            parse_mode="Markdown"
        )
        
        # Go back to filters menu
        message_text, keyboard = await build_filters_menu(task_id)
        await update.message.reply_text(
            message_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        del context.user_data['prefix_task_id']
    
    elif 'suffix_task_id' in context.user_data:
        task_id = context.user_data['suffix_task_id']
        await db_call(db.update_task_filter, task_id, "suffix", text, True)
        
        # Send confirmation message
        await update.message.reply_text(
            f"✅ **Suffix added successfully!**\n\n"
            f"🔤 **Your suffix:** `{text}`\n\n"
            f"💡 *This will be added after each forwarded message*",
            parse_mode="Markdown"
        )
        
        # Go back to filters menu
        message_text, keyboard = await build_filters_menu(task_id)
        await update.message.reply_text(
            message_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        del context.user_data['suffix_task_id']
    
    return ConversationHandler.END


# Helper function to get task by ID
async def get_task_by_id(task_id: int) -> Optional[Dict]:
    for user_id, tasks in tasks_cache.items():
        for task in tasks:
            if task.get('id') == task_id:
                return task
    return None


# ---------- Admin commands ----------
async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: add a user (optionally as admin)."""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
        return

    text = update.message.text.strip()
    parts = text.split()

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n"
            "**Usage:**\n"
            "`/adduser [USER_ID]` - Add regular user\n"
            "`/adduser [USER_ID] admin` - Add admin user",
            parse_mode="Markdown",
        )
        return

    try:
        new_user_id = int(parts[1])
        is_admin = len(parts) > 2 and parts[2].lower() == "admin"

        added = await db_call(db.add_allowed_user, new_user_id, None, is_admin, user_id)
        if added:
            role = "👑 Admin" if is_admin else "👤 User"
            await update.message.reply_text(
                f"✅ **User added!**\n\nID: `{new_user_id}`\nRole: {role}",
                parse_mode="Markdown",
            )
            # notify the added user (best-effort)
            try:
                await context.bot.send_message(new_user_id, "✅ You have been added. Send /start to begin.", parse_mode="Markdown")
            except Exception:
                logger.exception("Could not notify new allowed user %s", new_user_id)
        else:
            await update.message.reply_text(f"❌ **User `{new_user_id}` already exists!**", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ **Invalid user ID!**\n\nUser ID must be a number.", parse_mode="Markdown")


async def removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: remove a user and stop their forwarding permanently in this process."""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
        return

    text = update.message.text.strip()
    parts = text.split()

    if len(parts) < 2:
        await update.message.reply_text("❌ **Invalid format!**\n\n**Usage:** `/removeuser [USER_ID]`", parse_mode="Markdown")
        return

    try:
        remove_user_id = int(parts[1])

        removed = await db_call(db.remove_allowed_user, remove_user_id)
        if removed:
            # Best-effort: disconnect user's Telethon client and clear in-memory state
            if remove_user_id in user_clients:
                try:
                    client = user_clients[remove_user_id]
                    # remove event handler if present
                    handler = handler_registered.get(remove_user_id)
                    if handler:
                        try:
                            client.remove_event_handler(handler)
                        except Exception:
                            logger.exception("Error removing event handler for removed user %s", remove_user_id)
                        handler_registered.pop(remove_user_id, None)

                    await client.disconnect()
                except Exception:
                    logger.exception("Error disconnecting client for removed user %s", remove_user_id)
                finally:
                    user_clients.pop(remove_user_id, None)

            # mark as logged out in users table and clear caches
            try:
                await db_call(db.save_user, remove_user_id, None, None, None, False)
            except Exception:
                logger.exception("Error saving user logged_out state for %s", remove_user_id)

            tasks_cache.pop(remove_user_id, None)
            target_entity_cache.pop(remove_user_id, None)
            handler_registered.pop(remove_user_id, None)

            await update.message.reply_text(f"✅ **User `{remove_user_id}` removed!**", parse_mode="Markdown")

            # Notify removed user (best-effort)
            try:
                await context.bot.send_message(remove_user_id, "❌ You have been removed. Contact the owner to regain access.", parse_mode="Markdown")
            except Exception:
                logger.exception("Could not notify removed user %s", remove_user_id)
        else:
            await update.message.reply_text(f"❌ **User `{remove_user_id}` not found!**", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ **Invalid user ID!**\n\nUser ID must be a number.", parse_mode="Markdown")


async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: list allowed users."""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
        return

    users = await db_call(db.get_all_allowed_users)

    if not users:
        await update.message.reply_text("📋 **No Allowed Users**\n\nThe allowed users list is empty.", parse_mode="Markdown")
        return

    user_list = "👥 **Allowed Users**\n\n"
    user_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, user in enumerate(users, 1):
        role_emoji = "👑" if user["is_admin"] else "👤"
        role_text = "Admin" if user["is_admin"] else "User"
        username = user["username"] if user["username"] else "Unknown"

        user_list += f"{i}. {role_emoji} **{role_text}**\n"
        user_list += f"   ID: `{user['user_id']}`\n"
        if user["username"]:
            user_list += f"   Username: {username}\n"
        user_list += "\n"

    user_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    user_list += f"Total: **{len(users)} user(s)**"

    await update.message.reply_text(user_list, parse_mode="Markdown")


# ---------- Chat listing functions ----------
async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ **You need to connect your account first!**\n\n" "Use /login to connect.", parse_mode="Markdown")
        return

    await update.message.reply_text("🔄 **Fetching your chats...**")

    await show_chat_categories(user_id, update.message.chat.id, None, context)


async def show_chat_categories(user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
    if user_id not in user_clients:
        return

    message_text = (
        "🗂️ **Chat ID Categories**\n\n"
        "📋 Choose which type of chat IDs you want to see:\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🤖 **Bots** - Bot accounts\n"
        "📢 **Channels** - Broadcast channels\n"
        "👥 **Groups** - Group chats\n"
        "👤 **Private** - Private conversations\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💡 Select a category below:"
    )

    keyboard = [
        [InlineKeyboardButton("🤖 Bots", callback_data="chatids_bots_0"), InlineKeyboardButton("📢 Channels", callback_data="chatids_channels_0")],
        [InlineKeyboardButton("👥 Groups", callback_data="chatids_groups_0"), InlineKeyboardButton("👤 Private", callback_data="chatids_private_0")],
    ]

    if message_id:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_categorized_chats(user_id: int, chat_id: int, message_id: int, category: str, page: int, context: ContextTypes.DEFAULT_TYPE):
    from telethon.tl.types import User, Channel, Chat

    if user_id not in user_clients:
        return

    client = user_clients[user_id]

    categorized_dialogs = []
    async for dialog in client.iter_dialogs():
        entity = dialog.entity

        if category == "bots":
            if isinstance(entity, User) and entity.bot:
                categorized_dialogs.append(dialog)
        elif category == "channels":
            if isinstance(entity, Channel) and getattr(entity, "broadcast", False):
                categorized_dialogs.append(dialog)
        elif category == "groups":
            if isinstance(entity, (Channel, Chat)) and not (isinstance(entity, Channel) and getattr(entity, "broadcast", False)):
                categorized_dialogs.append(dialog)
        elif category == "private":
            if isinstance(entity, User) and not entity.bot:
                categorized_dialogs.append(dialog)

    PAGE_SIZE = 10
    total_pages = max(1, (len(categorized_dialogs) + PAGE_SIZE - 1) // PAGE_SIZE)
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_dialogs = categorized_dialogs[start:end]

    category_emoji = {"bots": "🤖", "channels": "📢", "groups": "👥", "private": "👤"}
    category_name = {"bots": "Bots", "channels": "Channels", "groups": "Groups", "private": "Private Chats"}

    emoji = category_emoji.get(category, "💬")
    name = category_name.get(category, "Chats")

    if not categorized_dialogs:
        chat_list = f"{emoji} **{name}**\n\n"
        chat_list += f"📭 **No {name.lower()} found!**\n\n"
        chat_list += "Try another category."
    else:
        chat_list = f"{emoji} **{name}** (Page {page + 1}/{total_pages})\n\n"
        chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        for i, dialog in enumerate(page_dialogs, start + 1):
            chat_name = dialog.name[:30] if dialog.name else "Unknown"
            chat_list += f"{i}. **{chat_name}**\n"
            chat_list += f"   🆔 `{dialog.id}`\n\n"

        chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        chat_list += f"📊 Total: {len(categorized_dialogs)} {name.lower()}\n"
        chat_list += "💡 Tap to copy the ID!"

    keyboard = []

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"chatids_{category}_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"chatids_{category}_{page + 1}"))

    if nav_row:
        keyboard.append(nav_row)

    keyboard.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="chatids_back")])

    await context.bot.edit_message_text(chat_list, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# ---------- Fixed Forwarding Core with Forward Tag Support ----------
def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    """Attach a NewMessage handler once per client/user to avoid duplicates and store the handler (so it can be removed)."""
    if handler_registered.get(user_id):
        return

    async def _hot_message_handler(event):
        try:
            await optimized_gc()
            
            message_text = getattr(event, "raw_text", None) or getattr(getattr(event, "message", None), "message", None)
            if not message_text:
                return

            chat_id = getattr(event, "chat_id", None) or getattr(getattr(event, "message", None), "chat_id", None)
            if chat_id is None:
                return

            user_id = None
            for uid, client_obj in user_clients.items():
                if client_obj == event.client:
                    user_id = uid
                    break
            
            if user_id is None:
                return

            user_tasks = tasks_cache.get(user_id)
            if not user_tasks:
                return

            for task in user_tasks:
                if chat_id in task.get("source_ids", []):
                    # Check task settings
                    settings = await db_call(db.get_task_settings, task['id'])
                    if not settings['control_enabled'] or not settings['outgoing_enabled']:
                        continue
                    
                    # Get filters for this task
                    filters = await db_call(db.get_task_filters, task['id'])
                    active_filters = [f for f in filters if f['is_active']]
                    
                    # Apply filters
                    if not await apply_filters(message_text, active_filters):
                        continue
                    
                    # Apply prefix/suffix
                    final_text = await apply_prefix_suffix(message_text, active_filters)
                    
                    for target_id in task.get("target_ids", []):
                        try:
                            global send_queue
                            if send_queue is None:
                                logger.debug("Send queue not initialized; dropping forward job")
                                continue
                            await send_queue.put((user_id, event.client, int(target_id), final_text, settings['forward_tag_enabled']))
                        except asyncio.QueueFull:
                            logger.warning("Send queue full, dropping forward job for user=%s target=%s", user_id, target_id)
        except Exception:
            logger.exception("Error in hot message handler for user %s", user_id)

    try:
        client.add_event_handler(_hot_message_handler, events.NewMessage())
        handler_registered[user_id] = _hot_message_handler
        logger.info("Registered NewMessage handler for user %s", user_id)
    except Exception:
        logger.exception("Failed to add event handler for user %s", user_id)


async def apply_filters(message_text: str, filters: List[Dict]) -> bool:
    """Apply filters to message text, return True if message should be forwarded"""
    if not filters:
        return True  # No filters active, forward everything
    
    text = message_text.strip()
    
    # Check if raw_text filter is active (allows all messages)
    raw_text_active = any(f["filter_type"] == "raw_text" and f["is_active"] for f in filters)
    if raw_text_active:
        return True
    
    for filter_obj in filters:
        filter_type = filter_obj['filter_type']
        
        if filter_type == 'numbers_only' and filter_obj['is_active']:
            if text.isdigit():
                return True
                
        elif filter_type == 'alphabets_only' and filter_obj['is_active']:
            if text.isalpha():
                return True
                
        elif filter_type == 'removed_numbers' and filter_obj['is_active']:
            # Remove numbers and check if there's text left
            cleaned = re.sub(r'\d+', '', text)
            if cleaned.strip():
                return True
                
        elif filter_type == 'removed_alphabets' and filter_obj['is_active']:
            # Remove alphabets and check if there's text left
            cleaned = re.sub(r'[a-zA-Z]+', '', text)
            if cleaned.strip():
                return True
    
    return False


async def apply_prefix_suffix(message_text: str, filters: List[Dict]) -> str:
    """Apply prefix and suffix to message text"""
    text = message_text
    prefix = ""
    suffix = ""
    
    for filter_obj in filters:
        if filter_obj['filter_type'] == 'prefix' and filter_obj['is_active']:
            prefix = filter_obj.get('value', '')
        elif filter_obj['filter_type'] == 'suffix' and filter_obj['is_active']:
            suffix = filter_obj.get('value', '')
    
    return f"{prefix}{text}{suffix}"


async def resolve_target_entity_once(user_id: int, client: TelegramClient, target_id: int) -> Optional[object]:
    """Try to resolve a target entity and cache it. Returns entity or None."""
    if user_id not in target_entity_cache:
        target_entity_cache[user_id] = {}

    if target_id in target_entity_cache[user_id]:
        return target_entity_cache[user_id][target_id]

    try:
        entity = await client.get_input_entity(int(target_id))
        target_entity_cache[user_id][target_id] = entity
        return entity
    except Exception:
        logger.debug("Could not resolve target %s for user %s now", target_id, user_id)
        return None


async def resolve_targets_for_user(user_id: int, target_ids: List[int]):
    """Background resolver that attempts to resolve targets for a user."""
    client = user_clients.get(user_id)
    if not client:
        return
    for tid in target_ids:
        for attempt in range(3):
            ent = await resolve_target_entity_once(user_id, client, tid)
            if ent:
                logger.info("Resolved target %s for user %s", tid, user_id)
                break
            await asyncio.sleep(TARGET_RESOLVE_RETRY_SECONDS)


async def send_worker_loop(worker_id: int):
    """OPTIMIZED Worker that consumes send_queue and performs client.send_message with forward tag support."""
    logger.info("Send worker %d started", worker_id)
    global send_queue
    if send_queue is None:
        logger.error("send_worker_loop started before send_queue initialized")
        return

    while True:
        try:
            user_id, client, target_id, message_text, forward_tag_enabled = await send_queue.get()
        except asyncio.CancelledError:
            # Worker cancelled during shutdown
            break
        except Exception:
            # if loop closed or other error
            logger.exception("Error getting item from send_queue in worker %d", worker_id)
            break

        try:
            entity = None
            if user_id in target_entity_cache:
                entity = target_entity_cache[user_id].get(target_id)
            if not entity:
                entity = await resolve_target_entity_once(user_id, client, target_id)
            if not entity:
                logger.debug("Skipping send: target %s unresolved for user %s", target_id, user_id)
                continue

            try:
                if forward_tag_enabled:
                    # Forward with tag
                    await client.forward_messages(entity, int(target_id), message_text)
                else:
                    # Send without forward tag
                    await client.send_message(entity, message_text)
                logger.debug("Forwarded message for user %s to %s (forward_tag: %s)", user_id, target_id, forward_tag_enabled)
            except FloodWaitError as fwe:
                wait = int(getattr(fwe, "seconds", 10))
                logger.warning("FloodWait for %s seconds. Pausing worker %d", wait, worker_id)
                await asyncio.sleep(wait + 1)
                try:
                    await send_queue.put((user_id, client, target_id, message_text, forward_tag_enabled))
                except asyncio.QueueFull:
                    logger.warning("Send queue full while re-enqueueing after FloodWait; dropping message.")
            except Exception as e:
                logger.exception("Error sending message for user %s to %s: %s", user_id, target_id, e)

        except Exception:
            logger.exception("Unexpected error in send worker %d", worker_id)
        finally:
            try:
                send_queue.task_done()
            except Exception:
                # If queue or loop closed, ignore
                pass


async def start_send_workers():
    global _send_workers_started, send_queue, worker_tasks
    if _send_workers_started:
        return

    # create queue on the current running loop (safe)
    if send_queue is None:
        send_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)

    for i in range(SEND_WORKER_COUNT):
        t = asyncio.create_task(send_worker_loop(i + 1))
        worker_tasks.append(t)

    _send_workers_started = True
    logger.info("Spawned %d send workers", SEND_WORKER_COUNT)


async def start_forwarding_for_user(user_id: int):
    """Ensure client exists, register handler (once), and ensure caches created."""
    if user_id not in user_clients:
        return

    client = user_clients[user_id]
    tasks_cache.setdefault(user_id, [])
    target_entity_cache.setdefault(user_id, {})

    ensure_handler_registered_for_user(user_id, client)


# ---------- Session restore and initialization ----------
async def restore_sessions():
    logger.info("🔄 Restoring sessions...")

    # fetch logged-in users in a thread to avoid blocking the event loop
    def _fetch_logged_in_users():
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT user_id, session_data FROM users WHERE is_logged_in = 1")
        return cur.fetchall()

    try:
        users = await asyncio.to_thread(_fetch_logged_in_users)
    except Exception:
        logger.exception("Error fetching logged-in users from DB")
        users = []

    # Preload tasks cache from DB (single DB call off the loop)
    try:
        all_active = await db_call(db.get_all_active_tasks)
    except Exception:
        logger.exception("Error fetching active tasks from DB")
        all_active = []

    tasks_cache.clear()
    for t in all_active:
        uid = t["user_id"]
        tasks_cache.setdefault(uid, [])
        tasks_cache[uid].append({"id": t["id"], "label": t["label"], "source_ids": t["source_ids"], "target_ids": t["target_ids"], "is_active": 1})

    logger.info("📊 Found %d logged in user(s)", len(users))

    # Restore sessions in batches to avoid memory spikes
    batch_size = 5
    for i in range(0, len(users), batch_size):
        batch = users[i:i + batch_size]
        restore_tasks = []
        
        for row in batch:
            # row may be sqlite3.Row or tuple
            try:
                user_id = row["user_id"] if isinstance(row, dict) or hasattr(row, "keys") else row[0]
                session_data = row["session_data"] if isinstance(row, dict) or hasattr(row, "keys") else row[1]
            except Exception:
                try:
                    user_id, session_data = row[0], row[1]
                except Exception:
                    continue

            if session_data:
                restore_tasks.append(restore_single_session(user_id, session_data))
        
        if restore_tasks:
            await asyncio.gather(*restore_tasks, return_exceptions=True)
            await asyncio.sleep(1)  # Small delay between batches


async def restore_single_session(user_id: int, session_data: str):
    """Restore a single user session with error handling"""
    try:
        client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
        await client.connect()

        if await client.is_user_authorized():
            user_clients[user_id] = client
            target_entity_cache.setdefault(user_id, {})
            # Try to resolve all targets for this user's tasks in background
            user_tasks = tasks_cache.get(user_id, [])
            all_targets = []
            for tt in user_tasks:
                all_targets.extend(tt.get("target_ids", []))
            if all_targets:
                try:
                    asyncio.create_task(resolve_targets_for_user(user_id, list(set(all_targets))))
                except Exception:
                    logger.exception("Failed to schedule resolve_targets_for_user on restore for %s", user_id)
            await start_forwarding_for_user(user_id)
            logger.info("✅ Restored session for user %s", user_id)
        else:
            await db_call(db.save_user, user_id, None, None, None, False)
            logger.warning("⚠️ Session expired for user %s", user_id)
    except Exception as e:
        logger.exception("❌ Failed to restore session for user %s: %s", user_id, e)
        try:
            await db_call(db.save_user, user_id, None, None, None, False)
        except Exception:
            logger.exception("Error marking user logged out after failed restore for %s", user_id)


# ---------- Graceful shutdown cleanup ----------
async def shutdown_cleanup():
    """Disconnect Telethon clients and cancel worker tasks cleanly."""
    logger.info("Shutdown cleanup: cancelling worker tasks and disconnecting clients...")

    # cancel worker tasks
    for t in list(worker_tasks):
        try:
            t.cancel()
        except Exception:
            logger.exception("Error cancelling worker task")
    if worker_tasks:
        # wait for tasks to finish cancellation
        try:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        except Exception:
            logger.exception("Error while awaiting worker task cancellations")

    # disconnect telethon clients in batches
    user_ids = list(user_clients.keys())
    batch_size = 5
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        disconnect_tasks = []
        for uid in batch:
            client = user_clients.get(uid)
            if client:
                # remove handler if present
                handler = handler_registered.get(uid)
                if handler:
                    try:
                        client.remove_event_handler(handler)
                    except Exception:
                        logger.exception("Error removing event handler during shutdown for user %s", uid)
                    handler_registered.pop(uid, None)

                disconnect_tasks.append(client.disconnect())
        
        if disconnect_tasks:
            await asyncio.gather(*disconnect_tasks, return_exceptions=True)
    
    user_clients.clear()

    # close DB connection if needed
    try:
        db.close_connection()
    except Exception:
        logger.exception("Error closing DB connection during shutdown")

    logger.info("Shutdown cleanup complete.")


# ---------- Application post_init: start send workers and restore sessions ----------
async def post_init(application: Application):
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()

    logger.info("🔧 Initializing bot...")

    await application.bot.delete_webhook(drop_pending_updates=True)
    logger.info("🧹 Cleared webhooks")

    # Ensure configured OWNER_IDS are present in DB as admin users
    if OWNER_IDS:
        for oid in OWNER_IDS:
            try:
                is_admin = await db_call(db.is_user_admin, oid)
                if not is_admin:
                    await db_call(db.add_allowed_user, oid, None, True, None)
                    logger.info("✅ Added owner/admin from env: %s", oid)
            except Exception:
                logger.exception("Error adding owner/admin %s from env", oid)

    # Ensure configured ALLOWED_USERS are present in DB as allowed users (non-admin)
    if ALLOWED_USERS:
        for au in ALLOWED_USERS:
            try:
                await db_call(db.add_allowed_user, au, None, False, None)
                logger.info("✅ Added allowed user from env: %s", au)
            except Exception:
                logger.exception("Error adding allowed user %s from env: %s", au)

    # start send workers and restore sessions
    await start_send_workers()
    await restore_sessions()

    # register a monitoring callback with the webserver (best-effort, thread-safe)
    async def _collect_metrics():
        """
        Run inside the bot event loop to safely access asyncio objects and in-memory state.
        """
        try:
            q = None
            try:
                q = send_queue.qsize() if send_queue is not None else None
            except Exception:
                q = None
            return {
                "send_queue_size": q,
                "worker_count": len(worker_tasks),
                "active_user_clients_count": len(user_clients),
                "tasks_cache_counts": {uid: len(tasks_cache.get(uid, [])) for uid in list(tasks_cache.keys())},
                "memory_usage_mb": _get_memory_usage_mb(),
            }
        except Exception as e:
            return {"error": f"failed to collect metrics in loop: {e}"}

    def _forward_metrics():
        """
        This wrapper runs in the Flask thread; it will call into the bot's event loop to gather data safely.
        """
        global MAIN_LOOP
        if MAIN_LOOP is None:
            return {"error": "bot main loop not available"}

        try:
            future = asyncio.run_coroutine_threadsafe(_collect_metrics(), MAIN_LOOP)
            return future.result(timeout=1.0)
        except Exception as e:
            logger.exception("Failed to collect metrics from main loop")
            return {"error": f"failed to collect metrics: {e}"}

    try:
        register_monitoring(_forward_metrics)
    except Exception:
        logger.exception("Failed to register monitoring callback with webserver")

    logger.info("✅ Bot initialized!")


def _get_memory_usage_mb():
    """Get current memory usage in MB"""
    try:
        import psutil
        process = psutil.Process()
        return round(process.memory_info().rss / 1024 / 1024, 2)
    except ImportError:
        return None


# ---------- Main -----------
def main():
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found")
        return

    if not API_ID or not API_HASH:
        logger.error("❌ API_ID or API_HASH not found")
        return

    logger.info("🤖 Starting Forwarder Bot...")

    # start webserver thread first (keeps /health available)
    start_server_thread()

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # Add conversation handler for forwadd
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("forwadd", forwadd_command)],
        states={
            TASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, task_name_handler)],
            SOURCE_IDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, source_ids_handler)],
            TARGET_IDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, target_ids_handler)],
            PREFIX_SUFFIX: [MessageHandler(filters.TEXT & ~filters.COMMAND, prefix_suffix_handler)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    # Add all handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("logout", logout_command))
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("fortasks", fortasks_command))
    application.add_handler(CommandHandler("getallid", getallid_command))
    application.add_handler(CommandHandler("adduser", adduser_command))
    application.add_handler(CommandHandler("removeuser", removeuser_command))
    application.add_handler(CommandHandler("listusers", listusers_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_login_process))
    # Add handler for delete confirmation
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_delete_handler))

    logger.info("✅ Bot ready!")
    try:
        application.run_polling(drop_pending_updates=True)
    finally:
        # run a final cleanup on a fresh loop to ensure Telethon clients are disconnected
        try:
            asyncio.run(shutdown_cleanup())
        except Exception:
            logger.exception("Error during shutdown cleanup")


if __name__ == "__main__":
    main()
