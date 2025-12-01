#!/usr/bin/env python3
import os
import asyncio
import logging
import functools
import gc
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
send_queue: Optional[asyncio.Queue[Tuple[int, TelegramClient, int, str]]] = None

UNAUTHORIZED_MESSAGE = """🚫 Access Denied!

You are not authorized to use this bot.

📞 Call this number: 07089430305

Or

🗨️ Message Developer: [HEMMY](https://t.me/justmemmy)
"""

# Track worker tasks so we can cancel them on shutdown
worker_tasks: List[asyncio.Task] = []
_send_workers_started = False

# MAIN loop reference for cross-thread metrics collection
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

# OPTIMIZED: Memory management
_last_gc_run = 0
GC_INTERVAL = 300  # Run GC every 5 minutes

# Per-task settings defaults (persisted if DB supports it)
TASK_SETTINGS_DEFAULT = {
    "filter_raw": False,
    "filter_numbers": False,
    "filter_alpha": False,
    "filter_remove_alpha": False,
    "filter_remove_numeric": False,
    "filter_prefix": None,
    "filter_suffix": None,
    # On by default as requested
    "outgoing": True,
    "forward_tag": True,
    "control": True,
}

# forwadd multi-step states
FORWADD_STATE_NAME = 1
FORWADD_STATE_SOURCES = 2
FORWADD_STATE_TARGETS = 3

forwadd_states: Dict[int, Dict] = {}  # user_id -> {"step": ..., ...}
task_settings_state: Dict[int, Dict] = {}  # temporary interactive states (prefix/suffix/delete_confirm)


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
                disable_web_page_preview=True,
            )
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_text(
                UNAUTHORIZED_MESSAGE,
                disable_web_page_preview=True,
            )
        return False

    return True


# ---------- Simple UI handlers (updated to remove monospace formatting) ----------
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

👤 User: {user_name}
📱 Phone: {user_phone}
{status_emoji} Status: {status_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 COMMANDS:

🔐 Account Management:
  • /login - Connect your Telegram account
  • /logout - Disconnect your account

📨 Forwarding Tasks:
  • /forwadd - Create a new forwarding task
  • /fortasks - List all your tasks

🆔 Utilities:
  • /getallid - Get all your chat IDs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ How it works:
1. Connect your account with /login
2. Create a forwarding task
3. Send ONLY NUMBERS in source chat
4. Bot forwards to target (no "Forwarded from" tag!)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("📋 My Tasks", callback_data="show_tasks")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])

    # safe: update.message is present for /start
    await update.message.reply_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    # OPTIMIZED: Check current user count
    if len(user_clients) >= MAX_CONCURRENT_USERS:
        await message.reply_text(
            "❌ Server at capacity!\n\n"
            "Too many users are currently connected. Please try again later.",
        )
        return

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    login_states[user_id] = {"client": client, "step": "waiting_phone"}

    await message.reply_text(
        "📱 Enter your phone number (with country code):\n\n"
        "Example: +1234567890\n\n"
        "⚠️ Make sure to include the + sign!",
    )


async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # If user is in logout confirmation flow, handle that first
    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return

    # forwadd flow and task_settings flows are handled elsewhere; this handler focuses on login
    if user_id not in login_states:
        return

    state = login_states[user_id]
    text = update.message.text.strip()
    client = state["client"]

    try:
        if state["step"] == "waiting_phone":
            processing_msg = await update.message.reply_text(
                "⏳ Processing... Requesting verification code from Telegram..."
            )

            result = await client.send_code_request(text)
            state["phone"] = text
            state["phone_code_hash"] = result.phone_code_hash
            state["step"] = "waiting_code"

            await processing_msg.edit_text(
                "✅ Code sent!\n\n"
                "Enter the verification code in this format:\n"
                "verify12345\n\n"
                "Type 'verify' followed immediately by your code (no spaces)."
            )

        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text(
                    "❌ Invalid format! Please use: verify12345"
                )
                return

            code = text[6:]

            if not code or not code.isdigit():
                await update.message.reply_text("❌ Invalid code! Code must be digits.")
                return

            verifying_msg = await update.message.reply_text("🔄 Verifying...")

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
                    "✅ Successfully connected!\n\n"
                    f"Name: {me.first_name}\n"
                    f"Phone: {state['phone']}\n\n"
                    "You can now create forwarding tasks with:\n"
                    "/forwadd"
                )

            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await verifying_msg.edit_text(
                    "🔐 2FA Password Required\n\n"
                    "Enter your 2-step verification password in this format:\n"
                    "passwordYourPassword123\n\n"
                    "Type 'password' followed immediately by your 2FA password (no spaces)."
                )

        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text(
                    "❌ Invalid format! Please use: passwordYourPassword123"
                )
                return

            password = text[8:]

            if not password:
                await update.message.reply_text("❌ No password provided!")
                return

            verifying_msg = await update.message.reply_text("🔄 Verifying 2FA...")

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
                "✅ Successfully connected!\n\n"
                f"Name: {me.first_name}\n"
                f"Phone: {state['phone']}\n\n"
                "You can now create forwarding tasks!"
            )

    except Exception as e:
        logger.exception("Error during login process for %s", user_id)
        await update.message.reply_text(f"❌ Error: {str(e)}\nPlease try /login again.")
        if user_id in login_states:
            try:
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
        await message.reply_text("❌ You're not connected!\nUse /login to connect your account.")
        return

    logout_states[user_id] = {"phone": user["phone"]}

    await message.reply_text(
        "⚠️ Confirm Logout\n\n"
        f"Enter your phone number to confirm disconnection:\n\n"
        f"Your connected phone: {user['phone']}\n\n"
        "Type your phone number exactly to confirm logout."
    )


async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id

    if user_id not in logout_states:
        return False

    text = update.message.text.strip()
    stored_phone = logout_states[user_id]["phone"]

    if text != stored_phone:
        await update.message.reply_text(
            "❌ Phone number doesn't match!\n\n"
            f"Expected: {stored_phone}\n"
            f"You entered: {text}\n\n"
            "Please try again or use /start to cancel."
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
        "👋 Account disconnected successfully!\n\n"
        "✅ All your forwarding tasks have been stopped.\n"
        "🔄 Use /login to connect again."
    )
    return True


# ---------- Interactive /forwadd multi-step implementation ----------
async def forwadd_command_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start multi-step forwadd flow by asking task name."""
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ You need to connect your account first!\nUse /login to connect your Telegram account.")
        return

    forwadd_states[user_id] = {"step": FORWADD_STATE_NAME}
    await update.message.reply_text("🎯 Let's create a new forwarding task!\n\n📝 What should be the name for your new task?")


async def handle_forwadd_steps(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in forwadd_states:
        return
    state = forwadd_states[user_id]
    text = update.message.text.strip()

    if state["step"] == FORWADD_STATE_NAME:
        if not text:
            await update.message.reply_text("📝 Please enter a task name.")
            return
        state["name"] = text
        state["step"] = FORWADD_STATE_SOURCES
        await update.message.reply_text(
            f"👍 Great! The task name is “{text}”.\n\n"
            "Now, please enter the source chat ID(s) you want to use (separated by spaces if more than one).\n\n"
            "If you don't know the IDs, use /getallid."
        )
    elif state["step"] == FORWADD_STATE_SOURCES:
        try:
            sources = [int(x) for x in text.replace(",", " ").split() if x.strip()]
            if not sources:
                raise ValueError
            state["sources"] = sources
            state["step"] = FORWADD_STATE_TARGETS
            await update.message.reply_text(
                f"🔗 Your source ID(s): {', '.join(map(str, sources))}.\n\n"
                "Next, enter the target chat ID(s) (separated by spaces if more than one).\n"
                "Use /getallid if you need help finding IDs."
            )
        except Exception:
            await update.message.reply_text("❗ Please enter valid numeric source IDs, separated by spaces.")
    elif state["step"] == FORWADD_STATE_TARGETS:
        try:
            targets = [int(x) for x in text.replace(",", " ").split() if x.strip()]
            if not targets:
                raise ValueError
            name = state["name"]
            sources = state["sources"]

            # Try to call DB method with settings param if available; fallback if not
            try:
                added = await db_call(db.add_forwarding_task, user_id, name, sources, targets, TASK_SETTINGS_DEFAULT.copy())
            except TypeError:
                # older DB signature without settings
                added = await db_call(db.add_forwarding_task, user_id, name, sources, targets)

            if added:
                # Update in-memory cache immediately (hot path)
                tasks_cache.setdefault(user_id, [])
                tasks_cache[user_id].append(
                    {
                        "id": None,
                        "label": name,
                        "source_ids": sources,
                        "target_ids": targets,
                        "is_active": 1,
                        "settings": TASK_SETTINGS_DEFAULT.copy(),
                    }
                )
                # schedule async resolve of target entities (background)
                try:
                    asyncio.create_task(resolve_targets_for_user(user_id, targets))
                except Exception:
                    logger.exception("Failed to schedule resolve_targets_for_user task")

                await update.message.reply_text(
                    f"🎉 Task '{name}' created successfully!\n\n"
                    f"📥 Sources: {', '.join(map(str, sources))}\n"
                    f"📤 Targets: {', '.join(map(str, targets))}\n\n"
                    "You can view or manage your task in /fortasks."
                )
            else:
                await update.message.reply_text(
                    f"❌ Task '{name}' already exists!"
                )

            # cleanup state
            del forwadd_states[user_id]
        except Exception as e:
            logger.exception("Error creating task: %s", e)
            await update.message.reply_text("❗ Please enter valid numeric target IDs, separated by spaces.")


# ---------- fortasks: show tasks as inline buttons ----------
async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    # Ensure tasks_cache populated from DB on demand if empty
    if user_id not in tasks_cache or not tasks_cache.get(user_id):
        try:
            db_tasks = await db_call(db.get_user_tasks, user_id)
            tasks_cache[user_id] = []
            for t in db_tasks:
                task = {
                    "id": t.get("id"),
                    "label": t.get("label"),
                    "source_ids": t.get("source_ids", []),
                    "target_ids": t.get("target_ids", []),
                    "is_active": t.get("is_active", 1),
                    "settings": t.get("settings", TASK_SETTINGS_DEFAULT.copy()) if isinstance(t.get("settings", None), dict) else TASK_SETTINGS_DEFAULT.copy()
                }
                tasks_cache[user_id].append(task)
        except Exception:
            logger.exception("Error loading tasks from DB for user %s", user_id)

    tasks = tasks_cache.get(user_id) or []

    if not tasks:
        await message.reply_text(
            "🗂️ No Active Tasks\n\n"
            "You don't have any forwarding tasks yet.\n\n"
            "Create one with:\n"
            "/forwadd"
        )
        return

    keyboard = []
    for i, task in enumerate(tasks, 1):
        label = task['label']
        display = f"{i}. {label}"
        # Each task label is an inline button
        keyboard.append([InlineKeyboardButton(display, callback_data=f"taskmenu_{label}")])

    header = "🗂️ Your Forwarding Tasks\n\nTap a task below to manage it.\n\n"
    footer = f"\nTotal: {len(tasks)} task(s)"
    await message.reply_text(header + footer, reply_markup=InlineKeyboardMarkup(keyboard))


# ---------- Callback button handler: main entrypoint for inline UI ----------
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    if not await check_authorization(update, context):
        return

    await query.answer()

    data = query.data
    user_id = query.from_user.id

    # navigation: show tasks
    if data == "show_tasks":
        await query.message.delete()
        await fortasks_command(update, context)
        return

    # Login/Logout buttons from start UI
    if data == "login":
        await query.message.delete()
        await login_command(update, context)
        return
    if data == "logout":
        await query.message.delete()
        await logout_command(update, context)
        return

    # chat id navigation handled earlier
    if data.startswith("chatids_"):
        # existing behavior preserved
        if data == "chatids_back":
            await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
            return
        parts = data.split("_")
        if len(parts) >= 3:
            category = parts[1]
            page = int(parts[2])
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context)
            return

    # Task menu entry
    if data.startswith("taskmenu_"):
        label = data[len("taskmenu_"):]
        task = task_get(user_id, label)
        if not task:
            await query.message.reply_text("❗ Task not found.")
            return
        # Ensure settings exist
        if "settings" not in task or not isinstance(task["settings"], dict):
            task["settings"] = TASK_SETTINGS_DEFAULT.copy()

        s = task["settings"]
        emoji = lambda k: "✅" if s.get(k, False) else "❌"

        msg = f"🗂️ Task Menu for {label}\n\nManage or customize this task. Tap an option below:"
        buttons = [
            [InlineKeyboardButton("🧮 Filters", callback_data=f"tasksetfilter_{label}")],
            [
                InlineKeyboardButton(f"{emoji('outgoing')} Outgoing", callback_data=f"toggle_outgoing_{label}"),
                InlineKeyboardButton(f"{emoji('forward_tag')} Forward Tag", callback_data=f"toggle_forwardtag_{label}"),
            ],
            [InlineKeyboardButton(f"{emoji('control')} Control", callback_data=f"toggle_control_{label}")],
            [InlineKeyboardButton("🗑️ Delete", callback_data=f"taskdelete_{label}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="show_tasks")],
        ]

        try:
            await query.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
        except Exception:
            # message may have been non-editable (older message), send new
            await query.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
        task_settings_state[user_id] = {"label": label, "step": "menu"}
        return

    # Toggle Outgoing / Forward Tag / Control
    for setting, prefix in [("outgoing", "toggle_outgoing_"), ("forward_tag", "toggle_forwardtag_"), ("control", "toggle_control_")]:
        if data.startswith(prefix):
            label = data[len(prefix):]
            task = task_get(user_id, label)
            if not task:
                await query.message.reply_text("❗ Task not found.")
                return
            if "settings" not in task:
                task["settings"] = TASK_SETTINGS_DEFAULT.copy()
            task["settings"][setting] = not task["settings"].get(setting, True)
            # persist if DB supports update_task_settings
            try:
                await db_call(db.update_task_settings, user_id, label, task["settings"])
            except Exception:
                # ignore if DB doesn't support yet
                pass
            # re-show menu with updated emoji
            await button_handler(update, context)
            return

    # Delete initiation: prompt user to type task name
    if data.startswith("taskdelete_"):
        label = data[len("taskdelete_"):]
        task = task_get(user_id, label)
        if not task:
            await query.message.reply_text("❗ Task not found.")
            return
        prompt = f"🗑️ You're about to delete task '{label}'.\n\nPlease type the task name exactly to confirm deletion."
        try:
            await query.message.edit_text(prompt)
        except Exception:
            await query.message.reply_text(prompt)
        task_settings_state[user_id] = {"label": label, "step": "delete_confirm"}
        return

    # Filters submenu entry
    if data.startswith("tasksetfilter_"):
        label = data[len("tasksetfilter_"):]
        task = task_get(user_id, label)
        if not task:
            await query.message.reply_text("❗ Task not found.")
            return
        if "settings" not in task:
            task["settings"] = TASK_SETTINGS_DEFAULT.copy()
        s = task["settings"]
        def fe(k, title): return ("✅" if s.get(k, False) else "❌") + " " + title
        msg = f"🧰 Filters for {label}\n\nChoose filters to turn On/Off:"
        filter_buttons = [
            [InlineKeyboardButton(fe('filter_raw', 'Raw text'), callback_data=f"toggle_filterraw_{label}")],
            [InlineKeyboardButton(fe('filter_numbers', 'Numbers only'), callback_data=f"toggle_filternum_{label}")],
            [InlineKeyboardButton(fe('filter_alpha', 'Alphabets only'), callback_data=f"toggle_filteralpha_{label}")],
            [InlineKeyboardButton(fe('filter_remove_alpha', 'Removed Alphabetic'), callback_data=f"toggle_remalpha_{label}")],
            [InlineKeyboardButton(fe('filter_remove_numeric', 'Removed Numeric'), callback_data=f"toggle_remnum_{label}")],
            [InlineKeyboardButton(fe('filter_prefix', 'Prefix'), callback_data=f"set_prefix_{label}"), InlineKeyboardButton(fe('filter_suffix', 'Suffix'), callback_data=f"set_suffix_{label}")],
            [InlineKeyboardButton("⬅️ Back to Task", callback_data=f"taskmenu_{label}")],
        ]
        try:
            await query.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(filter_buttons))
        except Exception:
            await query.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(filter_buttons))
        task_settings_state[user_id] = {"label": label, "step": "filters"}
        return

    # Filter toggles
    filter_map = {
        "toggle_filterraw_": "filter_raw",
        "toggle_filternum_": "filter_numbers",
        "toggle_filteralpha_": "filter_alpha",
        "toggle_remalpha_": "filter_remove_alpha",
        "toggle_remnum_": "filter_remove_numeric",
    }
    for prefix, key in filter_map.items():
        if data.startswith(prefix):
            label = data[len(prefix):]
            task = task_get(user_id, label)
            if not task:
                await query.message.reply_text("❗ Task not found.")
                return
            if "settings" not in task:
                task["settings"] = TASK_SETTINGS_DEFAULT.copy()
            task["settings"][key] = not task["settings"].get(key, False)
            try:
                await db_call(db.update_task_settings, user_id, label, task["settings"])
            except Exception:
                pass
            # return to filter menu (instant update)
            await button_handler(update, context)
            return

    # Prefix / Suffix setting triggers
    if data.startswith("set_prefix_"):
        label = data[len("set_prefix_"):]
        prompt = f"✏️ Send the prefix you want to set for task '{label}'.\n\nSend an empty message to clear it."
        try:
            await query.message.edit_text(prompt)
        except Exception:
            await query.message.reply_text(prompt)
        task_settings_state[user_id] = {"label": label, "step": "prefix"}
        return
    if data.startswith("set_suffix_"):
        label = data[len("set_suffix_"):]
        prompt = f"✏️ Send the suffix you want to set for task '{label}'.\n\nSend an empty message to clear it."
        try:
            await query.message.edit_text(prompt)
        except Exception:
            await query.message.reply_text(prompt)
        task_settings_state[user_id] = {"label": label, "step": "suffix"}
        return

    # Final delete confirm via inline (Yes/Cancel)
    if data.startswith("confirm_del_"):
        label = data[len("confirm_del_"):]
        # perform deletion
        try:
            deleted = await db_call(db.remove_forwarding_task, user_id, label)
        except Exception:
            deleted = False
        # remove from cache
        if user_id in tasks_cache:
            tasks_cache[user_id] = [t for t in tasks_cache[user_id] if t.get("label") != label]
        if deleted:
            await query.message.edit_text(f"✅ Task '{label}' deleted successfully.")
        else:
            await query.message.edit_text(f"❌ Failed to delete task '{label}'. It may not exist.")
        if user_id in task_settings_state:
            task_settings_state.pop(user_id, None)
        return

    # Cancel action from confirm UI or menus
    if data.startswith("taskmenu_") and data.endswith("_cancel"):
        label = data[len("taskmenu_"):-len("_cancel")]
        await query.message.edit_text("Action cancelled.")
        task_settings_state.pop(user_id, None)
        return

    # Fallback unknown callback
    await query.message.reply_text("Unknown action.")


# ---------- Task settings text handler (prefix/suffix, delete confirm typing) ----------
async def task_settings_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in task_settings_state:
        return
    state = task_settings_state[user_id]
    label = state.get("label")
    step = state.get("step")
    text = update.message.text.strip()

    task = task_get(user_id, label)
    if not task:
        await update.message.reply_text("❗ Task not found (it may have been removed).")
        task_settings_state.pop(user_id, None)
        return

    if step == "prefix":
        # set prefix to text or clear if empty
        val = text if text else None
        task["settings"]["filter_prefix"] = val
        try:
            await db_call(db.update_task_settings, user_id, label, task["settings"])
        except Exception:
            pass
        await update.message.reply_text(f"✅ Prefix set to: {val if val else '(none)'} for '{label}'.")
        task_settings_state.pop(user_id, None)
        return
    if step == "suffix":
        val = text if text else None
        task["settings"]["filter_suffix"] = val
        try:
            await db_call(db.update_task_settings, user_id, label, task["settings"])
        except Exception:
            pass
        await update.message.reply_text(f"✅ Suffix set to: {val if val else '(none)'} for '{label}'.")
        task_settings_state.pop(user_id, None)
        return
    if step == "delete_confirm":
        if text == label:
            # show inline confirm yes/cancel
            buttons = [
                [InlineKeyboardButton("Yes — confirm deletion", callback_data=f"confirm_del_{label}"),
                 InlineKeyboardButton("Cancel", callback_data=f"taskmenu_{label}")]
            ]
            await update.message.reply_text(f"Are you sure you want to delete task '{label}'?", reply_markup=InlineKeyboardMarkup(buttons))
            task_settings_state[user_id] = {"label": label, "step": "delete_final"}
        else:
            await update.message.reply_text("❗ Name doesn't match. Please type the task name exactly to confirm deletion.")
        return


# ---------- Chat listing functions (unchanged) ----------
async def show_chat_categories(user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
    if user_id not in user_clients:
        return

    message_text = (
        "🗂️ Chat ID Categories\n\n"
        "Choose which type of chat IDs you want to see:\n\n"
        "🤖 Bots - Bot accounts\n"
        "📢 Channels - Broadcast channels\n"
        "👥 Groups - Group chats\n"
        "👤 Private - Private conversations\n\n"
        "Select a category below:"
    )

    keyboard = [
        [InlineKeyboardButton("🤖 Bots", callback_data="chatids_bots_0"), InlineKeyboardButton("📢 Channels", callback_data="chatids_channels_0")],
        [InlineKeyboardButton("👥 Groups", callback_data="chatids_groups_0"), InlineKeyboardButton("👤 Private", callback_data="chatids_private_0")],
    ]

    if message_id:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard))


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
        chat_list = f"{emoji} {name}\n\nNo {name.lower()} found! Try another category."
    else:
        chat_list = f"{emoji} {name} (Page {page + 1}/{total_pages})\n\n"
        for i, dialog in enumerate(page_dialogs, start + 1):
            chat_name = dialog.name[:30] if dialog.name else "Unknown"
            chat_list += f"{i}. {chat_name}\n   ID: {dialog.id}\n\n"
        chat_list += f"📊 Total: {len(categorized_dialogs)} {name.lower()}\n"

    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"chatids_{category}_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"chatids_{category}_{page + 1}"))
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="chatids_back")])

    try:
        await context.bot.edit_message_text(chat_list, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception:
        await context.bot.send_message(chat_id=chat_id, text=chat_list, reply_markup=InlineKeyboardMarkup(keyboard))


# ---------- OPTIMIZED Forwarding core: handler registration, message handler, send worker, resolver ----------
def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    """Attach a NewMessage handler once per client/user to avoid duplicates and store the handler (so we can remove it)."""
    if handler_registered.get(user_id):
        return

    async def _hot_message_handler(event):
        try:
            # OPTIMIZED: Batch process messages and run GC periodically
            await optimized_gc()

            # Extract text
            message_text = getattr(event, "raw_text", None) or getattr(getattr(event, "message", None), "message", None)
            if not message_text:
                return

            # Get chat_id reliably
            chat_id = getattr(event, "chat_id", None) or getattr(getattr(event, "message", None), "chat_id", None)
            if chat_id is None:
                return

            user_tasks = tasks_cache.get(user_id)
            if not user_tasks:
                return

            # For each task, check if chat_id is a source, then apply filters and forward
            for task in user_tasks:
                if chat_id in task.get("source_ids", []):
                    settings = task.get("settings", {}) or {}
                    # Control toggle: if control is off, don't forward
                    if settings.get("control") is False:
                        continue

                    # Outgoing toggle: if message is outgoing (sent by the user), Telethon event has .out attribute
                    # event.out is True for outgoing messages
                    is_outgoing = getattr(event, "out", False)
                    if not settings.get("outgoing", True) and is_outgoing:
                        continue

                    # Apply filters to message_text to determine final messages to send
                    outputs = apply_filters_to_text(message_text, settings)

                    # Prepare sends
                    for target_id in task.get("target_ids", []):
                        for out_text in outputs:
                            try:
                                global send_queue
                                if send_queue is None:
                                    logger.debug("Send queue not initialized; dropping forward job")
                                    continue
                                await send_queue.put((user_id, client, int(target_id), out_text))
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


def apply_filters_to_text(message_text: str, settings: Dict) -> List[str]:
    """
    Apply the filters described in the task settings to the incoming message text.
    Returns a list of strings that should be forwarded (can be multiple lines).
    """
    # Default: only forward numeric-only messages (legacy behaviour), but if filters enabled, they override.
    text = message_text or ""
    # If Forward Tag is handled elsewhere; here we only manipulate content.

    results: List[str] = []

    # Helper to split into words and classify numeric vs alpha
    words = text.split()
    has_alpha = any(any(ch.isalpha() for ch in w) for w in words)
    has_digit = any(any(ch.isdigit() for ch in w) for w in words)

    # If Raw text filter on: forward entire text unmodified (with prefix/suffix)
    if settings.get("filter_raw"):
        out = text
        prefix = settings.get("filter_prefix") or ""
        suffix = settings.get("filter_suffix") or ""
        results.append(f"{prefix}{out}{suffix}")
        return results

    # Numbers only filter
    if settings.get("filter_numbers"):
        # Forward only if there's any digit in the message; otherwise ignore
        if any(ch.isdigit() for ch in text):
            out = text
            prefix = settings.get("filter_prefix") or ""
            suffix = settings.get("filter_suffix") or ""
            results.append(f"{prefix}{out}{suffix}")
        return results

    # Alphabets only filter
    if settings.get("filter_alpha"):
        if any(ch.isalpha() for ch in text):
            out = text
            prefix = settings.get("filter_prefix") or ""
            suffix = settings.get("filter_suffix") or ""
            results.append(f"{prefix}{out}{suffix}")
        return results

    # Removed Alphabetic: From mixed text (both letters and numbers), remove numeric words and forward only alphabetic words one by one.
    if settings.get("filter_remove_alpha"):
        if has_alpha and has_digit:
            for w in words:
                if any(ch.isalpha() for ch in w) and not any(ch.isdigit() for ch in w):
                    prefix = settings.get("filter_prefix") or ""
                    suffix = settings.get("filter_suffix") or ""
                    results.append(f"{prefix}{w}{suffix}")
        return results

    # Removed Numeric: From mixed text, remove alphabetic words and forward numeric words one by one.
    if settings.get("filter_remove_numeric"):
        if has_alpha and has_digit:
            for w in words:
                if any(ch.isdigit() for ch in w) and not any(ch.isalpha() for ch in w):
                    prefix = settings.get("filter_prefix") or ""
                    suffix = settings.get("filter_suffix") or ""
                    results.append(f"{prefix}{w}{suffix}")
        return results

    # If no filters are active, preserve legacy behavior: only forward messages that are purely numeric
    if not any(settings.get(k) for k in [
        "filter_raw", "filter_numbers", "filter_alpha", "filter_remove_alpha", "filter_remove_numeric"
    ]):
        if text.strip().isdigit():
            prefix = settings.get("filter_prefix") or ""
            suffix = settings.get("filter_suffix") or ""
            results.append(f"{prefix}{text.strip()}{suffix}")
        # else: do not forward
        return results

    # Fallback: if filters exist but didn't match, return empty
    return results


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
    """OPTIMIZED Worker that consumes send_queue and performs client.send_message with backoff on FloodWait."""
    logger.info("Send worker %d started", worker_id)
    global send_queue
    if send_queue is None:
        logger.error("send_worker_loop started before send_queue initialized")
        return

    while True:
        try:
            user_id, client, target_id, message_text = await send_queue.get()
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
                await client.send_message(entity, message_text)
                logger.debug("Forwarded message for user %s to %s", user_id, target_id)
            except FloodWaitError as fwe:
                wait = int(getattr(fwe, "seconds", 10))
                logger.warning("FloodWait for %s seconds. Pausing worker %d", wait, worker_id)
                await asyncio.sleep(wait + 1)
                try:
                    await send_queue.put((user_id, client, target_id, message_text))
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


# ---------- OPTIMIZED Session restore and initialization ----------
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
        tasks_cache[uid].append({
            "id": t["id"],
            "label": t["label"],
            "source_ids": t["source_ids"],
            "target_ids": t["target_ids"],
            "is_active": 1,
            "settings": t.get("settings", TASK_SETTINGS_DEFAULT.copy())
        })

    logger.info("📊 Found %d logged in user(s)", len(users))

    # OPTIMIZED: Restore sessions in batches to avoid memory spikes
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


# ---------- Utility helpers ----------
def task_get(user_id: int, label: str) -> Optional[Dict]:
    """Find a task object in the in-memory cache for a user."""
    user_tasks = tasks_cache.get(user_id) or []
    return next((t for t in user_tasks if t.get("label") == label), None)


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

    # Command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("logout", logout_command))
    # /forwadd starts the interactive flow
    application.add_handler(CommandHandler("forwadd", forwadd_command_start))
    application.add_handler(CommandHandler("fortasks", fortasks_command))
    application.add_handler(CommandHandler("getallid", lambda u, c: asyncio.create_task(getallid_command(u, c))))
    application.add_handler(CommandHandler("adduser", lambda u, c: asyncio.create_task(adduser_command(u, c))))
    application.add_handler(CommandHandler("removeuser", lambda u, c: asyncio.create_task(removeuser_command(u, c))))
    application.add_handler(CommandHandler("listusers", lambda u, c: asyncio.create_task(listusers_command(u, c))))

    # Callback query handler for inline buttons
    application.add_handler(CallbackQueryHandler(button_handler))

    # Unified text handler: login flow, forwadd steps, task settings steps
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, extended_unified_text_handler))

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
