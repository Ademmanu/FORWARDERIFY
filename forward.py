#!/usr/bin/env python3
import os
import asyncio
import logging
import functools
import gc
import re
from typing import Dict, List, Optional, Tuple, Set, Callable, Any
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

# Task creation states
task_creation_states: Dict[int, Dict[str, Any]] = {}  # user_id -> {step: str, name: str, source_ids: List[int], target_ids: List[int]}

# OPTIMIZED: Hot-path caches with memory limits
tasks_cache: Dict[int, List[Dict]] = {}  # user_id -> list of task dicts
target_entity_cache: Dict[int, Dict[int, object]] = {}  # user_id -> {target_id: resolved_entity}
# handler_registered maps user_id -> handler callable (so we can remove it)
handler_registered: Dict[int, Callable] = {}

# Global send queue is created later on the running event loop (in post_init/start_send_workers)
send_queue: Optional[asyncio.Queue[Tuple[int, TelegramClient, int, str, Dict, bool, Optional[int], Optional[int]]]] = None

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


# ---------- Message filtering functions ----------
def extract_words(text: str) -> List[str]:
    """Extract words from text, preserving emojis and special characters"""
    # This regex pattern captures words including those with special characters and emojis
    return re.findall(r'\S+', text)

def is_numeric_word(word: str) -> bool:
    """Check if word contains only digits (numeric)"""
    return word.isdigit()

def is_alphabetic_word(word: str) -> bool:
    """Check if word contains only letters (alphabetic)"""
    return word.isalpha()

def contains_numeric(word: str) -> bool:
    """Check if word contains any digits"""
    return any(char.isdigit() for char in word)

def contains_alphabetic(word: str) -> bool:
    """Check if word contains any letters"""
    return any(char.isalpha() for char in word)

def contains_only_special(word: str) -> bool:
    """Check if word contains only special characters (no letters or digits)"""
    return not (contains_numeric(word) or contains_alphabetic(word))

def apply_filters(message_text: str, task_filters: Dict) -> List[str]:
    """Apply filters to message text and return list of messages to forward"""
    if not message_text:
        return []
    
    # Default filters if not specified
    filters_enabled = task_filters.get('filters', {})
    
    # If raw text is enabled, forward everything with prefix/suffix
    if filters_enabled.get('raw_text', False):
        processed = message_text
        if filters_enabled.get('prefix'):
            processed = filters_enabled['prefix'] + processed
        if filters_enabled.get('suffix'):
            processed = processed + filters_enabled['suffix']
        return [processed]
    
    messages_to_send = []
    
    # Extract words from message
    words = extract_words(message_text)
    
    # Process based on enabled filters
    if filters_enabled.get('numbers_only', False):
        # Forward only numeric words
        numeric_words = [word for word in words if is_numeric_word(word)]
        for word in numeric_words:
            processed = word
            if filters_enabled.get('prefix'):
                processed = filters_enabled['prefix'] + processed
            if filters_enabled.get('suffix'):
                processed = processed + filters_enabled['suffix']
            messages_to_send.append(processed)
    
    elif filters_enabled.get('alphabets_only', False):
        # Forward only alphabetic words
        alphabetic_words = [word for word in words if is_alphabetic_word(word)]
        for word in alphabetic_words:
            processed = word
            if filters_enabled.get('prefix'):
                processed = filters_enabled['prefix'] + processed
            if filters_enabled.get('suffix'):
                processed = processed + filters_enabled['suffix']
            messages_to_send.append(processed)
    
    elif filters_enabled.get('removed_alphabetic', False):
        # Keep letters and special characters, remove numeric words
        # Words that have letters (or special chars) but no numbers
        non_numeric_words = []
        for word in words:
            # Skip pure numeric words
            if is_numeric_word(word):
                continue
            # Keep words that contain letters or are pure special characters
            if contains_alphabetic(word) or contains_only_special(word):
                non_numeric_words.append(word)
        
        for word in non_numeric_words:
            processed = word
            if filters_enabled.get('prefix'):
                processed = filters_enabled['prefix'] + processed
            if filters_enabled.get('suffix'):
                processed = processed + filters_enabled['suffix']
            messages_to_send.append(processed)
    
    elif filters_enabled.get('removed_numeric', False):
        # Keep numbers and special characters, remove alphabetic words
        # Words that have numbers (or special chars) but no letters
        non_alphabetic_words = []
        for word in words:
            # Skip pure alphabetic words
            if is_alphabetic_word(word):
                continue
            # Keep words that contain numbers or are pure special characters
            if contains_numeric(word) or contains_only_special(word):
                non_alphabetic_words.append(word)
        
        for word in non_alphabetic_words:
            processed = word
            if filters_enabled.get('prefix'):
                processed = filters_enabled['prefix'] + processed
            if filters_enabled.get('suffix'):
                processed = processed + filters_enabled['suffix']
            messages_to_send.append(processed)
    
    else:
        # No specific filter enabled, forward all words with prefix/suffix
        for word in words:
            processed = word
            if filters_enabled.get('prefix'):
                processed = filters_enabled['prefix'] + processed
            if filters_enabled.get('suffix'):
                processed = processed + filters_enabled['suffix']
            messages_to_send.append(processed)
    
    return messages_to_send


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


# ---------- Simple UI handlers (left mostly unchanged) ----------
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
  /login - Connect your Telegram account
  /logout - Disconnect your account

📨 **Forwarding Tasks:**
  /forwadd - Create a new forwarding task
  /fortasks - List all your tasks

🆔 **Utilities:**
  /getallid - Get all your chat IDs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ **How it works:**
1. Connect your account with /login
2. Create a forwarding task
3. Send messages in source chat
4. Bot forwards to target with your chosen filters!

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
        parse_mode="Markdown",
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    if not await check_authorization(update, context):
        return

    await query.answer()

    if query.data == "login":
        await query.message.delete()
        await login_command(update, context)
    elif query.data == "logout":
        await query.message.delete()
        await logout_command(update, context)
    elif query.data == "show_tasks":
        await query.message.delete()
        await fortasks_command(update, context)
    elif query.data.startswith("chatids_"):
        user_id = query.from_user.id
        if query.data == "chatids_back":
            await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
        else:
            parts = query.data.split("_")
            category = parts[1]
            page = int(parts[2])
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context)
    elif query.data.startswith("task_"):
        await handle_task_menu(update, context)
    elif query.data.startswith("filter_"):
        await handle_filter_menu(update, context)
    elif query.data.startswith("toggle_"):
        await handle_toggle_action(update, context)
    elif query.data.startswith("delete_"):
        await handle_delete_action(update, context)
    elif query.data.startswith("prefix_"):
        await handle_prefix_suffix(update, context)
    elif query.data.startswith("suffix_"):
        await handle_prefix_suffix(update, context)
    elif query.data.startswith("confirm_delete_"):
        await handle_confirm_delete(update, context)


# ---------- Task creation flow (New) ----------
async def forwadd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the interactive task creation process"""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\nUse /login to connect your Telegram account.",
            parse_mode="Markdown"
        )
        return

    # Start task creation process
    task_creation_states[user_id] = {
        "step": "waiting_name",
        "name": "",
        "source_ids": [],
        "target_ids": []
    }

    await update.message.reply_text(
        "🎯 **Let's create a new forwarding task!**\n\n"
        "📝 **Step 1 of 3:** Please enter a name for your task.\n\n"
        "💡 *Example: My Forwarding Task*",
        parse_mode="Markdown"
    )


async def handle_task_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle interactive task creation steps"""
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if user_id not in task_creation_states:
        return

    state = task_creation_states[user_id]

    try:
        if state["step"] == "waiting_name":
            if not text:
                await update.message.reply_text("❌ **Please enter a valid task name!**")
                return

            state["name"] = text
            state["step"] = "waiting_source"

            await update.message.reply_text(
                f"✅ **Task name saved:** {text}\n\n"
                "📥 **Step 2 of 3:** Please enter the source chat ID(s).\n\n"
                "You can enter multiple IDs separated by spaces.\n"
                "💡 *Use /getallid to find your chat IDs*\n\n"
                "**Example:** `123456789 987654321`",
                parse_mode="Markdown"
            )

        elif state["step"] == "waiting_source":
            if not text:
                await update.message.reply_text("❌ **Please enter at least one source ID!**")
                return

            try:
                source_ids = [int(id_str.strip()) for id_str in text.split() if id_str.strip().lstrip('-').isdigit()]
                if not source_ids:
                    await update.message.reply_text("❌ **Please enter valid numeric IDs!**")
                    return

                state["source_ids"] = source_ids
                state["step"] = "waiting_target"

                await update.message.reply_text(
                    f"✅ **Source IDs saved:** {', '.join(map(str, source_ids))}\n\n"
                    "📤 **Step 3 of 3:** Please enter the target chat ID(s).\n\n"
                    "You can enter multiple IDs separated by spaces.\n"
                    "💡 *Use /getallid to find your chat IDs*\n\n"
                    "**Example:** `111222333`",
                    parse_mode="Markdown"
                )

            except ValueError:
                await update.message.reply_text("❌ **Please enter valid numeric IDs only!**")

        elif state["step"] == "waiting_target":
            if not text:
                await update.message.reply_text("❌ **Please enter at least one target ID!**")
                return

            try:
                target_ids = [int(id_str.strip()) for id_str in text.split() if id_str.strip().lstrip('-').isdigit()]
                if not target_ids:
                    await update.message.reply_text("❌ **Please enter valid numeric IDs!**")
                    return

                state["target_ids"] = target_ids

                # Create the task with default filters
                task_filters = {
                    "filters": {
                        "raw_text": False,
                        "numbers_only": False,
                        "alphabets_only": False,
                        "removed_alphabetic": False,
                        "removed_numeric": False,
                        "prefix": "",
                        "suffix": ""
                    },
                    "outgoing": True,      # ✅ On by default
                    "forward_tag": False,   # ❌ Off by default (no tag)
                    "control": True        # ✅ On by default (running)
                }

                added = await db_call(db.add_forwarding_task, 
                                     user_id, 
                                     state["name"], 
                                     state["source_ids"], 
                                     state["target_ids"],
                                     task_filters)

                if added:
                    # Update in-memory cache
                    tasks_cache.setdefault(user_id, [])
                    tasks_cache[user_id].append({
                        "id": None,
                        "label": state["name"],
                        "source_ids": state["source_ids"],
                        "target_ids": state["target_ids"],
                        "is_active": 1,
                        "filters": task_filters
                    })

                    # Schedule async resolve of target entities
                    try:
                        asyncio.create_task(resolve_targets_for_user(user_id, target_ids))
                    except Exception:
                        logger.exception("Failed to schedule resolve_targets_for_user task")

                    # Send success message
                    await update.message.reply_text(
                        f"🎉 **Task created successfully!**\n\n"
                        f"📋 **Name:** {state['name']}\n"
                        f"📥 **Sources:** {', '.join(map(str, state['source_ids']))}\n"
                        f"📤 **Targets:** {', '.join(map(str, state['target_ids']))}\n\n"
                        "✅ All filters are set to default:\n"
                        "• Outgoing: ✅ On\n"
                        "• Forward Tag: ❌ Off\n"
                        "• Control: ✅ On\n\n"
                        "Use /fortasks to manage your task!",
                        parse_mode="Markdown"
                    )

                    # Clean up state
                    del task_creation_states[user_id]

                else:
                    await update.message.reply_text(
                        f"❌ **Task '{state['name']}' already exists!**\n\n"
                        "Please choose a different name.",
                        parse_mode="Markdown"
                    )

            except ValueError:
                await update.message.reply_text("❌ **Please enter valid numeric IDs only!**")

    except Exception as e:
        logger.exception("Error in task creation for user %s: %s", user_id, e)
        await update.message.reply_text(
            f"❌ **Error creating task:** {str(e)}\n\n"
            "Please try again with /forwadd",
            parse_mode="Markdown"
        )
        if user_id in task_creation_states:
            del task_creation_states[user_id]


# ---------- Task Menu System ----------
async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all tasks with inline buttons"""
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    # Use in-memory cache for fast response
    tasks = tasks_cache.get(user_id) or []

    if not tasks:
        await message.reply_text(
            "📋 **No Active Tasks**\n\n"
            "You don't have any forwarding tasks yet.\n\n"
            "Create one with:\n"
            "/forwadd",
            parse_mode="Markdown"
        )
        return

    task_list = "📋 **Your Forwarding Tasks**\n\n"
    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    keyboard = []
    
    for i, task in enumerate(tasks, 1):
        task_list += f"{i}. **{task['label']}**\n"
        task_list += f"   📥 Sources: {', '.join(map(str, task['source_ids']))}\n"
        task_list += f"   📤 Targets: {', '.join(map(str, task['target_ids']))}\n\n"
        
        # Add task as clickable button
        keyboard.append([InlineKeyboardButton(f"{i}. {task['label']}", callback_data=f"task_{task['label']}")])

    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    task_list += f"Total: **{len(tasks)} task(s)**\n\n"
    task_list += "💡 **Tap any task below to manage it!**"

    await message.reply_text(
        task_list,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_task_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show task management menu"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("task_", "")
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return
    
    # Get current filter states
    filters = task.get("filters", {})
    filter_settings = filters.get("filters", {})
    
    # Build status emojis
    outgoing_emoji = "✅" if filters.get("outgoing", True) else "❌"
    forward_tag_emoji = "✅" if filters.get("forward_tag", False) else "❌"
    control_emoji = "✅" if filters.get("control", True) else "❌"
    
    message_text = f"🔧 **Task Management: {task_label}**\n\n"
    message_text += f"📥 **Sources:** {', '.join(map(str, task['source_ids']))}\n"
    message_text += f"📤 **Targets:** {', '.join(map(str, task['target_ids']))}\n\n"
    message_text += "⚙️ **Settings:**\n"
    message_text += f"{outgoing_emoji} Outgoing - Controls if outgoing messages are forwarded\n"
    message_text += f"{forward_tag_emoji} Forward Tag - Shows/hides 'Forwarded from' tag\n"
    message_text += f"{control_emoji} Control - Pauses/runs forwarding\n\n"
    message_text += "💡 **Tap any option below to change it!**"
    
    keyboard = [
        [InlineKeyboardButton("🔍 Filters", callback_data=f"filter_{task_label}")],
        [
            InlineKeyboardButton(f"{outgoing_emoji} Outgoing", callback_data=f"toggle_{task_label}_outgoing"),
            InlineKeyboardButton(f"{forward_tag_emoji} Forward Tag", callback_data=f"toggle_{task_label}_forward_tag")
        ],
        [
            InlineKeyboardButton(f"{control_emoji} Control", callback_data=f"toggle_{task_label}_control"),
            InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{task_label}")
        ],
        [InlineKeyboardButton("🔙 Back to Tasks", callback_data="show_tasks")]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_filter_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show filter management menu"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("filter_", "")
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return
    
    # Get current filter states
    filters = task.get("filters", {})
    filter_settings = filters.get("filters", {})
    
    # Build emojis for each filter
    raw_text_emoji = "✅" if filter_settings.get("raw_text", False) else "❌"
    numbers_only_emoji = "✅" if filter_settings.get("numbers_only", False) else "❌"
    alphabets_only_emoji = "✅" if filter_settings.get("alphabets_only", False) else "❌"
    removed_alphabetic_emoji = "✅" if filter_settings.get("removed_alphabetic", False) else "❌"
    removed_numeric_emoji = "✅" if filter_settings.get("removed_numeric", False) else "❌"
    
    prefix = filter_settings.get("prefix", "")
    suffix = filter_settings.get("suffix", "")
    prefix_text = f"'{prefix}'" if prefix else "Not set"
    suffix_text = f"'{suffix}'" if suffix else "Not set"
    
    message_text = f"🔍 **Filters for: {task_label}**\n\n"
    message_text += "Apply filters to messages before forwarding:\n\n"
    message_text += "📋 **Available Filters:**\n"
    message_text += f"{raw_text_emoji} Raw text - Forward any text\n"
    message_text += f"{numbers_only_emoji} Numbers only - Forward only numbers\n"
    message_text += f"{alphabets_only_emoji} Alphabets only - Forward only letters\n"
    message_text += f"{removed_alphabetic_emoji} Removed Alphabetic - Keep letters & special chars, remove numbers\n"
    message_text += f"{removed_numeric_emoji} Removed Numeric - Keep numbers & special chars, remove letters\n"
    message_text += f"📝 **Prefix:** {prefix_text}\n"
    message_text += f"📝 **Suffix:** {suffix_text}\n\n"
    message_text += "💡 **Multiple filters can be active at once!**"
    
    keyboard = [
        [
            InlineKeyboardButton(f"{raw_text_emoji} Raw text", callback_data=f"toggle_{task_label}_raw_text"),
            InlineKeyboardButton(f"{numbers_only_emoji} Numbers only", callback_data=f"toggle_{task_label}_numbers_only")
        ],
        [
            InlineKeyboardButton(f"{alphabets_only_emoji} Alphabets only", callback_data=f"toggle_{task_label}_alphabets_only"),
            InlineKeyboardButton(f"{removed_alphabetic_emoji} Removed Alphabetic", callback_data=f"toggle_{task_label}_removed_alphabetic")
        ],
        [
            InlineKeyboardButton(f"{removed_numeric_emoji} Removed Numeric", callback_data=f"toggle_{task_label}_removed_numeric"),
            InlineKeyboardButton("📝 Prefix/Suffix", callback_data=f"toggle_{task_label}_prefix_suffix")
        ],
        [InlineKeyboardButton("🔙 Back to Task", callback_data=f"task_{task_label}")]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_toggle_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle toggle actions for filters and settings"""
    query = update.callback_query
    user_id = query.from_user.id
    data_parts = query.data.replace("toggle_", "").split("_")
    
    if len(data_parts) < 2:
        await query.answer("Invalid action!", show_alert=True)
        return
    
    task_label = data_parts[0]
    toggle_type = "_".join(data_parts[1:])
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task_index = -1
    for i, t in enumerate(user_tasks):
        if t["label"] == task_label:
            task_index = i
            break
    
    if task_index == -1:
        await query.answer("Task not found!", show_alert=True)
        return
    
    task = user_tasks[task_index]
    filters = task.get("filters", {})
    
    # Handle different toggle types
    if toggle_type == "outgoing":
        filters["outgoing"] = not filters.get("outgoing", True)
        status = "✅ On" if filters["outgoing"] else "❌ Off"
        await query.answer(f"Outgoing messages: {status}")
        
    elif toggle_type == "forward_tag":
        filters["forward_tag"] = not filters.get("forward_tag", False)
        status = "✅ On" if filters["forward_tag"] else "❌ Off"
        await query.answer(f"Forward tag: {status}")
        
    elif toggle_type == "control":
        filters["control"] = not filters.get("control", True)
        status = "✅ On" if filters["control"] else "❌ Off"
        await query.answer(f"Forwarding control: {status}")
        
    elif toggle_type in ["raw_text", "numbers_only", "alphabets_only", "removed_alphabetic", "removed_numeric"]:
        filter_settings = filters.get("filters", {})
        filter_settings[toggle_type] = not filter_settings.get(toggle_type, False)
        filters["filters"] = filter_settings
        status = "✅ On" if filter_settings[toggle_type] else "❌ Off"
        await query.answer(f"{toggle_type.replace('_', ' ').title()}: {status}")
        
    elif toggle_type == "prefix_suffix":
        # Show prefix/suffix setup menu
        await show_prefix_suffix_menu(query, task_label)
        return
    
    elif toggle_type == "clear_prefix_suffix":
        # Clear prefix and suffix
        filter_settings = filters.get("filters", {})
        filter_settings["prefix"] = ""
        filter_settings["suffix"] = ""
        filters["filters"] = filter_settings
        await query.answer("✅ Prefix and suffix cleared!")
    
    else:
        await query.answer(f"Unknown toggle type: {toggle_type}")
        return
    
    # Update task in cache
    task["filters"] = filters
    tasks_cache[user_id][task_index] = task
    
    # Update task in database (async)
    try:
        asyncio.create_task(
            db_call(db.update_task_filters, user_id, task_label, filters)
        )
    except Exception as e:
        logger.exception("Error updating task filters in DB: %s", e)
    
    # Show appropriate menu based on toggle type
    if toggle_type in ["outgoing", "forward_tag", "control"]:
        await handle_task_menu(update, context)
    elif toggle_type in ["raw_text", "numbers_only", "alphabets_only", "removed_alphabetic", "removed_numeric", "clear_prefix_suffix"]:
        await handle_filter_menu(update, context)


async def show_prefix_suffix_menu(query, task_label):
    """Show menu for setting prefix/suffix"""
    user_id = query.from_user.id
    
    # Find the task to get current prefix/suffix
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return
    
    filters = task.get("filters", {})
    filter_settings = filters.get("filters", {})
    prefix = filter_settings.get("prefix", "")
    suffix = filter_settings.get("suffix", "")
    
    message_text = f"🔤 **Prefix/Suffix Setup for: {task_label}**\n\n"
    message_text += "Add custom text to messages:\n\n"
    message_text += f"📝 **Current Prefix:** '{prefix}'\n"
    message_text += f"📝 **Current Suffix:** '{suffix}'\n\n"
    message_text += "💡 **Examples:**\n"
    message_text += "• Prefix '🔔 ' adds a bell before each message\n"
    message_text += "• Suffix ' ✅' adds a checkmark after\n"
    message_text += "• Use any characters: emojis, signs, numbers, letters\n\n"
    message_text += "**Tap an option below to set it!**"
    
    keyboard = [
        [InlineKeyboardButton("➕ Set Prefix", callback_data=f"prefix_{task_label}_set")],
        [InlineKeyboardButton("➕ Set Suffix", callback_data=f"suffix_{task_label}_set")],
        [InlineKeyboardButton("🗑️ Clear Prefix/Suffix", callback_data=f"toggle_{task_label}_clear_prefix_suffix")],
        [InlineKeyboardButton("🔙 Back to Filters", callback_data=f"filter_{task_label}")]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_prefix_suffix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle prefix/suffix setup"""
    query = update.callback_query
    user_id = query.from_user.id
    data_parts = query.data.split("_")
    
    if len(data_parts) < 3:
        await query.answer("Invalid action!", show_alert=True)
        return
    
    action_type = data_parts[0]  # prefix or suffix
    task_label = data_parts[1]
    action = data_parts[2] if len(data_parts) > 2 else ""
    
    if action == "set":
        # Store state and ask for input
        context.user_data[f"waiting_{action_type}"] = task_label
        await query.edit_message_text(
            f"📝 **Enter the {action_type} text for task '{task_label}':**\n\n"
            f"Type your {action_type} text now.\n"
            f"💡 *You can use any characters: emojis 🔔, signs ⚠️, numbers 123, letters ABC*\n\n"
            f"**Example:** If you want the {action_type} '🔔 ', type: 🔔 ",
            parse_mode="Markdown"
        )
    else:
        await query.answer(f"Action: {action_type} {action}")


async def handle_prefix_suffix_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle prefix/suffix text input"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    # Check if we're waiting for prefix or suffix
    waiting_prefix = context.user_data.get("waiting_prefix")
    waiting_suffix = context.user_data.get("waiting_suffix")
    
    if waiting_prefix:
        task_label = waiting_prefix
        action_type = "prefix"
        del context.user_data["waiting_prefix"]
    elif waiting_suffix:
        task_label = waiting_suffix
        action_type = "suffix"
        del context.user_data["waiting_suffix"]
    else:
        return
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task_index = -1
    for i, t in enumerate(user_tasks):
        if t["label"] == task_label:
            task_index = i
            break
    
    if task_index == -1:
        await update.message.reply_text("❌ Task not found!")
        return
    
    task = user_tasks[task_index]
    filters = task.get("filters", {})
    filter_settings = filters.get("filters", {})
    
    # Set the prefix or suffix
    if action_type == "prefix":
        filter_settings["prefix"] = text
        confirmation = f"✅ **Prefix set to:** '{text}'"
    else:
        filter_settings["suffix"] = text
        confirmation = f"✅ **Suffix set to:** '{text}'"
    
    filters["filters"] = filter_settings
    
    # Update task in cache
    task["filters"] = filters
    tasks_cache[user_id][task_index] = task
    
    # Update task in database (async)
    try:
        asyncio.create_task(
            db_call(db.update_task_filters, user_id, task_label, filters)
        )
    except Exception as e:
        logger.exception("Error updating task filters in DB: %s", e)
    
    await update.message.reply_text(
        f"{confirmation}\n\n"
        f"Task: **{task_label}**\n\n"
        "All messages will now include this text when forwarded!",
        parse_mode="Markdown"
    )


async def handle_delete_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle task deletion"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("delete_", "")
    
    message_text = f"🗑️ **Delete Task: {task_label}**\n\n"
    message_text += "⚠️ **Are you sure you want to delete this task?**\n\n"
    message_text += "This action cannot be undone!\n"
    message_text += "All forwarding will stop immediately."
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_{task_label}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"task_{task_label}")
        ]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Confirm and execute task deletion"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("confirm_delete_", "")
    
    # Delete task from database
    deleted = await db_call(db.remove_forwarding_task, user_id, task_label)
    
    if deleted:
        # Update in-memory cache
        if user_id in tasks_cache:
            tasks_cache[user_id] = [t for t in tasks_cache[user_id] if t.get("label") != task_label]
        
        await query.edit_message_text(
            f"✅ **Task '{task_label}' deleted successfully!**\n\n"
            "All forwarding for this task has been stopped.",
            parse_mode="Markdown"
        )
    else:
        await query.edit_message_text(
            f"❌ **Task '{task_label}' not found!**",
            parse_mode="Markdown"
        )


# ---------- Login/logout commands ----------
async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    # OPTIMIZED: Check current user count
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

    # First check if we're in task creation
    if user_id in task_creation_states:
        await handle_task_creation(update, context)
        return
    
    # Check if we're waiting for prefix/suffix input
    if context.user_data.get("waiting_prefix") or context.user_data.get("waiting_suffix"):
        await handle_prefix_suffix_input(update, context)
        return
    
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
                    "/forwadd",
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
            "/adduser [USER_ID] - Add regular user\n"
            "/adduser [USER_ID] admin - Add admin user",
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


# ---------- OPTIMIZED Forwarding core: handler registration, message handler, send worker, resolver ----------
def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    """Attach a NewMessage handler once per client/user to avoid duplicates and store the handler (so it can be removed)."""
    if handler_registered.get(user_id):
        return

    async def _hot_message_handler(event):
        try:
            # OPTIMIZED: Batch process messages and run GC periodically
            await optimized_gc()
            
            # Get message details
            message = getattr(event, "message", None)
            if not message:
                return
                
            message_text = getattr(event, "raw_text", None) or getattr(message, "message", None)
            if not message_text:
                return

            chat_id = getattr(event, "chat_id", None) or getattr(message, "chat_id", None)
            if chat_id is None:
                return

            user_tasks = tasks_cache.get(user_id)
            if not user_tasks:
                return

            # Check if message is outgoing (from the user themselves)
            message_outgoing = getattr(message, "out", False)
            
            for task in user_tasks:
                # Skip if task control is off
                if not task.get("filters", {}).get("control", True):
                    continue
                    
                # Skip if message is outgoing and outgoing filter is off
                if message_outgoing and not task.get("filters", {}).get("outgoing", True):
                    continue
                    
                if chat_id in task.get("source_ids", []):
                    # Get forward tag setting
                    forward_tag = task.get("filters", {}).get("forward_tag", False)
                    
                    # Apply filters to message
                    filtered_messages = apply_filters(message_text, task.get("filters", {}))
                    
                    for filtered_msg in filtered_messages:
                        for target_id in task.get("target_ids", []):
                            try:
                                # ensure send_queue exists and is on running loop
                                global send_queue
                                if send_queue is None:
                                    logger.debug("Send queue not initialized; dropping forward job")
                                    continue
                                    
                                # Pass source chat ID and message ID for forwarding
                                await send_queue.put((user_id, client, int(target_id), filtered_msg, 
                                                     task.get("filters", {}), forward_tag, 
                                                     chat_id if forward_tag else None,  # Add source chat ID
                                                     message.id if forward_tag else None))
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
            # Updated to include source_chat_id
            user_id, client, target_id, message_text, task_filters, forward_tag, source_chat_id, message_id = await send_queue.get()
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
                if forward_tag and source_chat_id and message_id:
                    # Forward the original message with tag
                    try:
                        source_entity = await client.get_input_entity(int(source_chat_id))
                        # Forward the original message
                        await client.forward_messages(entity, message_id, source_entity)
                        logger.debug("Forwarded message with tag for user %s to %s", user_id, target_id)
                    except Exception as e:
                        logger.warning("Failed to forward with tag, falling back to regular send: %s", e)
                        # Fallback to regular message if forwarding fails
                        await client.send_message(entity, message_text)
                else:
                    # Send as regular message without tag
                    await client.send_message(entity, message_text)
                    logger.debug("Forwarded message without tag for user %s to %s", user_id, target_id)
                    
            except FloodWaitError as fwe:
                wait = int(getattr(fwe, "seconds", 10))
                logger.warning("FloodWait for %s seconds. Pausing worker %d", wait, worker_id)
                await asyncio.sleep(wait + 1)
                try:
                    # Update queue put with correct number of arguments
                    await send_queue.put((user_id, client, target_id, message_text, task_filters, forward_tag, source_chat_id, message_id))
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
            "filters": t.get("filters", {})
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

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("logout", logout_command))
    application.add_handler(CommandHandler("forwadd", forwadd_command))
    application.add_handler(CommandHandler("fortasks", fortasks_command))
    application.add_handler(CommandHandler("getallid", getallid_command))
    application.add_handler(CommandHandler("adduser", adduser_command))
    application.add_handler(CommandHandler("removeuser", removeuser_command))
    application.add_handler(CommandHandler("listusers", listusers_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Add separate handler for delete confirmations
    application.add_handler(CallbackQueryHandler(handle_confirm_delete, pattern=r"^confirm_delete_"))
    
    # Add message handlers for various states
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_login_process))

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
