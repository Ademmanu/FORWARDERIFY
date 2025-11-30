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
send_queue: Optional[asyncio.Queue[Tuple[int, TelegramClient, int, str, bool]]] = None

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

# Task creation states
task_creation_states: Dict[int, Dict] = {}

# Filter types
FILTER_RAW_TEXT = "raw_text"
FILTER_NUMBERS_ONLY = "numbers_only" 
FILTER_ALPHABETS_ONLY = "alphabets_only"
FILTER_REMOVED_NUMBERS = "removed_numbers"
FILTER_REMOVED_ALPHABETS = "removed_alphabets"
FILTER_PREFIX_SUFFIX = "prefix_suffix"


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
def apply_filters(message_text: str, task_filters: List[str], prefix: str = "", suffix: str = "") -> Optional[str]:
    """
    Apply filters to message text and return filtered text or None if message should be filtered out.
    """
    if not message_text:
        return None
        
    # If no filters or raw_text filter is enabled, forward everything
    if not task_filters or FILTER_RAW_TEXT in task_filters:
        return f"{prefix}{message_text}{suffix}"
    
    # Check each filter condition
    filtered = False
    
    for filter_type in task_filters:
        if filter_type == FILTER_NUMBERS_ONLY:
            if not message_text.strip().isdigit():
                filtered = True
                break
                
        elif filter_type == FILTER_ALPHABETS_ONLY:
            if not message_text.strip().isalpha():
                filtered = True
                break
                
        elif filter_type == FILTER_REMOVED_NUMBERS:
            # Remove numbers and keep only alphabets and other characters
            message_text = re.sub(r'\d+', '', message_text)
            
        elif filter_type == FILTER_REMOVED_ALPHABETS:
            # Remove alphabets and keep only numbers and other characters
            message_text = re.sub(r'[a-zA-Z]+', '', message_text)
    
    if filtered:
        return None
        
    return f"{prefix}{message_text}{suffix}"


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
3. Configure filters and settings for each task
4. Bot forwards messages based on your settings!

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
    elif query.data.startswith("task_"):
        await handle_task_management(update, context)
    elif query.data.startswith("filter_"):
        await handle_filter_management(update, context)
    elif query.data.startswith("chatids_"):
        user_id = query.from_user.id
        if query.data == "chatids_back":
            await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
        else:
            parts = query.data.split("_")
            category = parts[1]
            page = int(parts[2])
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context)


# ---------- Task Management with Inline Buttons ----------
async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            "`/forwadd`",
            parse_mode="Markdown",
        )
        return

    message_text = "📋 **Your Forwarding Tasks**\n\n"
    message_text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    message_text += "💡 **Click on any task to manage its settings**\n\n"

    keyboard = []
    for task in tasks:
        status_emoji = "🟢" if task.get("control_enabled", True) else "🔴"
        keyboard.append([InlineKeyboardButton(f"{status_emoji} {task['label']}", callback_data=f"task_{task['label']}")])

    # Add back button if this was called from callback
    if update.callback_query:
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="show_tasks_back")])

    await message.reply_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


async def handle_task_management(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    
    if query.data == "show_tasks_back":
        await fortasks_command(update, context)
        return
        
    # Extract task label from callback data (task_<label>)
    task_label = query.data[5:]  # Remove "task_" prefix
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await query.answer("❌ Task not found!", show_alert=True)
        return

    # Build task management message
    outgoing_emoji = "✅" if task.get("outgoing_enabled", False) else "❌"
    forward_tag_emoji = "✅" if task.get("forward_tag_enabled", False) else "❌"
    control_emoji = "✅" if task.get("control_enabled", True) else "❌"
    
    active_filters = task.get("filters", [])
    filters_status = "None" if not active_filters else ", ".join(active_filters)
    
    message_text = f"""
🔧 **Task Management: {task_label}**

📥 **Sources:** {', '.join(map(str, task['source_ids']))}
📤 **Targets:** {', '.join(map(str, task['target_ids']))}

⚙️ **Current Settings:**
• Filters: {filters_status}
• Prefix: `{task.get('prefix', '')}`
• Suffix: `{task.get('suffix', '')}`

💡 **Click buttons below to toggle settings:**
"""

    keyboard = [
        [InlineKeyboardButton(f"🔧 Filters {get_filter_status_emoji(active_filters)}", callback_data=f"filter_{task_label}")],
        [InlineKeyboardButton(f"{outgoing_emoji} Outgoing Messages", callback_data=f"task_{task_label}_outgoing")],
        [InlineKeyboardButton(f"{forward_tag_emoji} Forward Tag", callback_data=f"task_{task_label}_forward_tag")],
        [InlineKeyboardButton(f"{control_emoji} Task Control", callback_data=f"task_{task_label}_control")],
        [InlineKeyboardButton("🗑️ Delete Task", callback_data=f"task_{task_label}_delete")],
        [InlineKeyboardButton("🔙 Back to Tasks", callback_data="show_tasks_back")]
    ]

    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


def get_filter_status_emoji(filters: List[str]) -> str:
    """Return emoji based on filter status"""
    if not filters:
        return "❌"
    elif FILTER_RAW_TEXT in filters:
        return "🔵"
    else:
        return "✅"


async def handle_filter_management(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    
    # Extract task label from callback data (filter_<label>)
    task_label = query.data[7:]  # Remove "filter_" prefix
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await query.answer("❌ Task not found!", show_alert=True)
        return

    current_filters = task.get("filters", [])
    
    message_text = f"""
🔧 **Filter Settings: {task_label}**

💡 **Select filters to apply:**
• Multiple filters can be active at once
• "Raw Text" forwards everything (overrides other filters)
• Other filters work together when combined

📋 **Current active filters:**
{', '.join(current_filters) if current_filters else 'None'}
"""

    keyboard = []
    
    # Filter buttons with status emojis
    filters_config = [
        (FILTER_RAW_TEXT, "📝 Raw Text"),
        (FILTER_NUMBERS_ONLY, "🔢 Numbers Only"),
        (FILTER_ALPHABETS_ONLY, "🔤 Alphabets Only"),
        (FILTER_REMOVED_NUMBERS, "🚫 Remove Numbers"),
        (FILTER_REMOVED_ALPHABETS, "🚫 Remove Alphabets"),
        (FILTER_PREFIX_SUFFIX, "🏷️ Prefix/Suffix")
    ]
    
    # Create two columns for filters
    for i in range(0, len(filters_config), 2):
        row = []
        for j in range(2):
            if i + j < len(filters_config):
                filter_type, display_name = filters_config[i + j]
                emoji = "✅" if filter_type in current_filters else "❌"
                row.append(InlineKeyboardButton(f"{emoji} {display_name}", callback_data=f"filter_{task_label}_{filter_type}"))
        if row:
            keyboard.append(row)
    
    # Add control buttons
    keyboard.append([InlineKeyboardButton("💾 Save & Back", callback_data=f"task_{task_label}")])
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


async def handle_task_setting_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    
    # Parse callback data: task_<label>_<setting>
    parts = query.data.split("_")
    if len(parts) < 3:
        await query.answer("❌ Invalid command!", show_alert=True)
        return
        
    task_label = parts[1]
    setting = parts[2]
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task_index = None
    task = None
    for i, t in enumerate(user_tasks):
        if t["label"] == task_label:
            task = t
            task_index = i
            break
    
    if not task:
        await query.answer("❌ Task not found!", show_alert=True)
        return

    # Update the setting
    new_value = None
    
    if setting == "outgoing":
        new_value = not task.get("outgoing_enabled", False)
        task["outgoing_enabled"] = new_value
        await db_call(db.update_task_settings, user_id, task_label, {"outgoing_enabled": new_value})
        
    elif setting == "forward_tag":
        new_value = not task.get("forward_tag_enabled", False)
        task["forward_tag_enabled"] = new_value
        await db_call(db.update_task_settings, user_id, task_label, {"forward_tag_enabled": new_value})
        
    elif setting == "control":
        new_value = not task.get("control_enabled", True)
        task["control_enabled"] = new_value
        await db_call(db.update_task_settings, user_id, task_label, {"control_enabled": new_value})
        
    elif setting == "delete":
        # Confirm deletion
        keyboard = [
            [InlineKeyboardButton("✅ Confirm Delete", callback_data=f"task_{task_label}_delete_confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"task_{task_label}")]
        ]
        
        await query.edit_message_text(
            f"🗑️ **Confirm Deletion**\n\n"
            f"Are you sure you want to delete task **{task_label}**?\n\n"
            f"📥 Sources: {', '.join(map(str, task['source_ids']))}\n"
            f"📤 Targets: {', '.join(map(str, task['target_ids']))}\n\n"
            f"⚠️ **This action cannot be undone!**",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return
        
    elif setting == "delete_confirm":
        # Actually delete the task
        deleted = await db_call(db.remove_forwarding_task, user_id, task_label)
        if deleted:
            # Update cache
            if user_id in tasks_cache:
                tasks_cache[user_id] = [t for t in tasks_cache[user_id] if t.get("label") != task_label]
            
            await query.edit_message_text(
                f"✅ **Task '{task_label}' deleted successfully!**",
                parse_mode="Markdown"
            )
        else:
            await query.answer("❌ Failed to delete task!", show_alert=True)
        return

    # Update the task in cache
    if task_index is not None:
        tasks_cache[user_id][task_index] = task

    # Show updated task management screen
    await handle_task_management(update, context)


async def handle_filter_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    
    # Parse callback data: filter_<label>_<filter_type>
    parts = query.data.split("_")
    if len(parts) < 3:
        await query.answer("❌ Invalid command!", show_alert=True)
        return
        
    task_label = parts[1]
    filter_type = parts[2]
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await query.answer("❌ Task not found!", show_alert=True)
        return

    current_filters = task.get("filters", [])
    
    # Handle prefix/suffix separately
    if filter_type == FILTER_PREFIX_SUFFIX:
        if FILTER_PREFIX_SUFFIX in current_filters:
            # Already enabled, just go back
            await handle_filter_management(update, context)
            return
        else:
            # Enable prefix/suffix and ask for values
            current_filters.append(FILTER_PREFIX_SUFFIX)
            await db_call(db.update_task_settings, user_id, task_label, {"filters": current_filters})
            
            # Store state to collect prefix/suffix
            task_creation_states[user_id] = {
                "step": "waiting_prefix",
                "task_label": task_label
            }
            
            await query.edit_message_text(
                "🏷️ **Prefix/Suffix Setup**\n\n"
                "Please enter the prefix text (or send '-' for no prefix):",
                parse_mode="Markdown"
            )
            return
    
    # Toggle regular filter
    if filter_type in current_filters:
        current_filters.remove(filter_type)
    else:
        current_filters.append(filter_type)
    
    # Update in database
    await db_call(db.update_task_settings, user_id, task_label, {"filters": current_filters})
    
    # Update cache
    task["filters"] = current_filters
    
    # Show updated filter management screen
    await handle_filter_management(update, context)


# ---------- New /forwadd step-by-step process ----------
async def forwadd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\n" "Use /login to connect your Telegram account.", parse_mode="Markdown"
        )
        return

    # Start task creation process
    task_creation_states[user_id] = {"step": "waiting_name"}
    
    await update.message.reply_text(
        "🆕 **Creating New Forwarding Task**\n\n"
        "📝 **Step 1: Enter a name for your task**\n\n"
        "💡 **Example:** `mytask` or `channel_forward`\n\n"
        "This name will help you identify the task later.",
        parse_mode="Markdown"
    )


async def handle_task_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in task_creation_states:
        return
        
    state = task_creation_states[user_id]
    text = update.message.text.strip()
    
    try:
        if state["step"] == "waiting_name":
            # Validate task name
            if not text:
                await update.message.reply_text("❌ **Task name cannot be empty!**\n\nPlease enter a valid task name:")
                return
                
            # Check if task name already exists
            user_tasks = tasks_cache.get(user_id, [])
            for task in user_tasks:
                if task["label"] == text:
                    await update.message.reply_text(
                        f"❌ **Task '{text}' already exists!**\n\n"
                        "Please choose a different name:"
                    )
                    return
            
            state["label"] = text
            state["step"] = "waiting_sources"
            
            await update.message.reply_text(
                f"✅ **Task name set to: {text}**\n\n"
                "📥 **Step 2: Enter source chat ID(s)**\n\n"
                "💡 **Examples:**\n"
                "• Single ID: `123456789`\n"
                "• Multiple IDs: `123456789, 987654321, 555555555`\n\n"
                "Enter the chat ID(s) where messages will be forwarded FROM:",
                parse_mode="Markdown"
            )
            
        elif state["step"] == "waiting_sources":
            # Parse source IDs
            try:
                source_ids = [int(x.strip()) for x in text.split(",")]
                state["source_ids"] = source_ids
                state["step"] = "waiting_targets"
                
                await update.message.reply_text(
                    f"✅ **Source IDs set to: {', '.join(map(str, source_ids))}**\n\n"
                    "📤 **Step 3: Enter target chat ID(s)**\n\n"
                    "💡 **Examples:**\n"
                    "• Single ID: `987654321`\n"
                    "• Multiple IDs: `111111111, 222222222, 333333333`\n\n"
                    "Enter the chat ID(s) where messages will be forwarded TO:",
                    parse_mode="Markdown"
                )
                
            except ValueError:
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please enter valid numeric chat IDs separated by commas.\n\n"
                    "💡 **Example:** `123456789, 987654321`"
                )
                
        elif state["step"] == "waiting_targets":
            # Parse target IDs
            try:
                target_ids = [int(x.strip()) for x in text.split(",")]
                state["target_ids"] = target_ids
                
                # Create the task
                label = state["label"]
                source_ids = state["source_ids"]
                target_ids = state["target_ids"]
                
                added = await db_call(db.add_forwarding_task, user_id, label, source_ids, target_ids)
                if added:
                    # Update in-memory cache
                    tasks_cache.setdefault(user_id, [])
                    tasks_cache[user_id].append({
                        "id": None, 
                        "label": label, 
                        "source_ids": source_ids, 
                        "target_ids": target_ids, 
                        "is_active": 1,
                        "filters": [],
                        "outgoing_enabled": False,
                        "forward_tag_enabled": False,
                        "control_enabled": True,
                        "prefix": "",
                        "suffix": ""
                    })
                    
                    # Schedule async resolve of target entities
                    try:
                        asyncio.create_task(resolve_targets_for_user(user_id, target_ids))
                    except Exception:
                        logger.exception("Failed to schedule resolve_targets_for_user task")

                    await update.message.reply_text(
                        f"✅ **Task '{label}' created successfully!** 🎉\n\n"
                        f"📥 **Sources:** {', '.join(map(str, source_ids))}\n"
                        f"📤 **Targets:** {', '.join(map(str, target_ids))}\n\n"
                        "⚙️ **Use** `/fortasks` **to manage your task settings and filters!**",
                        parse_mode="Markdown",
                    )
                else:
                    await update.message.reply_text(
                        f"❌ **Failed to create task '{label}'!**\n\n"
                        "A task with this name might already exist.",
                        parse_mode="Markdown"
                    )
                
                # Clean up state
                del task_creation_states[user_id]
                
            except ValueError:
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please enter valid numeric chat IDs separated by commas.\n\n"
                    "💡 **Example:** `987654321, 123456789`"
                )
                
        elif state["step"] == "waiting_prefix":
            task_label = state["task_label"]
            prefix = text if text != "-" else ""
            
            state["prefix"] = prefix
            state["step"] = "waiting_suffix"
            
            await update.message.reply_text(
                f"✅ **Prefix set to:** `{prefix if prefix else 'None'}`\n\n"
                "Now enter the suffix text (or send '-' for no suffix):",
                parse_mode="Markdown"
            )
            
        elif state["step"] == "waiting_suffix":
            task_label = state["task_label"]
            prefix = state["prefix"]
            suffix = text if text != "-" else ""
            
            # Update task with prefix/suffix
            await db_call(db.update_task_settings, user_id, task_label, {
                "prefix": prefix,
                "suffix": suffix
            })
            
            # Update cache
            user_tasks = tasks_cache.get(user_id, [])
            for task in user_tasks:
                if task["label"] == task_label:
                    task["prefix"] = prefix
                    task["suffix"] = suffix
                    break
            
            await update.message.reply_text(
                f"✅ **Prefix/Suffix configured!**\n\n"
                f"• Prefix: `{prefix if prefix else 'None'}`\n"
                f"• Suffix: `{suffix if suffix else 'None'}`\n\n"
                "Your task will now apply these to forwarded messages.",
                parse_mode="Markdown"
            )
            
            # Clean up state
            del task_creation_states[user_id]
            
    except Exception as e:
        logger.exception("Error in task creation for user %s", user_id)
        await update.message.reply_text(
            f"❌ **Error creating task:** {str(e)}\n\n"
            "Please try /forwadd again.",
            parse_mode="Markdown"
        )
        if user_id in task_creation_states:
            del task_creation_states[user_id]


# ---------- Login/logout and other commands ----------
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

    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return

    if user_id in task_creation_states:
        await handle_task_creation(update, context)
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
            
            # Check if message is from user (outgoing)
            is_outgoing = event.message.out if hasattr(event, 'message') and hasattr(event.message, 'out') else False
            
            # Prefer raw_text for Telethon messages, fallback to message.message
            message_text = getattr(event, "raw_text", None) or getattr(getattr(event, "message", None), "message", None)
            if not message_text:
                return

            chat_id = getattr(event, "chat_id", None) or getattr(getattr(event, "message", None), "chat_id", None)
            if chat_id is None:
                return

            user_tasks = tasks_cache.get(user_id)
            if not user_tasks:
                return

            for task in user_tasks:
                # Skip if task control is disabled
                if not task.get("control_enabled", True):
                    continue
                    
                # Skip outgoing messages if not enabled
                if is_outgoing and not task.get("outgoing_enabled", False):
                    continue

                if chat_id in task.get("source_ids", []):
                    # Apply filters to message
                    filtered_text = apply_filters(
                        message_text, 
                        task.get("filters", []),
                        task.get("prefix", ""),
                        task.get("suffix", "")
                    )
                    
                    if filtered_text is None:
                        continue  # Message filtered out

                    for target_id in task.get("target_ids", []):
                        try:
                            # ensure send_queue exists and is on running loop
                            global send_queue
                            if send_queue is None:
                                logger.debug("Send queue not initialized; dropping forward job")
                                continue
                            await send_queue.put((user_id, client, int(target_id), filtered_text, task.get("forward_tag_enabled", False)))
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
            user_id, client, target_id, message_text, show_forward_tag = await send_queue.get()
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
                if show_forward_tag:
                    # Forward as is (with forward tag)
                    await client.forward_messages(entity, message_text)
                else:
                    # Send as new message (no forward tag)
                    await client.send_message(entity, message_text)
                logger.debug("Forwarded message for user %s to %s", user_id, target_id)
            except FloodWaitError as fwe:
                wait = int(getattr(fwe, "seconds", 10))
                logger.warning("FloodWait for %s seconds. Pausing worker %d", wait, worker_id)
                await asyncio.sleep(wait + 1)
                try:
                    await send_queue.put((user_id, client, target_id, message_text, show_forward_tag))
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
            "filters": t["filters"],
            "outgoing_enabled": t["outgoing_enabled"],
            "forward_tag_enabled": t["forward_tag_enabled"],
            "control_enabled": t["control_enabled"],
            "prefix": t["prefix"],
            "suffix": t["suffix"]
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
    
    # Add specific callback handlers for task management
    application.add_handler(CallbackQueryHandler(handle_task_setting_toggle, pattern="^task_.*_(outgoing|forward_tag|control|delete|delete_confirm)$"))
    application.add_handler(CallbackQueryHandler(handle_filter_toggle, pattern="^filter_.*"))
    
    # Handle both login process and task creation
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
