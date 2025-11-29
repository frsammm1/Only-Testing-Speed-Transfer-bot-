import os
import asyncio
import logging
import time
import math
import re
import mimetypes
from telethon import TelegramClient, events, utils, errors
from telethon.sessions import StringSession
from telethon.tl.types import (
    DocumentAttributeFilename, 
    DocumentAttributeVideo, 
    DocumentAttributeAudio,
    MessageMediaWebPage
)
from aiohttp import web

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH")
STRING_SESSION = os.environ.get("STRING_SESSION") 
BOT_TOKEN = os.environ.get("BOT_TOKEN")           
PORT = int(os.environ.get("PORT", 8080))

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CLIENT SETUP (MAX CONNECTIONS) ---
user_client = TelegramClient(
    StringSession(STRING_SESSION), 
    API_ID, 
    API_HASH, 
    connection_retries=None, 
    flood_sleep_threshold=60,
    request_retries=10,
    auto_reconnect=True
)

bot_client = TelegramClient(
    'bot_session', 
    API_ID, 
    API_HASH, 
    connection_retries=None, 
    flood_sleep_threshold=60,
    request_retries=10,
    auto_reconnect=True
)

# --- GLOBAL STATE ---
pending_requests = {} 
current_task = None
is_running = False
status_message = None
last_update_time = 0

# --- WEB SERVER ---
async def handle(request):
    return web.Response(text="🔥 Ultra Bot Running (72MB Buffer Mode) - Status: Active")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Web server started on port {PORT}")

# --- HELPER FUNCTIONS ---
def human_readable_size(size):
    if not size: return "0B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0: return f"{size:.2f}{unit}"
        size /= 1024.0
    return f"{size:.2f}TB"

def time_formatter(seconds):
    if seconds is None or seconds < 0: return "..."
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0: return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"

# --- PROGRESS CALLBACK ---
async def progress_callback(current, total, start_time, file_name):
    global last_update_time, status_message
    now = time.time()
    
    if now - last_update_time < 5: return # Update every 5 seconds to avoid flood
    last_update_time = now
    
    percentage = current * 100 / total if total > 0 else 0
    time_diff = now - start_time
    speed = current / time_diff if time_diff > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0
    
    filled = math.floor(percentage / 10)
    bar = "█" * filled + "░" * (10 - filled)
    
    try:
        await status_message.edit(
            f"⚡️ **Ultra Speed Transfer...**\n"
            f"📂 `{file_name}`\n"
            f"**{bar} {round(percentage, 1)}%**\n"
            f"🚀 `{human_readable_size(speed)}/s` | ⏳ `{time_formatter(eta)}`\n"
            f"💾 `{human_readable_size(current)} / {human_readable_size(total)}`"
        )
    except Exception: pass

# --- ULTRA BUFFERED STREAM (72MB BUFFER) ---
class UltraBufferedStream:
    def __init__(self, client, location, file_size, file_name, start_time):
        self.client = client
        self.location = location
        self.file_size = file_size
        self.name = file_name
        self.start_time = start_time
        self.current_bytes = 0
        
        # 8MB Chunks (Best for Telegram Uploads)
        self.chunk_size = 8 * 1024 * 1024 
        
        # MEGA BUFFER: 9 Chunks = 72MB in RAM
        self.queue = asyncio.Queue(maxsize=9) 
        
        self.downloader_task = asyncio.create_task(self._worker())
        self.buffer = b""

    async def _worker(self):
        """Pre-fetches chunks into RAM"""
        try:
            # Iter_download uses parallel workers if cryptg is installed
            async for chunk in self.client.iter_download(self.location, chunk_size=self.chunk_size):
                await self.queue.put(chunk)
            await self.queue.put(None) 
        except Exception as e:
            logger.error(f"Stream Worker Error: {e}")
            await self.queue.put(None)

    def __len__(self):
        return self.file_size

    async def read(self, size=-1):
        if size == -1: size = self.chunk_size
        
        while len(self.buffer) < size:
            chunk = await self.queue.get()
            
            if chunk is None: 
                # If stream ended normally or error occurred in worker
                if self.current_bytes < self.file_size:
                     # This triggers retry in the main loop
                    raise errors.RpcCallFailError("Incomplete Stream")
                break
                
            self.buffer += chunk
            self.current_bytes += len(chunk)
            
            # Progress update task
            asyncio.create_task(progress_callback(self.current_bytes, self.file_size, self.start_time, self.name))

        data = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return data

# --- FILE INFO HELPER ---
def get_file_info(message):
    file_name = "Unknown_File"
    mime_type = "application/octet-stream"
    
    # Handle WebPage Media (Fix for AttributeError)
    if isinstance(message.media, MessageMediaWebPage):
        return None, None # Skip download logic for pure links

    if message.file:
        mime_type = message.file.mime_type
        if message.file.name:
            file_name = message.file.name
        else:
            ext = mimetypes.guess_extension(mime_type) or ""
            if not ext:
                if "video" in mime_type: ext = ".mp4"
                elif "image" in mime_type: ext = ".jpg"
                elif "pdf" in mime_type: ext = ".pdf"
            file_name = f"File_{message.id}{ext}"
            
    # Fix Extensions logic
    if "video" in mime_type and not re.search(r'\.(mp4|mkv|avi|mov|webm)$', file_name, re.IGNORECASE):
        file_name += ".mp4"
    elif "pdf" in mime_type and not file_name.lower().endswith(".pdf"):
        file_name += ".pdf"
        
    return file_name, mime_type

# --- TRANSFER PROCESS (FIXED) ---
async def transfer_process(event, source_id, dest_id, start_msg, end_msg):
    global is_running, status_message
    
    status_message = await event.respond(f"🔥 **Ultra Queue Engine Started!**\nSource: `{source_id}`")
    total_processed = 0
    
    try:
        # Iterate Messages
        async for message in user_client.iter_messages(source_id, min_id=start_msg-1, max_id=end_msg+1, reverse=True):
            if not is_running:
                await status_message.edit("🛑 **Stopped by User!**")
                break

            if getattr(message, 'action', None): continue

            # --- RETRY LOOP (Retry 3 times per file) ---
            retries = 3
            success = False
            
            while retries > 0 and not success:
                try:
                    # 1. REFRESH MESSAGE (Fixes FileReferenceExpired)
                    fresh_msg = await user_client.get_messages(source_id, ids=message.id)
                    if not fresh_msg: break # Message deleted

                    file_name, mime_type = get_file_info(fresh_msg)
                    
                    # Agar WebPage link hai aur koi media nahi
                    if not file_name and fresh_msg.text:
                        await bot_client.send_message(dest_id, fresh_msg.text)
                        success = True
                        continue
                    elif not file_name: 
                        # Unknown media type
                        break

                    await status_message.edit(f"🔍 **Processing:** `{file_name}`\nAttempt: {4-retries}")

                    # 2. START TRANSFER
                    start_time = time.time()
                    
                    # Attempt Direct Copy First (Fastest)
                    if not success:
                        try:
                            # Try copying using Bot API first (saves bandwidth)
                            await bot_client.send_file(dest_id, fresh_msg.media, caption=fresh_msg.text or "")
                            success = True
                            await status_message.edit(f"✅ **Direct Copied:** `{file_name}`")
                        except Exception:
                            pass # Fallback to stream

                    # 3. STREAMING (If Direct Copy Fails)
                    if not success:
                        attributes = fresh_msg.document.attributes if hasattr(fresh_msg, 'document') else []
                        thumb = await user_client.download_media(fresh_msg, thumb=-1)
                        
                        # Create 72MB Buffer Stream
                        stream_file = UltraBufferedStream(
                            user_client, 
                            fresh_msg.media.document if hasattr(fresh_msg.media, 'document') else fresh_msg.media.photo,
                            fresh_msg.file.size,
                            file_name,
                            start_time
                        )
                        
                        force_doc = not ("video" in mime_type or "image" in mime_type)
                        
                        await bot_client.send_file(
                            dest_id,
                            file=stream_file,
                            caption=fresh_msg.text or "",
                            attributes=attributes,
                            thumb=thumb,
                            supports_streaming=True,
                            file_size=fresh_msg.file.size,
                            force_document=force_doc,
                            part_size_kb=8192 # FORCE 8MB CHUNKS
                        )
                        
                        if thumb and os.path.exists(thumb): os.remove(thumb)
                        success = True
                        await status_message.edit(f"✅ **Streamed:** `{file_name}`")

                except (errors.FileReferenceExpiredError, errors.MediaEmptyError):
                    logger.warning(f"Ref Expired on {message.id}, refreshing...")
                    retries -= 1
                    await asyncio.sleep(2)
                    continue # Loop wapas chalega, fresh_msg lega
                    
                except errors.FloodWaitError as e:
                    logger.warning(f"FloodWait {e.seconds}s")
                    await asyncio.sleep(e.seconds)
                    # Don't decrease retries for FloodWait
                
                except Exception as e:
                    logger.error(f"Failed {message.id}: {e}")
                    retries -= 1
                    await asyncio.sleep(2)

            if not success:
                try: 
                    await bot_client.send_message(event.chat_id, f"❌ **Skipped:** `{message.id}` after 3 attempts.")
                except: pass
            
            total_processed += 1

        if is_running:
            await status_message.edit(f"✅ **Job Done!**\nTotal Processed: `{total_processed}`")

    except Exception as e:
        await status_message.edit(f"❌ **Critical Error:** {e}")
    finally:
        is_running = False

# --- COMMANDS ---
@bot_client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await event.respond("🟢 **Ultra Bot Ready!**\n`/clone Source Dest`")

@bot_client.on(events.NewMessage(pattern='/clone'))
async def clone_init(event):
    global is_running
    if is_running: return await event.respond("⚠️ Busy in another task...")
    try:
        args = event.text.split()
        pending_requests[event.chat_id] = {'source': int(args[1]), 'dest': int(args[2])}
        await event.respond("✅ **Set!** Send Range Link (e.g., `https://t.me/c/xxx/10 - https://t.me/c/xxx/20`)")
    except: await event.respond("❌ Usage: `/clone -100xxx -100yyy`")

@bot_client.on(events.NewMessage())
async def range_listener(event):
    global current_task, is_running
    if event.chat_id not in pending_requests or "t.me" not in event.text: return
    try:
        links = event.text.strip().split("-")
        msg1, msg2 = int(links[0].split("/")[-1]), int(links[1].split("/")[-1]) # More robust ID extraction
        if msg1 > msg2: msg1, msg2 = msg2, msg1
        
        data = pending_requests.pop(event.chat_id)
        is_running = True
        current_task = asyncio.create_task(transfer_process(event, data['source'], data['dest'], msg1, msg2))
    except Exception as e: await event.respond(f"❌ Error: {e}")

@bot_client.on(events.NewMessage(pattern='/stop'))
async def stop_handler(event):
    global is_running
    is_running = False
    if current_task: current_task.cancel()
    await event.respond("🛑 **Stopped!**")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    user_client.start()
    loop.create_task(start_web_server())
    bot_client.start(bot_token=BOT_TOKEN)
    logger.info("Bot is Running...")
    bot_client.run_until_disconnected()
            
