import os
import asyncio
import logging
import time
import math
import re
import mimetypes
from telethon import TelegramClient, events, utils
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, MessageNotModifiedError
from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo, DocumentAttributeAudio
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

# --- CLIENT SETUP ---
# Max Performance Settings
user_client = TelegramClient(
    StringSession(STRING_SESSION), 
    API_ID, 
    API_HASH, 
    connection_retries=None, 
    flood_sleep_threshold=60,
    request_retries=10
)
bot_client = TelegramClient(
    'bot_session', 
    API_ID, 
    API_HASH, 
    connection_retries=None, 
    flood_sleep_threshold=60,
    request_retries=10
)

# --- GLOBAL STATE ---
pending_requests = {} 
current_task = None
is_running = False
status_message = None
last_update_time = 0

# --- WEB SERVER ---
async def handle(request):
    return web.Response(text="Bot is Running (72MB Buffer)! 🔥")

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
    
    if now - last_update_time < 4: return 
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
        
        # 8MB Chunks (Standard High Speed)
        self.chunk_size = 8 * 1024 * 1024 
        
        # MEGA BUFFER: 9 Chunks = 72MB in RAM
        # This allows massive pre-fetching
        self.queue = asyncio.Queue(maxsize=9) 
        
        self.downloader_task = asyncio.create_task(self._worker())
        self.buffer = b""

    async def _worker(self):
        """Aggressively fetches chunks to fill 72MB Buffer"""
        try:
            async for chunk in self.client.iter_download(self.location, chunk_size=self.chunk_size):
                await self.queue.put(chunk)
            await self.queue.put(None) 
        except Exception as e:
            logger.error(f"Worker Error: {e}")
            await self.queue.put(None)

    def __len__(self):
        return self.file_size

    async def read(self, size=-1):
        if size == -1: size = self.chunk_size
        
        while len(self.buffer) < size:
            chunk = await self.queue.get()
            
            if chunk is None: 
                await self.queue.put(None) 
                break
                
            self.buffer += chunk
            self.current_bytes += len(chunk)
            
            asyncio.create_task(progress_callback(self.current_bytes, self.file_size, self.start_time, self.name))

        data = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return data

# --- SMART FILE INFO DETECTOR ---
def get_file_info(message):
    file_name = "Unknown_File"
    mime_type = "application/octet-stream"
    
    if message.file:
        mime_type = message.file.mime_type
        if message.file.name:
            file_name = message.file.name
        else:
            ext = mimetypes.guess_extension(mime_type)
            if not ext:
                if "video" in mime_type: ext = ".mp4"
                elif "image" in mime_type: ext = ".jpg"
                elif "pdf" in mime_type: ext = ".pdf"
                else: ext = ""
            file_name = f"File_{message.id}{ext}"
            
    # CRITICAL: Fix Extensions
    if "video" in mime_type and not re.search(r'\.(mp4|mkv|avi|mov|webm)$', file_name, re.IGNORECASE):
        file_name += ".mp4"
    elif "pdf" in mime_type and not file_name.lower().endswith(".pdf"):
        file_name += ".pdf"
        
    return file_name, mime_type

# --- ATTRIBUTE CLEANER ---
def get_clean_attributes(message, file_name):
    attributes = []
    attributes.append(DocumentAttributeFilename(file_name=file_name))
    
    if message.media and hasattr(message.media, 'document'):
        for attr in message.media.document.attributes:
            if isinstance(attr, DocumentAttributeVideo):
                attributes.append(DocumentAttributeVideo(
                    duration=attr.duration,
                    w=attr.w,
                    h=attr.h,
                    round_message=attr.round_message,
                    supports_streaming=True
                ))
            elif isinstance(attr, DocumentAttributeAudio):
                attributes.append(attr)
    return attributes

# --- LINK PARSER ---
def extract_id_from_link(link):
    regex = r"(\d+)$"
    match = re.search(regex, link)
    if match: return int(match.group(1))
    return None

# --- TRANSFER PROCESS ---
async def transfer_process(event, source_id, dest_id, start_msg, end_msg):
    global is_running, status_message
    
    status_message = await event.respond(f"🔥 **Ultra Queue Engine Started!**\nSource: `{source_id}`")
    total_processed = 0
    
    try:
        async for message in user_client.iter_messages(source_id, min_id=start_msg-1, max_id=end_msg+1, reverse=True):
            if not is_running:
                await status_message.edit("🛑 **Stopped by User!**")
                break

            if getattr(message, 'action', None): continue

            try:
                file_name, mime_type = get_file_info(message)
                await status_message.edit(f"🔍 **Processing:** `{file_name}`")

                if not message.media:
                    await bot_client.send_message(dest_id, message.text)
                else:
                    sent = False
                    start_time = time.time()
                    
                    # 1. Direct Copy
                    try:
                        await bot_client.send_file(dest_id, message.media, caption=message.text or "")
                        sent = True
                        await status_message.edit(f"✅ **Fast Copied:** `{file_name}`")
                    except Exception:
                        pass 

                    # 2. 72MB Buffered Stream
                    if not sent:
                        attributes = get_clean_attributes(message, file_name)
                        thumb = await user_client.download_media(message, thumb=-1)
                        
                        stream_file = UltraBufferedStream(
                            user_client, 
                            message.media.document if hasattr(message.media, 'document') else message.media.photo,
                            message.file.size,
                            file_name,
                            start_time
                        )
                        
                        force_doc = not ("video" in mime_type or "image" in mime_type)
                        
                        await bot_client.send_file(
                            dest_id,
                            file=stream_file,
                            caption=message.text or "",
                            attributes=attributes,
                            thumb=thumb,
                            supports_streaming=True,
                            file_size=message.file.size,
                            force_document=force_doc,
                            mime_type=mime_type
                        )
                        
                        if thumb and os.path.exists(thumb): os.remove(thumb)
                        await status_message.edit(f"✅ **Sent:** `{file_name}`")

                total_processed += 1
                
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.error(f"Failed {message.id}: {e}")
                try: await bot_client.send_message(event.chat_id, f"❌ **Skipped:** `{file_name}`\nReason: `{str(e)[:50]}`")
                except: pass
                continue

        if is_running:
            await status_message.edit(f"✅ **Job Done!**\nTotal: `{total_processed}`")

    except Exception as e:
        await status_message.edit(f"❌ **Error:** {e}")
    finally:
        is_running = False

# --- COMMANDS ---
@bot_client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await event.respond("🟢 **Ultra Bot Ready!**\n`/clone Source Dest`")

@bot_client.on(events.NewMessage(pattern='/clone'))
async def clone_init(event):
    global is_running
    if is_running: return await event.respond("⚠️ Busy...")
    try:
        args = event.text.split()
        pending_requests[event.chat_id] = {'source': int(args[1]), 'dest': int(args[2])}
        await event.respond("✅ **Set!** Send Range Link.")
    except: await event.respond("❌ Usage: `/clone -100xxx -100yyy`")

@bot_client.on(events.NewMessage())
async def range_listener(event):
    global current_task, is_running
    if event.chat_id not in pending_requests or "t.me" not in event.text: return
    try:
        links = event.text.strip().split("-")
        msg1, msg2 = extract_id_from_link(links[0]), extract_id_from_link(links[1])
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
    bot_client.run_until_disconnected()


