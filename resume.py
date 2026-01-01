
"""
CRICOVERSE - Professional Hand Cricket Telegram Bot
A feature-rich, group-based Hand Cricket game engine
Single file implementation - Part 1 of 10
"""

import logging
import asyncio
import random
import time
import json
import sqlite3  # <--- New for SQL
import shutil   # <--- New for Backup
import os
import html  # <--- Add this at the top with other imports
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
from enum import Enum
from collections import defaultdict

from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup,
    ChatMember
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters
)
from telegram.constants import ParseMode
from telegram.error import TelegramError, Forbidden

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup,
    ChatMember,
    InputMediaPhoto
)
from telegram.constants import ParseMode

logger = logging.getLogger(__name__)

# CRITICAL: Set your bot token and owner ID here
BOT_TOKEN = "8428604292:AAGOkKYweTyb-moVTMPCrQkAgRPwIhQ1s5k"
OWNER_ID = 7460266461  # Replace with your Telegram user ID
SUPPORT_GROUP_ID = -1002707382739  # Replace with your support group ID

# Game Constants
class GamePhase(Enum):
    IDLE = "idle"
    # ... Team Phases ...
    TEAM_JOINING = "team_joining"
    HOST_SELECTION = "host_selection"
    CAPTAIN_SELECTION = "captain_selection"
    TEAM_EDIT = "team_edit"
    OVER_SELECTION = "over_selection"
    TOSS = "toss"
    MATCH_IN_PROGRESS = "match_in_progress"
    INNINGS_BREAK = "innings_break"
    MATCH_ENDED = "match_ended"
    SUPER_OVER = "super_over"
    
    # ... SOLO PHASES (New) ...
    SOLO_JOINING = "solo_joining"
    SOLO_MATCH = "solo_match"

class MatchEvent(Enum):
    DOT_BALL = "dot"
    RUNS_1 = "1run"
    RUNS_2 = "2runs"
    RUNS_3 = "3runs"
    RUNS_4 = "4runs"
    RUNS_5 = "5runs"
    RUNS_6 = "6runs"
    WICKET = "wicket"
    NO_BALL = "noball"
    WIDE = "wide"
    FREE_HIT = "freehit"
    DRS_REVIEW = "drs_review"
    DRS_OUT = "drs_out"
    DRS_NOT_OUT = "drs_notout"
    INNINGS_BREAK = "innings_break"
    VICTORY = "victory"

# GIF URLs for match events
GIFS = {
    MatchEvent.DOT_BALL: [
        "CgACAgQAAyEFAATEuZi2AAIEsmlL3oS80G_hP2r73pB1Xp9fja2TAAJ2EwACARH5UrcBeu1Hx7x-NgQ"
    ],
    MatchEvent.RUNS_1: [
        "CgACAgUAAyEFAATEuZi2AAIE_mlL51w3IW0jthJmfZeMqNVpFRfUAAIiLQACeNjhVxT4d9Xn2PI-NgQ",
        "CgACAgUAAyEFAATU3pgLAAIKkWlE9Gqtq1mAjiu926NvWRGfxQW1AAIJHAACOMrpVwhrNvXoibUAATYE",
        "CgACAgUAAyEFAATEuZi2AAIFYmlL7mOnFTT69LGMLS9G2oA6EHJpAAJsHQACRV1hVnaw6OSdIDwQNgQ"
    ],
    MatchEvent.RUNS_2: [
        "CgACAgUAAyEFAATEuZi2AAIFK2lL6X8FPyJRYp9RbF6DiAAB-RqzvAACWR0AAkVdYVY9UqOGM0nDajYE",
        "CgACAgUAAyEFAATU3pgLAAIKi2lE9GrIvY93_Dcaiv8zaa0IbES6AALJGgACN2_pV4f4uWRTw9wxNgQ"
    ],
    MatchEvent.RUNS_3: [
        "CgACAgUAAyEFAATEuZi2AAIFQGlL64CXO07OHbHMip1g2Lu0HFayAAJlHQACRV1hVkXx8RdRbQniNgQ",
        "CgACAgUAAyEFAATU3pgLAAIKf2lE9Gq72p6bgh1C8K9SjTyciqXfAAI2DwACPzbQVnca7Od2bSquNgQ"
    ],
    MatchEvent.RUNS_4: [
        "CgACAgUAAyEGAATYx4tPAAJIvmlMBASE6vZ-FK1_CKrtrHRpUi5WAAJSCAACD_YgVo49O55ICLAENgQ",
        "CgACAgUAAxkBAAIKY2lNWXZwCPa1mikPTuiI-im6KsXZAALbCgAC5WCoVXTWQ_MhLqz4NgQ",
        "CgACAgUAAyEGAATYx4tPAAJKRGlM-l-WWxsOUMrQJWlDsnrShZALAAKtDAACFqM4VMeSD_FLQu8MNgQ",
        "CgACAgUAAyEGAAShX2HTAAIgpWlMOtRIxiwO5A91S3qnzJ3hNJpFAAJTBgACmdE5V_Z3vM_sBDZCNgQ",
        "CgACAgUAAyEFAATYx4tPAAJDtWlLmks4fC6UZFYmqqV_i-B8_jC1AAJcFwACITAQVA4cFTAQ7BfKNgQ"
    ],
    MatchEvent.RUNS_5: [
        "CgACAgQAAyEFAATU3pgLAAIKiGlE9GoYG_0qTVEd3Le7R6qvyWrWAAJeGwACryS5UH5WGCXTJywAATYE",
        "CgACAgQAAyEFAATEuZi2AAIE6mlL4TanjQPWyDaNCpaXtOq-CVtOAAJ_IAACMudhUlC2yWKM8GmFNgQ",
        "CgACAgUAAyEFAATEuZi2AAIFTWlL7Eq7OGaFKKEfosOF_jAtHWTUAALmHAAChf5gVivH4SvOeCpRNgQ"
    ],
    MatchEvent.RUNS_6: [
        "CgACAgUAAyEGAAShX2HTAAIhH2lMRrNUrjRV4GW2K8booBvMtTG9AAKrCgACJXRpVOeF4ynzTcBoNgQ",
        "CgACAgUAAyEGAATYx4tPAAJItmlMA-mbxLqNhGcc8S785y2j5BWEAAKzDQAC9WdJVnVvz6iMeR39NgQ",
        "CgACAgUAAyEGAATYx4tPAAJHmWlL_f9GFzB3wlmreOcoJdNeQb5pAAJpAwAClZdBVj1oWzydv8lMNgQ",
        "CgACAgQAAyEFAATEuZi2AAIE6GlL4QXc1nMUBKOdGkLrPuPPYfUPAAJ-IAACMudhUqLnowABXPhb3DYE",
        "CgACAgUAAyEFAATU3pgLAAIKjmlE9GrcsVDgJe8ohHimK7JQf-MeAAJdFwACITAQVNF-Nok7Tly0NgQ",
        "CgACAgUAAyEFAATYx4tPAAJDw2lLmkzfNB56Io-uMPnGQmOTuU3wAAKJAwAC0ymZV0m1AAEE0NAEjTYE",
        "CgACAgUAAyEFAATU3pgLAAIKfGlE9GqHxSIInO0P4wSVuD5xbNiNAAJgGQACzouoVeTU9nOOeNqDNgQ"
    ],
    MatchEvent.WICKET: [
        "CgACAgQAAyEFAATU3pgLAAIKhGlE9Go2nsCXKpBBjglIQ2I3ZObsAAKvFQACaewBUkT0IZS8qdW4NgQ",
        "CgACAgQAAyEFAATU3pgLAAIKhWlE9GpEJp5SCDH35xUN97QPkkdSAAK1EwACMv1pUfLrRWYa9zWLNgQ",
        "CgACAgUAAyEFAATU3pgLAAIKhmlE9GoVK8ybgnUTS502q1YMSG35AALqAwACIHhpV7c1o-HTQNSPNgQ",
        "CgACAgQAAyEFAATU3pgLAAIKjWlE9GqL7Uad2y2fznl2ZvasOk_xAALaGQACh1UBUdFsFVeRv5qwNgQ",
        "CgACAgUAAyEGAATYx4tPAAJHa2lL_VmXp7nhZMuNPVRgbDmv54uXAAKQCAACBRCRVj5VjvOl6j21NgQ",
        "CgACAgUAAyEGAAShX2HTAAIh3WlM785mkSB-K9myKNbS1lfWmB6fAAKRBgAC_DYZVhtRUsAAAW_fvzYE"
    ],
    MatchEvent.NO_BALL: [
        "https://tenor.com/bBvYA.gif"
    ],
    MatchEvent.WIDE: [
        "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExbWdubjB0YmVuZnMwdXBwODg5MzZ0cjFsNWl4ZXN1MzltOW1yZng5dCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/YtI7H5jotPvh9Z09t6/giphy.gif"
    ],
    MatchEvent.FREE_HIT: [
        "https://t.me/cricoverse/42"
    ],
    MatchEvent.DRS_REVIEW: [
        "https://t.me/cricoverse/37"
    ],
    MatchEvent.DRS_OUT: [
        "https://pin.it/4HD5YcJOA"
    ],
    MatchEvent.DRS_NOT_OUT: [
        "https://tenor.com/bOVyJ.gif"
    ],
    MatchEvent.INNINGS_BREAK: [
    "CgACAgUAAxkBAAIjxGlViI35Zggv28khmw7xO9VzmT5IAALCDgACWnBJVhxhPkgGPYgDOAQ"
    ],
    MatchEvent.VICTORY: [
        "CgACAgUAAxkBAAIjuGlVh2s6GJm-hhGKFVH7Li3J-JOvAAI6GQACdi_xVJ8ztQiJSfOAOAQ"
    ],
    "cheer":  ["https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExcTFudnkxcWhzZmFlazQ2MHN6emY2c3JjY3J4MWV2Z2JjdzRkcGVyOCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/humidv0MqqdO5ZoYhn/giphy.gif" ]

}

# --- GLOBAL HELPER FUNCTION ---
# --- GLOBAL HELPER FUNCTION (FIXED) ---
def get_user_tag(user):
    """Returns a clickable HTML link for the user"""
    if not user:
        return "Unknown"
    
    # ✅ FIX: Handle both User objects AND Player objects
    try:
        # Case 1: Telegram User object (has .id)
        if hasattr(user, 'id'):
            user_id = user.id
            first_name = user.first_name
        # Case 2: Player object (has .user_id)
        elif hasattr(user, 'user_id'):
            user_id = user.user_id
            first_name = user.first_name
        else:
            return "Unknown"
        
        # Clean the name to prevent HTML errors
        clean_name = html.escape(first_name)
        return f"<a href='tg://user?id={user_id}'>{clean_name}</a>"
    except Exception as e:
        logger.error(f"Error creating user tag: {e}")
        return "Unknown"

# 🎨 GLOBAL MEDIA ASSETS (Safe Placeholders)
MEDIA_ASSETS = {
    "welcome": "AgACAgUAAxkBAAIdyWlTp9syIgPRPjquDqt54sgTC-Z9AAJ6DGsb01NgVjGEUURN66O4AQADAgADeQADOAQ",
    "help": "AgACAgUAAxkBAAIdy2lTp95r3XCIVm55c0mSwkDgfFWGAAKJDGsbr1UoVkvOnY3eXShdAQADAgADeQADOAQ",
    "mode_select": "AgACAgUAAxkBAAIdx2lTp8g0IDv3cKvTW-Ooh_gz8R7dAAIrDGsbDteYViuXgUxDLxUMAQADAgADeAADOAQ",
    "joining": "AgACAgUAAxkBAAIdzWlTp-KgP8GXw9l-D2XBw7v-FA4VAAJ7DGsb01NgVt9gLCPgUloqAQADAgADeQADOAQ",
    "host": "AgACAgUAAxkBAAIdz2lTp-WtFzDA6sVPjY41r3SGoG2GAAJ8DGsb01NgVuefZsMJZ2HHAQADAgADeQADOAQ",
    "stats": "AgACAgUAAxkBAAId02lTp-xZPysRE2VhGL4Yp9KtVm4SAAJ-DGsb01NgVreDh-MZ-z_jAQADAgADeQADOAQ",
    "squads": "AgACAgUAAxkBAAId2WlTp_UE7DKMJo-9xuG9dVh3pzFOAAKBDGsb01NgVh7rYgwitpwfAQADAgADeQADOAQ",
    "toss": "AgACAgUAAxkBAAId0WlTp-lzYWWT64K71rHLFpD6sx2sAAJ9DGsb01NgVnXTBXaUD1d8AQADAgADeQADOAQ",
    "h2h": "AgACAgUAAxkBAAId1WlTp--27rO3UJj8sutYs-rOa-pvAAJ_DGsb01NgVpRo2vl34sA4AQADAgADeQADOAQ",
    "botstats": "AgACAgUAAxkBAAId22lTp_hdHv53dZE8QVpjiaMMUPcnAAKCDGsb01NgVtTy4XXDT9DbAQADAgADeQADOAQ",
    "scorecard": "AgACAgUAAxkBAAId12lTp_Ka_4tK_1di7kku0QOIDC3tAAKADGsb01NgVt8iUO7Ss8vjAQADAgADeQADOAQ" # Scorecard BG
}
# Commentary templates
# Ultimate Professional English Commentary (Expanded)
COMMENTARY = {
    "dot": [
        "Solid defense! No run conceded. 🧱",
        "Beaten! That was a jaffa! 🔥",
        "Straight to the fielder. Dot ball. 😐",
        "Swing and a miss! The batsman had no clue. 💨",
        "Dot ball. Pressure is building up on the batting side! 😰",
        "Respect the bowler! Good delivery in the corridor of uncertainty. 🙌",
        "No run there. Excellent fielding inside the circle. 🤐",
        "Played back to the bowler. 🤚",
        "A loud shout for LBW, but turned down. Dot ball. 🔉",
        "Good line and length. The batsman leaves it alone. 👀",
        "Can't get it through the gap. Frustration growing! 😤",
        "Top class bowling! Giving nothing away. 🔒",
        "Defended with a straight bat. Textbook cricket. 📚",
        "The batsman is struggling to time the ball. 🐢",
        "Another dot! The required run rate is creeping up. 📈"
    ],
    "single": [
        "Quick single! Good running between the wickets. 🏃‍♂️",
        "Push and run! Strike rotated smartly. 🔄",
        "Just a single added to the tally. 1️⃣",
        "Good call! One run completed safely. 👟",
        "Direct hit missed! That was close. 🎯",
        "Tucked away off the hips for a single. 🏏",
        "Dropped at his feet and they scamper through. ⚡",
        "Fielder fumbles, and they steal a run. 🤲",
        "Sensible batting. Taking the single on offer. 🧠",
        "Driven to long-on for one. 🚶",
        "Smart cricket! Rotating the strike to keep the scoreboard ticking. ⏱️",
        "A little hesitation, but they make it in the end. 😅"
    ],
    "double": [
        "In the gap! They will get two easily. ✌️",
        "Great running between the wickets! Two runs added. 🏃‍♂️🏃‍♂️",
        "Pushed hard for the second! Excellent fitness shown. 💪",
        "Fielder was slow to react! They steal a couple. 😴",
        "Two runs added. Good placement into the deep. ⚡",
        "They turn for the second run immediately! Aggressive running. ⏩",
        "Misfield allows them to come back for two. 🤦‍♂️",
        "Good throw from the deep, but the batsman is safe. ⚾",
        "Calculated risk taken for the second run! ✅",
        "The fielder cuts it off, but they get a couple. 🛡️"
    ],
    "triple": [
        "Superb fielding effort! Saved the boundary just in time. 🛑 3 runs.",
        "They are running hard! Three runs taken. 🏃‍♂️💨",
        "Excellent stamina! Pushing for the third run. 🔋",
        "Just short of the boundary! 3 runs added to the score. 🚧",
        "The outfield is slow, the ball stops just before the rope. 🐢",
        "Great relay throw! But they collect three runs. 🤝"
    ],
    "boundary": [
        "CRACKING SHOT! Raced to the fence like a bullet! 🚀 FOUR!",
        "What timing! Found the gap perfectly. 🏎️ 4 Runs!",
        "Beautiful Cover Drive! That is a textbook shot! 😍",
        "The fielder is just a spectator! That's a boundary! 👀",
        "One bounce and over the rope! Four runs! 🎾",
        "Misfield and four! The bowler is absolutely furious. 😠",
        "Surgical precision! Cut away past point for FOUR! 🔪",
        "Pulled away powerfully! No chance for the fielder. 🤠",
        "Straight down the ground! Umpire had to duck! 🦆 FOUR!",
        "Edged but it flies past the slip cordon! Lucky boundary. 🍀",
        "Swept away fine! The fielder gives chase in vain. 🧹",
        "That was pure elegance! Caressed to the boundary. ✨",
        "Power and placement! A terrific shot for four. 💪",
        "Short ball punished! Dispatched to the fence. 👮‍♂️",
        "Drilled through the covers! What a sound off the bat! 🔊"
    ],
    "five": [
        "FIVE RUNS! Overthrows! Bonus runs for the team. 🎁",
        "Comedy of errors on the field! 5 runs conceded. 🤡",
        "Running for five! Incredible stamina displayed! 🏃‍♂️💨",
        "Bonus runs! The batting team is delighted with that gift. 🎉",
        "Throw hits the stumps and deflects away! 5 runs! 🎱"
    ],
    "six": [
        "HUGE! That's out of the stadium! 🌌 SIX!",
        "Muscle power! Sent into orbit! 💪",
        "MAXIMUM! What a clean connection! 💥",
        "It's raining sixes! Destruction mode activated! 🔨",
        "Helicopter Shot! That is magnificent! 🚁",
        "That's a monster hit! The bowler looks devastated. 😭",
        "Gone with the wind! High and handsome! 🌬️",
        "That ball is in the parking lot! Fetch that! 🚗",
        "Clean striking! It's landed in the top tier! 🏟️",
        "Upper cut sails over third man! What a shot! ✂️",
        "Smoked down the ground! That is a massive six! 🚬",
        "The crowd catches it! That's a fan favorite shot! 🙌",
        "Pick that up! Sent traveling into the night sky! 🚀",
        "Pure timing! He didn't even try to hit that hard. 🪄",
        "The bowler missed the yorker, and it's gone for SIX! 📏"
    ],
    "wicket": [
        "OUT! Game over for the batsman! ❌",
        "Clean Bowled! Shattered the stumps! 🪵",
        "Caught! Fielder makes no mistake. Wicket! 👐",
        "Gone! The big fish is in the net! 🎣",
        "Edged and taken! A costly mistake by the batsman. 🏏",
        "Stumping! Lightning fast hands by the keeper! ⚡",
        "Run Out! A terrible mix-up in the middle. 🚦",
        "LBW! That looked plumb! The finger goes up! ☝️",
        "Caught and Bowled! Great reflexes by the bowler! 🤲",
        "Hit Wicket! Oh no, he stepped on his own stumps! 😱",
        "The partnership is broken! Massive moment in the game. 💔",
        "He has holed out to the deep! End of a good innings. 🔚",
        "Golden Duck! He goes back without troubling the scorers. 🦆",
        "The stumps are taking a walk! cartwheeling away! 🤸‍♂️",
        "What a catch! He plucked that out of thin air! 🦅"
    ],
    "noball": [
        "NO BALL! Overstepped the line! 🚨",
        "Free Hit coming up! A free swing for the batsman! 🔥",
        "Illegal delivery. Umpire signals No Ball. 🙅‍♂️",
        "That was a beamer! Dangerous delivery. No Ball. 🤕",
        "Bowler loses his grip. No Ball called. 🧼"
    ],
    "wide": [
        "Wide Ball! Radar is off. 📡",
        "Too wide! Extra run conceded. 🎁",
        "Wayward delivery. Drifting down the leg side. 🚌",
        "Too high! Umpire signals a wide for height. 🦒",
        "Spilled down the leg side. Keeper collects it. Wide. 🧤"
    ]
}

# Data storage paths
DATA_DIR = "resume_data"
USERS_FILE = os.path.join(DATA_DIR, "users.json")
MATCHES_FILE = os.path.join(DATA_DIR, "matches.json")
STATS_FILE = os.path.join(DATA_DIR, "stats.json")
ACHIEVEMENTS_FILE = os.path.join(DATA_DIR, "achievements.json")
BANNED_GROUPS_FILE = os.path.join(DATA_DIR, "banned_groups.json")
GROUPS_FILE = os.path.join(DATA_DIR, "groups.json")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
DB_FILE = "cricoverse.db" # SQL Database File

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

# Global data structures
active_matches: Dict[int, 'Match'] = {}
user_data: Dict[int, Dict] = {}
match_history: List[Dict] = []
player_stats: Dict[int, Dict] = {}
achievements: Dict[int, List[str]] = {}
registered_groups: Dict[int, Dict] = {}
banned_groups: Set[int] = set()
bot_start_time = time.time()

# Initialize data structures from files
def load_data():
    """Load all data from JSON files"""
    global user_data, match_history, player_stats, achievements, registered_groups
    
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, 'r') as f:
                user_data = {int(k): v for k, v in json.load(f).items()}
        
        if os.path.exists(MATCHES_FILE):
            with open(MATCHES_FILE, 'r') as f:
                match_history = json.load(f)
        
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, 'r') as f:
                player_stats = {int(k): v for k, v in json.load(f).items()}
        
        if os.path.exists(ACHIEVEMENTS_FILE):
            with open(ACHIEVEMENTS_FILE, 'r') as f:
                achievements = {int(k): v for k, v in json.load(f).items()}
        
        if os.path.exists(GROUPS_FILE):
            with open(GROUPS_FILE, 'r') as f:
                registered_groups = {int(k): v for k, v in json.load(f).items()}
        
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading data: {e}")

def save_data():
    """Save data to BOTH SQL Database AND JSON Files"""
    try:
        # 1. SQL Save (Primary & Fast)
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("BEGIN TRANSACTION")
        
        for uid, data in user_data.items():
            c.execute("INSERT OR REPLACE INTO users (user_id, data) VALUES (?, ?)", (uid, json.dumps(data)))
        for uid, data in player_stats.items():
            c.execute("INSERT OR REPLACE INTO player_stats (user_id, data) VALUES (?, ?)", (uid, json.dumps(data)))
        for match in match_history:
            mid = match.get("match_id", str(time.time()))
            c.execute("INSERT OR REPLACE INTO matches (match_id, data) VALUES (?, ?)", (mid, json.dumps(match)))
        for gid, data in registered_groups.items():
            c.execute("INSERT OR REPLACE INTO groups (group_id, data) VALUES (?, ?)", (gid, json.dumps(data)))
        for uid, data in achievements.items():
            c.execute("INSERT OR REPLACE INTO achievements (user_id, data) VALUES (?, ?)", (uid, json.dumps(data)))

        conn.commit()
        conn.close()
        
        # 2. JSON Save (Secondary / Manual Backup)
        with open(USERS_FILE, 'w') as f: json.dump(user_data, f, indent=2)
        with open(STATS_FILE, 'w') as f: json.dump(player_stats, f, indent=2)
        with open(MATCHES_FILE, 'w') as f: json.dump(match_history, f, indent=2)
        with open(GROUPS_FILE, 'w') as f: json.dump(registered_groups, f, indent=2)
        with open(ACHIEVEMENTS_FILE, 'w') as f: json.dump(achievements, f, indent=2)
        
        # ✅ 3. SAVE BANNED GROUPS
        with open(BANNED_GROUPS_FILE, 'w') as f:
            json.dump(list(banned_groups), f, indent=2)

    except Exception as e:
        logger.error(f"Error saving data: {e}")


# --- DUAL STORAGE MANAGER (SQL + JSON) ---

def init_db():
    """Initialize SQL Tables"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Hum data ko TEXT format (JSON String) mein store karenge taaki code complex na ho
    c.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS player_stats (user_id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS matches (match_id TEXT PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS groups (group_id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS achievements (user_id INTEGER PRIMARY KEY, data TEXT)''')
    conn.commit()
    conn.close()

def save_data():
    """Save data to BOTH SQL Database AND JSON Files"""
    try:
        # 1. SQL Save (Primary & Fast)
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("BEGIN TRANSACTION")
        
        for uid, data in user_data.items():
            c.execute("INSERT OR REPLACE INTO users (user_id, data) VALUES (?, ?)", (uid, json.dumps(data)))
        for uid, data in player_stats.items():
            c.execute("INSERT OR REPLACE INTO player_stats (user_id, data) VALUES (?, ?)", (uid, json.dumps(data)))
        for match in match_history:
            mid = match.get("match_id", str(time.time()))
            c.execute("INSERT OR REPLACE INTO matches (match_id, data) VALUES (?, ?)", (mid, json.dumps(match)))
        for gid, data in registered_groups.items():
            c.execute("INSERT OR REPLACE INTO groups (group_id, data) VALUES (?, ?)", (gid, json.dumps(data)))
        for uid, data in achievements.items():
            c.execute("INSERT OR REPLACE INTO achievements (user_id, data) VALUES (?, ?)", (uid, json.dumps(data)))

        conn.commit()
        conn.close()
        
        # 2. JSON Save (Secondary / Manual Backup)
        with open(USERS_FILE, 'w') as f: json.dump(user_data, f, indent=2)
        with open(STATS_FILE, 'w') as f: json.dump(player_stats, f, indent=2)
        with open(MATCHES_FILE, 'w') as f: json.dump(match_history, f, indent=2)
        with open(GROUPS_FILE, 'w') as f: json.dump(registered_groups, f, indent=2)
        with open(ACHIEVEMENTS_FILE, 'w') as f: json.dump(achievements, f, indent=2)

    except Exception as e:
        logger.error(f"Error saving data: {e}")

def load_data():
    """Load all data (Try SQL first, Fallback to JSON)"""
    global user_data, match_history, player_stats, achievements, registered_groups, banned_groups
    
    # Initialize DB if missing
    if not os.path.exists(DB_FILE):
        init_db()

    data_loaded_from_sql = False
    
    # --- TRY LOADING FROM SQL ---
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        c.execute("SELECT count(*) FROM users")
        if c.fetchone()[0] > 0:
            c.execute("SELECT user_id, data FROM users")
            user_data = {row[0]: json.loads(row[1]) for row in c.fetchall()}

            c.execute("SELECT user_id, data FROM player_stats")
            player_stats = {row[0]: json.loads(row[1]) for row in c.fetchall()}

            c.execute("SELECT data FROM matches")
            match_history = [json.loads(row[0]) for row in c.fetchall()]

            c.execute("SELECT group_id, data FROM groups")
            registered_groups = {row[0]: json.loads(row[1]) for row in c.fetchall()}

            c.execute("SELECT user_id, data FROM achievements")
            achievements = {row[0]: json.loads(row[1]) for row in c.fetchall()}
            
            data_loaded_from_sql = True
            logger.info("✅ Data loaded from SQL Database.")
            
        conn.close()
    except Exception as e:
        logger.error(f"SQL Load Error (Falling back to JSON): {e}")

    # --- FALLBACK TO JSON ---
    if not data_loaded_from_sql:
        logger.info("⚠️ Loading from JSON files...")
        try:
            if os.path.exists(USERS_FILE):
                with open(USERS_FILE, 'r') as f: 
                    user_data = {int(k): v for k, v in json.load(f).items()}
            if os.path.exists(STATS_FILE):
                with open(STATS_FILE, 'r') as f: 
                    player_stats = {int(k): v for k, v in json.load(f).items()}
            if os.path.exists(MATCHES_FILE):
                with open(MATCHES_FILE, 'r') as f: 
                    match_history = json.load(f)
            if os.path.exists(GROUPS_FILE):
                with open(GROUPS_FILE, 'r') as f: 
                    registered_groups = {int(k): v for k, v in json.load(f).items()}
            if os.path.exists(ACHIEVEMENTS_FILE):
                with open(ACHIEVEMENTS_FILE, 'r') as f: 
                    achievements = {int(k): v for k, v in json.load(f).items()}
            
            # JSON se load hone ke baad turant SQL me sync kar do
            save_data()
        except Exception: 
            pass
    
    # ✅ LOAD BANNED GROUPS
    if os.path.exists(BANNED_GROUPS_FILE):
        try:
            with open(BANNED_GROUPS_FILE, 'r') as f:
                banned_groups = set(json.load(f))
            logger.info(f"🚫 Loaded {len(banned_groups)} banned groups")
        except Exception as e:
            logger.error(f"Error loading banned groups: {e}")
            banned_groups = set()

# Initialize player stats for a user
def init_player_stats(user_id: int):
    """Initialize stats structure (Robust Fix for Missing Keys)"""
    # Default structures
    default_team = {
        "matches": 0, "runs": 0, "balls": 0, "wickets": 0, 
        "runs_conceded": 0, "balls_bowled": 0, "highest": 0, 
        "centuries": 0, "fifties": 0, "ducks": 0, "sixes": 0, "fours": 0,
        "mom": 0, "hat_tricks": 0, "captain_matches": 0, "captain_wins": 0
    }
    
    default_solo = {
        "matches": 0, "wins": 0, "runs": 0, "balls": 0,
        "wickets": 0, "highest": 0, "ducks": 0, "top_3_finishes": 0
    }

    # Case 1: New User
    if user_id not in player_stats:
        player_stats[user_id] = {
            "team": default_team.copy(),
            "solo": default_solo.copy()
        }
        save_data()
    
    # Case 2: Existing User (Check & Fix missing keys)
    else:
        changed = False
        
        # Check Team Stats
        if "team" not in player_stats[user_id]:
            # Old data migration logic
            old_data = player_stats[user_id].copy()
            player_stats[user_id]["team"] = default_team.copy()
            player_stats[user_id]["team"]["matches"] = old_data.get("matches_played", 0)
            player_stats[user_id]["team"]["runs"] = old_data.get("total_runs", 0)
            # ... map other fields if needed ...
            changed = True
        else:
            # Fill missing keys in 'team'
            for key, val in default_team.items():
                if key not in player_stats[user_id]["team"]:
                    player_stats[user_id]["team"][key] = val
                    changed = True
        
        # Check Solo Stats (CRITICAL FIX)
        if "solo" not in player_stats[user_id]:
            player_stats[user_id]["solo"] = default_solo.copy()
            changed = True
        else:
            # Fill missing keys in 'solo'
            for key, val in default_solo.items():
                if key not in player_stats[user_id]["solo"]:
                    player_stats[user_id]["solo"][key] = val
                    changed = True

        if changed: save_data()

# Player class to track individual player performance in a match
class Player:
    """Represents a player in the match"""
    def __init__(self, user_id: int, username: str, first_name: str):
        self.user_id = user_id
        self.username = username
        self.first_name = first_name
        self.runs = 0
        self.balls_faced = 0
        self.wickets = 0
        self.balls_bowled = 0
        self.runs_conceded = 0
        self.is_out = False
        self.dismissal_type = None
        self.dot_balls_faced = 0
        self.dot_balls_bowled = 0
        self.boundaries = 0
        self.sixes = 0
        self.overs_bowled = 0
        self.maiden_overs = 0
        self.no_balls = 0
        self.wides = 0
        self.has_bowled_this_over = False
        self.batting_timeouts = 0
        self.bowling_timeouts = 0
        self.is_bowling_banned = False
    
    def get_strike_rate(self) -> float:
        """Calculate batting strike rate"""
        if self.balls_faced == 0:
            return 0.0
        return round((self.runs / self.balls_faced) * 100, 2)
    
    def get_economy(self) -> float:
        """Calculate bowling economy rate"""
        if self.balls_bowled == 0:
            return 0.0
        overs = self.balls_bowled / 6
        if overs == 0:
            return 0.0
        return round(self.runs_conceded / overs, 2)
    
    def get_bowling_average(self) -> float:
        """Calculate bowling average"""
        if self.wickets == 0:
            return 0.0
        return round(self.runs_conceded / self.wickets, 2)

# Team class
class Team:
    """Represents a team in the match"""
    def __init__(self, name: str):
        self.name = name
        self.players: List[Player] = []
        self.captain_id: Optional[int] = None
        self.score = 0
        self.wickets = 0
        self.overs = 0.0
        self.balls = 0
        self.extras = 0
        self.drs_remaining = 1
        
        # Real Cricket Support
        self.current_batsman_idx: Optional[int] = None      # Striker
        self.current_non_striker_idx: Optional[int] = None  # Non-Striker
        self.out_players_indices = set() # Track who is out
        
        self.current_bowler_idx: Optional[int] = None
        self.penalty_runs = 0
        self.bowler_history: List[int] = []

    def is_all_out(self):
        # 1 player needs to remain not out to partner. If (Total - Out) < 2, then All Out.
        return (len(self.players) - len(self.out_players_indices)) < 2

    def swap_batsmen(self):
        """Swap Striker and Non-Striker"""
        if self.current_batsman_idx is not None and self.current_non_striker_idx is not None:
            self.current_batsman_idx, self.current_non_striker_idx = self.current_non_striker_idx, self.current_batsman_idx

    def add_player(self, player: Player):
        self.players.append(player)
    
    def remove_player(self, user_id: int) -> bool:
        for i, player in enumerate(self.players):
            if player.user_id == user_id:
                self.players.pop(i)
                return True
        return False
    
    def get_player(self, user_id: int) -> Optional[Player]:
        for player in self.players:
            if player.user_id == user_id:
                return player
        return None
    
    def get_player_by_serial(self, serial: int) -> Optional[Player]:
        if 1 <= serial <= len(self.players):
            return self.players[serial - 1]
        return None
    
    def get_available_bowlers(self) -> List[Player]:
        available = []
        last_bowler_idx = self.bowler_history[-1] if self.bowler_history else None
    
        for i, player in enumerate(self.players):
            if not player.is_bowling_banned and i != last_bowler_idx:
                available.append(player)
        return available
    
    def update_overs(self):
        """Update overs correctly - ball 1 = 0.1"""
        self.balls += 1
        complete_overs = (self.balls - 1) // 6  # -1 to make first ball = 0.1
        balls_in_over = ((self.balls - 1) % 6) + 1
        self.overs = complete_overs + (balls_in_over / 10)
    
    def get_current_over_balls(self) -> int:
        """Get balls in current over (1-6)"""
        return ((self.balls - 1) % 6) + 1 if self.balls > 0 else 0
    
    def complete_over(self):
        """Complete the current over"""
        remaining_balls = 6 - (self.balls % 6)
        self.balls += remaining_balls
        self.overs = self.balls // 6

# Match class - Core game engine
class Match:
    """Main match class that handles all game logic"""
    def __init__(self, group_id: int, group_name: str):
        self.group_id = group_id
        self.group_name = group_name
        self.phase = GamePhase.TEAM_JOINING
        self.match_id = f"{group_id}_{int(time.time())}"
        self.created_at = datetime.now()
        self.last_activity = time.time()  # Track last move time
        
        # Teams
        self.team_x = Team("Team X")
        self.team_y = Team("Team Y")
        self.editing_team: Optional[str] = None  # 'X' ya 'Y' store karega
        
        # Match settings
        self.host_id: Optional[int] = None
        self.total_overs = 0
        self.toss_winner: Optional[Team] = None
        self.batting_first: Optional[Team] = None
        self.bowling_first: Optional[Team] = None
        self.current_batting_team: Optional[Team] = None
        self.current_bowling_team: Optional[Team] = None
        
        # Match state
        self.innings = 1
        self.target = 0
        self.is_free_hit = False
        self.last_wicket_ball = None
        self.drs_in_progress = False
        self.team_x_timeout_used = False
        self.team_y_timeout_used = False
        
        # Timers and messages
        self.team_join_end_time: Optional[float] = None
        self.main_message_id: Optional[int] = None
        self.join_phase_task: Optional[asyncio.Task] = None
        
        # Ball tracking
        self.current_ball_data: Dict = {}
        self.ball_timeout_task: Optional[asyncio.Task] = None
        self.batsman_selection_task: Optional[asyncio.Task] = None
        self.bowler_selection_task: Optional[asyncio.Task] = None
        
        self.solo_players: List[Player] = [] # List of Player objects
        self.current_solo_bat_idx = 0
        self.current_solo_bowl_idx = 0
        self.solo_balls_this_spell = 0 # To track 3 ball rotation
        self.solo_join_end_time = 0
        self.host_change_votes = {}
        self.team_x_impact_count = 0  # Track number of substitutions used
        self.team_y_impact_count = 0
        self.team_x_impact_history = []  # List of (old_player_name, new_player_name)
        self.team_y_impact_history = []

        # Waiting states
        # Waiting states (FIXED: Added for batsman/bowler selection)
        self.waiting_for_batsman = False
        self.waiting_for_bowler = False
        self.batsman_selection_time: Optional[float] = None
        self.bowler_selection_time: Optional[float] = None

        # Game mode (for TEAM/SOLO distinction, default TEAM)
        self.game_mode = "TEAM"
        
        # Super over
        self.is_super_over = False
        self.super_over_batting_team: Optional[Team] = None
        
        # Match settings
        self.host_id: Optional[int] = None
        self.host_name: str = "Unknown"
        
        # Match log
        self.ball_by_ball_log: List[Dict] = []
        self.match_events: List[str] = []
    
    def get_team_by_name(self, name: str) -> Optional[Team]:
        """Get team by name"""
        if name == "Team X":
            return self.team_x
        elif name == "Team Y":
            return self.team_y
        return None
    
    def get_other_team(self, team: Team) -> Team:
        """Get the opposing team"""
        if team == self.team_x:
            return self.team_y
        return self.team_x
    
    def get_captain(self, team: Team) -> Optional[Player]:
        """Get team captain"""
        if team.captain_id:
            return team.get_player(team.captain_id)
        return None
    
    def add_event(self, event: str):
        """Add event to match log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.match_events.append(f"[{timestamp}] {event}")
    
    def get_required_run_rate(self) -> float:
        """Calculate required run rate for chasing team"""
        if self.innings != 2 or not self.current_batting_team:
            return 0.0
        
        runs_needed = self.target - self.current_batting_team.score
        balls_remaining = (self.total_overs * 6) - self.current_batting_team.balls
        
        if balls_remaining <= 0:
            return 0.0
        
        overs_remaining = balls_remaining / 6
        return round(runs_needed / overs_remaining, 2)
    
    def is_innings_complete(self) -> bool:
        """Check if current innings is complete"""
        if not self.current_batting_team or not self.current_bowling_team:
            return False
        
        # All out
        if self.current_batting_team.wickets >= len(self.current_batting_team.players) - 1:
            return True
        
        # Overs complete
        if self.current_batting_team.balls >= self.total_overs * 6:
            return True
        
        # Target chased in second innings
        if self.innings == 2 and self.current_batting_team.score >= self.target:
            return True
        
        return False
    
    def get_match_summary(self) -> str:
        """Generate detailed match summary"""
        summary_lines = []
        summary_lines.append("=" * 40)
        summary_lines.append("MATCH SUMMARY")
        summary_lines.append("=" * 40)
        summary_lines.append("")
        
        # First innings
        first_team = self.batting_first
        if first_team:
            summary_lines.append(f"{first_team.name}: {first_team.score}/{first_team.wickets}")
            summary_lines.append(f"Overs: {first_team.overs}")
            summary_lines.append("")
        
        # Second innings
        if self.innings >= 2:
            second_team = self.get_other_team(first_team)
            summary_lines.append(f"{second_team.name}: {second_team.score}/{second_team.wickets}")
            summary_lines.append(f"Overs: {second_team.overs}")
            summary_lines.append("")
        
        summary_lines.append("=" * 40)
        return "\n".join(summary_lines)

# Utility functions
def get_random_gif(event: MatchEvent) -> str:
    """Get random GIF for an event"""
    gifs = GIFS.get(event, [])
    if gifs:
        return random.choice(gifs)
    return ""

def get_random_commentary(event_type: str) -> str:
    """Get random commentary for an event"""
    comments = COMMENTARY.get(event_type, [])
    if comments:
        return random.choice(comments)
    return ""

def calculate_fifa_attributes(stats, mode="team"):
    """
    Advanced FIFA Rating Engine: Uses MOM, Captaincy & Hat-tricks for OVR
    """
    matches = stats.get("matches", 0)
    if matches == 0:
        return {"PAC": 0, "SHO": 0, "PAS": 0, "DRI": 0, "DEF": 0, "PHY": 0, "OVR": 0}

    runs = stats.get("runs", 0)
    balls = stats.get("balls", 0)
    wickets = stats.get("wickets", 0)
    
    # --- 1. PAC (Pace) -> Strike Rate & Speed ---
    sr = (runs / balls * 100) if balls > 0 else 0
    pac = min(99, int(sr / 2.5)) 
    
    # --- 2. SHO (Shooting) -> Batting Power ---
    # Boost for 6s and 4s
    boundaries = stats.get("fours", 0) + stats.get("sixes", 0)
    avg = (runs / matches) if matches > 0 else 0
    sho = min(99, int((avg * 1.5) + (boundaries / 2)))
    
    # --- 3. PAS (Passing) -> Consistency & Captaincy ---
    # Captaincy wins & MOMs boost Passing (Leadership)
    mom = stats.get("mom", 0)
    cap_wins = stats.get("captain_wins", 0)
    pas_base = int(matches * 1.5)
    pas_bonus = (mom * 5) + (cap_wins * 3)
    pas = min(99, pas_base + pas_bonus)
    
    # --- 4. DRI (Dribbling) -> Technique/Survival ---
    # Survival rate (balls faced per match)
    technique = (balls / matches) if matches > 0 else 0
    dri = min(99, int(technique * 4))
    
    # --- 5. DEF (Defense) -> Bowling & Hat-tricks ---
    hat_tricks = stats.get("hat_tricks", 0)
    defe_base = int(wickets * 4)
    defe_bonus = hat_tricks * 10
    defe = min(99, defe_base + defe_bonus)
    
    # --- 6. PHY (Physical) -> Workload ---
    balls_bowled = stats.get("balls_bowled", 0)
    workload = balls + balls_bowled
    phy = min(99, int(workload / 3))

    # --- OVR CALCULATION ---
    # Role detection for weighting
    if defe > sho: # Bowler
        base_ovr = (defe * 0.45) + (phy * 0.2) + (pas * 0.15) + (pac * 0.1) + (dri * 0.1)
    else: # Batsman
        base_ovr = (sho * 0.45) + (pac * 0.25) + (dri * 0.15) + (pas * 0.1) + (phy * 0.05)
        
    # Prestige Boost (MOMs & Milestones)
    prestige = (mom * 2) + stats.get("centuries", 0) * 3
    
    ovr = int(base_ovr + prestige)
    return {
        "PAC": pac, "SHO": sho, "PAS": pas, 
        "DRI": dri, "DEF": defe, "PHY": phy, 
        "OVR": min(99, max(45, ovr)) # Min 45 rating
    }

def generate_mini_scorecard(match: Match) -> str:
    """
    Generate Mini Scorecard with Current Stats
    Shows after: Wicket, Over Complete
    """
    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    # Calculate Run Rate
    overs_played = max(bat_team.overs, 0.1)
    current_rr = round(bat_team.score / overs_played, 2)
    
    # Get Current Batsmen
    striker = None
    non_striker = None
    if bat_team.current_batsman_idx is not None:
        striker = bat_team.players[bat_team.current_batsman_idx]
    if bat_team.current_non_striker_idx is not None:
        non_striker = bat_team.players[bat_team.current_non_striker_idx]
    
    # Get Last Bowler (Most Recent)
    last_bowler = None
    if bowl_team.current_bowler_idx is not None:
        last_bowler = bowl_team.players[bowl_team.current_bowler_idx]
    elif bowl_team.bowler_history:
        last_bowler_idx = bowl_team.bowler_history[-1]
        last_bowler = bowl_team.players[last_bowler_idx]
    
    # Build Message
    msg = "📊 <b>MINI SCORECARD</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Team Scores
    msg += f"🔵 <b>{match.team_x.name}:</b> {match.team_x.score}/{match.team_x.wickets} ({format_overs(match.team_x.balls)})\n"
    msg += f"🔴 <b>{match.team_y.name}:</b> {match.team_y.score}/{match.team_y.wickets} ({format_overs(match.team_y.balls)})\n\n"
    
    # Current Partnership
    msg += f"🏏 <b>BATTING - {bat_team.name}</b>\n"
    msg += f"📈 <b>Run Rate:</b> {current_rr}\n\n"
    
    if striker:
        sr = round((striker.runs / striker.balls_faced) * 100, 1) if striker.balls_faced > 0 else 0
        status = "*" if not striker.is_out else ""
        msg += f"🟢 <b>{striker.first_name}{status}:</b> {striker.runs} ({striker.balls_faced}) SR: {sr}\n"
    
    if non_striker:
        sr = round((non_striker.runs / non_striker.balls_faced) * 100, 1) if non_striker.balls_faced > 0 else 0
        status = "*" if not non_striker.is_out else ""
        msg += f"⚪ <b>{non_striker.first_name}{status}:</b> {non_striker.runs} ({non_striker.balls_faced}) SR: {sr}\n"
    
    msg += "\n"
    
    # Last Bowler Stats
    if last_bowler:
        econ = round(last_bowler.runs_conceded / max(last_bowler.balls_bowled/6, 0.1), 2)
        msg += f"⚾ <b>BOWLING - {bowl_team.name}</b>\n"
        msg += f"🎯 <b>{last_bowler.first_name}:</b> {last_bowler.wickets}/{last_bowler.runs_conceded} "
        msg += f"({format_overs(last_bowler.balls_bowled)}) Econ: {econ}\n"
    
    msg += "━━━━━━━━━━━━━━━━━━━━━"
    
    return msg

def get_card_design(ovr):
    """Returns Card Type & Emoji based on Rating"""
    if ovr >= 90: return "💎 ICON", "🔵" # Icon / TOTY
    elif ovr >= 85: return "⚡ HERO", "🟣" # Hero
    elif ovr >= 75: return "🥇 GOLD", "🟡" # Gold
    elif ovr >= 65: return "🥈 SILVER", "⚪" # Silver
    else: return "🥉 BRONZE", "🟤" # Bronze

def format_overs(balls: int) -> str:
    """Format balls to overs - First ball = 0.1"""
    if balls == 0:
        return "0.0"
    
    complete_overs = (balls - 1) // 6
    balls_in_over = ((balls - 1) % 6) + 1
    
    return f"{complete_overs}.{balls_in_over}"

def balls_to_float_overs(balls: int) -> float:
    """Convert balls to float overs"""
    return balls // 6 + (balls % 6) / 10

async def update_joining_board(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match):
    """
    Updates the Joining Board safely (Handling Photo Caption vs Text)
    """
    if not match.main_message_id: return

    # Generate fresh text
    text = get_team_join_message(match)
    
    # Buttons
    keyboard = [
        [InlineKeyboardButton("🔵 Join Team X", callback_data="join_team_x"),
         InlineKeyboardButton("🔴 Join Team Y", callback_data="join_team_y")],
        [InlineKeyboardButton("🚪 Leave Team", callback_data="leave_team")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        # Try editing as Photo Caption first (Since we are using Images)
        await context.bot.edit_message_caption(
            chat_id=chat_id,
            message_id=match.main_message_id,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        error_str = str(e).lower()
        
        # Agar "message is not modified" error hai, toh ignore karo (Sab same hai)
        if "message is not modified" in error_str:
            return
            
        # Agar error aaya ki "there is no caption" implies it's a TEXT message (Fallback)
        # Toh hum text edit karenge
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=match.main_message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
        except Exception as text_e:
            # Agar phir bhi fail hua, toh log karo par crash mat hone do
            pass

async def refresh_game_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match, caption: str, reply_markup: InlineKeyboardMarkup = None, media_key: str = None):
    """Smart Update: Edits existing message safely with HTML"""
    
    # Try editing first
    if match.main_message_id:
        try:
            if media_key and media_key in MEDIA_ASSETS:
                media = InputMediaPhoto(media=MEDIA_ASSETS[media_key], caption=caption, parse_mode=ParseMode.HTML)
                await context.bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=match.main_message_id,
                    media=media,
                    reply_markup=reply_markup
                )
            else:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=match.main_message_id,
                    text=caption,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
            return
        except Exception:
            pass # Edit failed (message deleted/too old), send new

    # Fallback: Send New
    try:
        if media_key and media_key in MEDIA_ASSETS:
            msg = await context.bot.send_photo(chat_id=chat_id, photo=MEDIA_ASSETS[media_key], caption=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            msg = await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        
        match.main_message_id = msg.message_id
        try: await context.bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id)
        except: pass
    except Exception as e:
        logger.error(f"Send failed: {e}")


# Is function ko add karo

# Important: Is function ko call karne ke liye niche wala update_team_edit_message use karo
async def update_team_edit_message(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Show Team Edit Panel (Final Fixed Version)"""
    
    # 1. Team List Text Generate Karo
    text = f"⚙️ <b>TEAM SETUP</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
    
    text += f"🔵 <b>Team X:</b>\n"
    for i, p in enumerate(match.team_x.players, 1):
        text += f"  {i}. {p.first_name}\n"
    if not match.team_x.players: text += "  (Empty)\n"
        
    text += f"\n🔴 <b>Team Y:</b>\n"
    for i, p in enumerate(match.team_y.players, 1):
        text += f"  {i}. {p.first_name}\n"
    if not match.team_y.players: text += "  (Empty)\n"
    text += "\n"

    # 2. Logic: Buttons based on State
    if match.editing_team:
        # --- SUB-MENU (Jab Edit Mode ON hai) ---
        text += f"🟢 <b>EDITING TEAM {match.editing_team}</b>\n"
        text += f"👉 Reply to user with <code>/add</code> to add.\n"
        text += f"👉 Reply to user with <code>/remove</code> to remove.\n"
        text += "👉 Click button below when done."
        
        # 'Done' button wapas Main Menu le jayega
        keyboard = [[InlineKeyboardButton(f"✅ Done with Team {match.editing_team}", callback_data="edit_back")]]
        
    else:
        # --- MAIN MENU (Team Select Karo) ---
        text += "👇 <b>Select a team to edit:</b>"
        keyboard = [
            # Note: Buttons ab 'edit_team_x' use kar rahe hain (no _mode)
            [InlineKeyboardButton("✏️ Edit Team X", callback_data="edit_team_x"), 
             InlineKeyboardButton("✏️ Edit Team Y", callback_data="edit_team_y")],
            [InlineKeyboardButton("✅ Finalize & Start", callback_data="team_edit_done")]
        ]

    await refresh_game_message(context, group_id, match, text, InlineKeyboardMarkup(keyboard), media_key="squads")

async def set_edit_team_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Edit Buttons & Set State Correctly"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    if user.id != match.host_id:
        await query.answer("⚠️ Only Host can edit!", show_alert=True)
        return

    # Button Logic (State Set Karo)
    if query.data == "edit_team_x":
        match.editing_team = "X"
    elif query.data == "edit_team_y":
        match.editing_team = "Y"
    elif query.data == "edit_back":
        match.editing_team = None # Back to Main Menu

    # UI Update Karo
    await update_team_edit_message(context, chat.id, match)

async def notify_support_group(context: ContextTypes.DEFAULT_TYPE, message: str):
    """Send notification to support group"""
    try:
        await context.bot.send_message(
            chat_id=SUPPORT_GROUP_ID,
            text=message
        )
    except Exception as e:
        logger.error(f"Failed to notify support group: {e}")

# --- CHEER COMMAND ---
async def cheer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cheer for a player by tagging them!"""
    chat = update.effective_chat
    user = update.effective_user
    
    # 1. Detect Target User
    target_name = "everyone"
    if update.message.reply_to_message:
        target_name = update.message.reply_to_message.from_user.first_name
    elif context.args:
        # Handle mentions like @username or text
        target_name = " ".join(context.args)

    # 2. Cheer Message
    cheer_msg = f"🎉 <b>CHEER SQUAD</b> 🎉\n"
    cheer_msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    cheer_msg += f"📣 <b>{user.first_name}</b> is screaming for <b>{target_name}</b>!\n\n"
    cheer_msg += "<i>\"COME ON! YOU GOT THIS! SHOW YOUR POWER! 🏏🔥\"</i>\n"
    cheer_msg += "━━━━━━━━━━━━━━━━━━━━━━"

    # 3. Send GIF
    await update.message.reply_animation(
        animation=MEDIA_ASSETS.get("cheer", "https://media.giphy.com/media/l41Yh18f5T01X55zW/giphy.gif"),
        caption=cheer_msg,
        parse_mode=ParseMode.HTML
    )


# --- SCORECARD COMMAND (Match Summary Style) ---
async def scorecard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ COMPLETE DETAILED SCORECARD - All Players with Full Stats"""
    chat = update.effective_chat
    
    if chat.id not in active_matches:
        await update.message.reply_text("⚠️ No live match running!")
        return

    match = active_matches[chat.id]
    
    # --- HELPER FUNCTIONS ---
    def get_batting_card(team):
        card = f"🏏 <b>{team.name} BATTING</b>\n"
        card += f"📊 <b>Score:</b> {team.score}/{team.wickets} ({format_overs(team.balls)})\n"
        card += "─────────────────────\n"
        
        batters = [p for p in team.players if p.balls_faced > 0 or p.is_out]
        if not batters:
            card += "<i>Yet to bat</i>\n"
        else:
            for p in batters:
                status = "*" if not p.is_out else ""
                sr = round((p.runs/max(p.balls_faced,1))*100, 1)
                card += f"<b>{p.first_name}{status}</b>: {p.runs} ({p.balls_faced}) SR: {sr}\n"
                if p.boundaries > 0 or p.sixes > 0:
                    card += f"  └ {p.boundaries}×4, {p.sixes}×6\n"
        
        card += f"\n<b>Extras:</b> {team.extras}\n"
        return card
    
    def get_bowling_card(team):
        card = f"⚾ <b>{team.name} BOWLING</b>\n"
        card += "─────────────────────\n"
        
        bowlers = [p for p in team.players if p.balls_bowled > 0]
        if not bowlers:
            card += "<i>No bowling data</i>\n"
        else:
            for p in bowlers:
                overs_str = format_overs(p.balls_bowled)
                econ = round(p.runs_conceded / max(p.balls_bowled/6, 0.1), 2)
                card += f"<b>{p.first_name}</b>: {p.wickets}/{p.runs_conceded} ({overs_str})\n"
                card += f"  └ Econ: {econ}\n"
        
        return card
    
    # --- BUILD FULL SCORECARD ---
    summary = "📊 <b>LIVE MATCH SCORECARD</b> 📊\n"
    summary += "═════════════════════\n\n"
    
    # Team X Batting
    summary += get_batting_card(match.team_x)
    summary += "\n"
    
    # Team X Bowling (Opponent's bowling)
    summary += get_bowling_card(match.team_y)
    summary += "\n═════════════════════\n\n"
    
    # Team Y Batting
    summary += get_batting_card(match.team_y)
    summary += "\n"
    
    # Team Y Bowling
    summary += get_bowling_card(match.team_x)
    summary += "\n═════════════════════"

    await update.message.reply_photo(
        photo=MEDIA_ASSETS.get("scorecard"),
        caption=summary,
        parse_mode=ParseMode.HTML
    )

async def cleanup_inactive_matches(context: ContextTypes.DEFAULT_TYPE):
    """Auto-end matches inactive for > 15 minutes"""
    current_time = time.time()
    inactive_threshold = 15 * 60  # 15 Minutes in seconds
    chats_to_remove = []

    # Check all active matches
    for chat_id, match in active_matches.items():
        if current_time - match.last_activity > inactive_threshold:
            chats_to_remove.append(chat_id)
            try:
                # Send Time Out Message
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="⏰ <b>Game Timeout!</b>\nMatch ended automatically due to 15 mins of inactivity.",
                    parse_mode=ParseMode.HTML
                )
                # Unpin message
                if match.main_message_id:
                    await context.bot.unpin_chat_message(chat_id=chat_id, message_id=match.main_message_id)
            except Exception as e:
                logger.error(f"Error ending inactive match {chat_id}: {e}")

    # Remove from memory
    for chat_id in chats_to_remove:
        if chat_id in active_matches:
            del active_matches[chat_id]

async def game_timer(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, player_type: str, player_name: str):
    """Handles 45s timer with Penalties & Disqualification"""
    try:
        # Wait 30 seconds
        await asyncio.sleep(30)
        
        # Warning
        await context.bot.send_message(group_id, f"⏳ <b>Hurry Up {player_name}!</b> 15 seconds left!", parse_mode=ParseMode.HTML)
        
        # Wait remaining 15 seconds
        await asyncio.sleep(15)
        
        # --- TIMEOUT HAPPENED ---
        await handle_timeout_penalties(context, group_id, match, player_type)
            
    except asyncio.CancelledError:
        pass # Timer stopped safely because player played

async def handle_timeout_penalties(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, player_type: str):
    """Process Penalties for Timeouts"""
    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    # --- BOWLER TIMEOUT ---
    if player_type == "bowler":
        bowler = bowl_team.players[bowl_team.current_bowler_idx]
        bowler.bowling_timeouts += 1
        
        # Case A: Disqualification (3 Timeouts)
        if bowler.bowling_timeouts >= 3:
            msg = f"🚫 <b>DISQUALIFIED!</b> {bowler.first_name} timed out 3 times!\n"
            msg += "⚠️ <b>The over will RESTART with a new bowler.</b>"
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            # Reset Balls for this over (Over restart logic)
            # Example: If balls were 3.2 (20 balls), reset to 3.0 (18 balls)
            current_over_balls = bowl_team.get_current_over_balls()
            bowl_team.balls -= current_over_balls
            
            # Remove bowler from attack
            bowl_team.current_bowler_idx = None
            bowler.is_bowling_banned = True # Ban for this match (optional, or just this over)
            
            # Request New Bowler
            match.current_ball_data = {} # Clear ball data
            await request_bowler_selection(context, group_id, match)
            return

        # Case B: No Ball (Standard Timeout)
        else:
            bat_team.score += 1 # Penalty Run
            bat_team.extras += 1
            match.is_free_hit = True # Activate Free Hit
            
            msg = f"⏰ <b>BOWLER TIMEOUT!</b> ({bowler.bowling_timeouts}/3)\n"
            msg += "🚫 <b>Result:</b> NO BALL! (+1 Run)\n"
            msg += "⚡ <b>Next ball is a FREE HIT!</b>"
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            # Reset inputs to allow re-bowl (No ball doesn't count legal ball)
            match.current_ball_data = {"bowler_id": bowler.user_id, "bowler_number": None, "group_id": group_id}
            
            # Restart Bowler Timer for re-bowl
            match.ball_timeout_task = asyncio.create_task(game_timer(context, group_id, match, "bowler", bowler.first_name))
            
            # Notify Bowler again
            try: await context.bot.send_message(bowler.user_id, "⚠️ <b>Timeout! It's a No Ball. Bowl again!</b>", parse_mode=ParseMode.HTML)
            except: pass

    # --- BATSMAN TIMEOUT ---
    elif player_type == "batsman":
        striker = bat_team.players[bat_team.current_batsman_idx]
        striker.batting_timeouts += 1
        
        # Case A: Hit Wicket (3 Timeouts)
        if striker.batting_timeouts >= 3:
            msg = f"🚫 <b>DISMISSED!</b> {striker.first_name} timed out 3 times.\n"
            msg += "❌ <b>Result:</b> HIT WICKET (OUT)!"
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            # Trigger Wicket Logic Manually
            match.current_ball_data["batsman_number"] = match.current_ball_data["bowler_number"] # Force match numbers to trigger out
            await process_ball_result(context, group_id, match)
            
        # Case B: Penalty Runs (-6 Runs)
        else:
            bat_team.score -= 6
            bat_team.score = max(0, bat_team.score) # Score negative nahi jayega
            
            msg = f"⏰ <b>BATSMAN TIMEOUT!</b> ({striker.batting_timeouts}/3)\n"
            msg += "📉 <b>Penalty:</b> -6 Runs!\n"
            msg += f"📊 <b>Score:</b> {bat_team.score}/{bat_team.wickets}\n"
            msg += "🔄 <b>Ball Counted.</b> (Dot Ball)"
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            # Count ball but no runs (Treat as Dot Ball)
            bowl_team.update_overs()
            match.current_ball_data = {} # Reset
            
            if bowl_team.get_current_over_balls() == 0:
                await check_over_complete(context, group_id, match)
            else:
                await execute_ball(context, group_id, match)

async def taunt_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Light Taunt"""

    chat = update.effective_chat
    user = update.effective_user

    if chat.id not in active_matches:
        await update.message.reply_text("⚠️ No active match right now.")
        return

    taunts = [
        "😏 Is that all you got? We're just warming up!",
        "🤔 Did you even practice? This is too easy!",
        "😎 Thanks for the practice session! Who's next?",
        "🎭 Are we playing cricket or waiting for miracles?",
        "⚡ Blink and you'll miss our victory! Too fast for you?"
    ]

    msg = (
        f"💬 <b>{user.first_name}</b> throws a taunt!\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{random.choice(taunts)}"
    )

    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def celebrate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Celebration GIF"""

    chat = update.effective_chat
    user = update.effective_user

    if chat.id not in active_matches:
        await update.message.reply_text("⚠️ No active match right now.")
        return

    celebration_gifs = [
        "https://media.giphy.com/media/g9582DNuQppxC/giphy.gif",
        "https://media.giphy.com/media/l0MYt5jPR6QX5pnqM/giphy.gif",
        "https://media.giphy.com/media/Is1O1TWV0LEJi/giphy.gif"
    ]

    caption = (
        f"🎉 <b>{user.first_name}</b> celebrates in style! 🎊\n\n"
        "<i>\"YESSS! That's how it's done!\"</i> 🔥"
    )

    await update.message.reply_animation(
        animation=random.choice(celebration_gifs),
        caption=caption,
        parse_mode=ParseMode.HTML
    )


async def huddle_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Team Motivation"""

    chat = update.effective_chat
    user = update.effective_user

    if chat.id not in active_matches:
        await update.message.reply_text("⚠️ No active match right now.")
        return

    huddle_messages = [
        "🔥 <b>COME ON TEAM!</b> We got this! Let's show them what we're made of! 💪",
        "⚡ <b>FOCUS UP!</b> One ball at a time. We're in this together! 🤝",
        "🎯 <b>STAY CALM!</b> Stick to the plan. Victory is ours! 🏆",
        "💥 <b>LET'S GO!</b> Time to dominate! Show no mercy! ⚔️",
        "🌟 <b>BELIEVE!</b> We've trained for this. Execute perfectly! ✨"
    ]

    msg = (
        f"📣 <b>{user.first_name}</b> calls a team huddle!\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{random.choice(huddle_messages)}"
    )

    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def solo_game_timer(context, chat_id, match, player_type, player_name):
    """Timer specifically for Solo Mode (45s)"""
    try:
        # Wait 30 seconds
        await asyncio.sleep(30)
        try:
            await context.bot.send_message(
                chat_id, 
                f"⏳ <b>Hurry Up {player_name}!</b> 15 seconds left!", 
                parse_mode=ParseMode.HTML
            )
        except: pass
        
        # Remaining 15 Seconds
        await asyncio.sleep(15)
        
        # Timeout Trigger
        await handle_solo_timeout(context, chat_id, match, player_type)
            
    except asyncio.CancelledError:
        pass

async def handle_solo_timeout(context, chat_id, match, player_type):
    """Handle Penalties for Solo Mode Timeouts"""
    
    # --- BATSMAN TIMEOUT ---
    if player_type == "batsman":
        batter = match.solo_players[match.current_solo_bat_idx]
        batter.batting_timeouts += 1
        
        # 3 Timeouts = AUTO OUT
        if batter.batting_timeouts >= 3:
            msg = f"🚫 <b>AUTO-OUT!</b> {batter.first_name} timed out 3 times.\n❌ <b>Result:</b> Wicket!"
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
            
            # Simulate Wicket Logic
            match.current_ball_data["batsman_number"] = match.current_ball_data["bowler_number"]
            await process_solo_turn_result(context, chat_id, match)
            return

        # < 3 Timeouts = -6 Penalty
        else:
            penalty = 6
            batter.runs -= penalty
            if batter.runs < 0: batter.runs = 0
            
            msg = f"⏰ <b>TIMEOUT WARNING!</b> ({batter.batting_timeouts}/3)\n"
            msg += f"📉 <b>Penalty:</b> -6 Runs deducted!\n"
            msg += f"📊 <b>Current Score:</b> {batter.runs}"
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
            
            batter.balls_faced += 1
            await rotate_solo_bowler(context, chat_id, match)

    # --- BOWLER TIMEOUT ---
    elif player_type == "bowler":
        bowler = match.solo_players[match.current_solo_bowl_idx]
        batter = match.solo_players[match.current_solo_bat_idx]
        bowler.bowling_timeouts += 1
        
        # 3 Timeouts = BANNED
        if bowler.bowling_timeouts >= 3:
            bowler.is_bowling_banned = True
            msg = f"🚫 <b>BANNED!</b> {bowler.first_name} timed out 3 times!\n"
            msg += "⚠️ <b>They are removed from bowling rotation!</b>"
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
            
            await rotate_solo_bowler(context, chat_id, match, force_new_bowler=True)
            return
            
        # < 3 Timeouts = NO BALL + FREE HIT
        else:
            batter.runs += 1
            match.is_free_hit = True
            
            msg = f"⏰ <b>BOWLER TIMEOUT!</b> ({bowler.bowling_timeouts}/3)\n"
            msg += "🚫 <b>Result:</b> NO BALL! (+1 Run)\n"
            msg += "⚡ <b>Next ball is a FREE HIT!</b>\n"
            msg += "🔄 <i>Bowler must bowl again!</i>"
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
            
            match.ball_timeout_task = asyncio.create_task(
                solo_game_timer(context, chat_id, match, "bowler", bowler.first_name)
            )

# Helper to Rotate Bowler in Solo (Handles the Skip Logic)
async def rotate_solo_bowler(context, chat_id, match, force_new_bowler=False):
    """Rotates bowler, skipping banned players"""
    
    if not force_new_bowler:
        match.solo_balls_this_spell += 1

    if match.solo_balls_this_spell >= 3 or force_new_bowler:
        match.solo_balls_this_spell = 0
        
        original_idx = match.current_solo_bowl_idx
        attempts = 0
        total_players = len(match.solo_players)
        
        while True:
            match.current_solo_bowl_idx = (match.current_solo_bowl_idx + 1) % total_players
            
            if match.current_solo_bowl_idx == match.current_solo_bat_idx:
                match.current_solo_bowl_idx = (match.current_solo_bowl_idx + 1) % total_players
                
            next_bowler = match.solo_players[match.current_solo_bowl_idx]
            
            if not next_bowler.is_bowling_banned:
                break
            
            attempts += 1
            if attempts > total_players:
                await context.bot.send_message(chat_id, "⚠️ All players banned! Game Over.")
                await end_solo_game_logic(context, chat_id, match)
                return

        new_bowler = match.solo_players[match.current_solo_bowl_idx]
        await context.bot.send_message(chat_id, f"🔄 <b>New Bowler:</b> {new_bowler.first_name}", parse_mode=ParseMode.HTML)

    await trigger_solo_ball(context, chat_id, match)

# Start command handler
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Welcome message with Support Notification"""
    user = update.effective_user
    chat = update.effective_chat
    
    is_new_user = False
    
    # User Init logic
    if user.id not in user_data:
        user_data[user.id] = {
            "user_id": user.id,
            "username": user.username or "",
            "first_name": user.first_name,
            "started_at": datetime.now().isoformat(),
            "total_matches": 0
        }
        init_player_stats(user.id)
        save_data()
        is_new_user = True

        # 📢 NOTIFY SUPPORT GROUP (New User)
        if chat.type == "private":
            try:
                await context.bot.send_message(
                    chat_id=SUPPORT_GROUP_ID,
                    text=f"🆕 <b>New User Started Bot</b>\n👤 {user.first_name} (<a href='tg://user?id={user.id}'>{user.id}</a>)\n🎈 @{user.username}",
                    parse_mode=ParseMode.HTML
                )
            except Exception: pass

    welcome_text = "🏏 <b>WELCOME TO CRICOVERSE</b> 🏏\n"
    welcome_text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    welcome_text += "The ultimate Hand Cricket experience on Telegram.\n\n"
    welcome_text += "🔥 <b>Features:</b>\n"
    welcome_text += "• 🏟 Group Matches\n"
    welcome_text += "• 📺 DRS System\n"
    welcome_text += "• 📊 Career Stats\n"
    welcome_text += "• 🎙 Live Commentary\n\n"
    welcome_text += "👇 <b>How to Play:</b>\n"
    welcome_text += "Add me to your group and type <code>/game</code> to start!"

    
    if chat.type == "private":
        await update.message.reply_photo(photo=MEDIA_ASSETS["welcome"], caption=welcome_text, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Bot is ready! Use /game to start.")

# Help command
# --- HELPER FUNCTIONS FOR HELP MENU ---
def get_help_main_text():
    return (
        "❓ <b>CRICOVERSE HELP CENTER</b> ❓\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "Welcome to the ultimate cricket bot!\n\n"
        "👇 <b>Select a category below:</b>\n\n"
        "👥 <b>Team Mode:</b> Commands for Team matches.\n"
        "⚔️ <b>Solo Mode:</b> Commands for Solo survival.\n"
        "📚 <b>Tutorial:</b> How to play guide."
    )

def draw_bar(value, length=10):
    """Creates a visual progress bar based on value (0-99)"""
    # 99 value = Full Bar, 0 = Empty
    filled_len = int((value / 100) * length)
    bar = "▰" * filled_len + "▱" * (length - filled_len)
    return bar

def calculate_detailed_ratings(stats, mode="team"):
    """Calculates granular ratings (0-99) for attributes"""
    matches = stats.get("matches", 0)
    if matches == 0: return {"PAC": 0, "SHO": 0, "PAS": 0, "DRI": 0, "DEF": 0, "PHY": 0, "OVR": 0}

    runs = stats.get("runs", 0)
    balls = stats.get("balls", 0)
    wickets = stats.get("wickets", 0)
    
    # Logic adjustment based on mode
    if mode == "team":
        sr = (runs / balls * 100) if balls > 0 else 0
        avg = (runs / matches) if matches > 0 else 0
        
        pac = min(99, int(sr / 2.5))              # Pace = Strike Rate
        sho = min(99, int(avg * 1.5) + int(stats.get("sixes", 0))) # Shooting = Avg + Power
        pas = min(99, int(matches * 1.5) + int(stats.get("fifties", 0)*5)) # Passing = Experience/Consistency
        dri = min(99, int((balls/matches) * 2))   # Dribbling = Time spent on crease
        defe = min(99, int(wickets * 4))          # Defense = Wickets
        phy = min(99, int((runs + (wickets*20)) / matches)) # Physical = Overall Impact
        
    else: # SOLO MODE (Survival Logic)
        wins = stats.get("wins", 0)
        
        pac = min(99, int(runs / matches))        # Pace = Avg Damage per game
        sho = min(99, int(wins * 10))             # Shooting = Wins (Killer Instinct)
        pas = min(99, int(stats.get("top_3_finishes", 0) * 5)) # Consistency in Top 3
        dri = min(99, int((balls/matches) * 3))   # Dribbling = Survival Time
        defe = min(99, int(wickets * 5))          # Defense = Knockouts
        phy = min(99, int(matches * 2))           # Physical = Veteran Status

    # OVR Calculation
    total = pac + sho + pas + dri + defe + phy
    ovr = int(total / 6) + 5 # Base boost
    
    return {"PAC": pac, "SHO": sho, "PAS": pas, "DRI": dri, "DEF": defe, "PHY": phy, "OVR": min(99, ovr)}

def get_help_team_text():
    return (
        "👥 <b>TEAM MODE COMMANDS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🛠 <b>Host Commands:</b>\n"
        "• <code>/game</code> - Setup Match\n"
        "• <code>/extend 60</code> - Add joining time\n"
        "• <code>/endmatch</code> - Force End Game\n"
        "• <code>/timeout</code> - Take Strategic Timeout\n\n"
        
        "🧢 <b>Captain Commands:</b>\n"
        "• <code>/batting [no]</code> - Select Batsman\n"
        "• <code>/bowling [no]</code> - Select Bowler\n"
        "• <code>/drs</code> - Review Wicket Decision\n\n"
        
        "📊 <b>General:</b>\n"
        "• <code>/scorecard</code> - View Match Summary\n"
        "• <code>/players</code> - View Squads\n"
        "• <code>/mystats</code> - Your Career Profile"
    )

def get_help_solo_text():
    return (
        "⚔️ <b>SOLO MODE COMMANDS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>1 vs All. Infinite Batting. Auto-Rotation.</i>\n\n"
        
        "🛠 <b>Host Commands:</b>\n"
        "• <code>/game</code> - Select 'Solo Mode' button\n"
        "• <code>/extendsolo 60</code> - Add joining time\n"
        "• <code>/endsolo</code> - End Game & Show Winner\n\n"
        
        "👤 <b>Player Commands:</b>\n"
        "• <code>/soloscore</code> - Live Leaderboard\n"
        "• <code>/soloplayers</code> - Player Status List\n"
        "• <code>/mystats</code> - Your Career Profile\n\n"
        
        "🎮 <b>Gameplay:</b>\n"
        "• <b>Batting:</b> Send 0-6 in Group Chat.\n"
        "• <b>Bowling:</b> Send 0-6 in Bot DM."
    )

def get_help_tutorial_text():
    return (
        "📚 <b>HOW TO PLAY</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>1. Starting a Game:</b>\n"
        "• Add bot to a group.\n"
        "• Type <code>/game</code> and choose Mode.\n"
        "• Players click 'Join'. Host clicks 'Start'.\n\n"
        
        "<b>2. Batting & Bowling:</b>\n"
        "• <b>Bowler:</b> Receives a DM from Bot. Sends a number (0-6).\n"
        "• <b>Batsman:</b> Bot alerts in Group. Batsman sends number (0-6) in Group.\n\n"
        
        "<b>3. Scoring:</b>\n"
        "• Same Number = <b>OUT</b> ❌\n"
        "• Different Number = <b>RUNS</b> 🏏\n\n"
        
        "<i>Tip: Use /mystats to check your RPG Level!</i>"
    )

# --- MAIN HELP COMMAND ---
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Interactive Help Menu"""
    
    # Main Menu Keyboard
    keyboard = [
        [InlineKeyboardButton("👥 Team Mode", callback_data="help_team"),
         InlineKeyboardButton("⚔️ Solo Mode", callback_data="help_solo")],
        [InlineKeyboardButton("📚 Tutorial", callback_data="help_tutorial")],
        [InlineKeyboardButton("❌ Close", callback_data="help_close")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send Photo with Caption
    await update.message.reply_photo(
        photo=MEDIA_ASSETS.get("help", "https://t.me/cricoverse/6"), # Fallback URL added
        caption=get_help_main_text(),
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )

# --- CALLBACK HANDLER FOR HELP NAVIGATION ---
async def help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Help Menu Navigation"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "help_close":
        await query.message.delete()
        return

    # Determine Text based on selection
    if data == "help_main":
        text = get_help_main_text()
        keyboard = [
            [InlineKeyboardButton("👥 Team Mode", callback_data="help_team"),
             InlineKeyboardButton("⚔️ Solo Mode", callback_data="help_solo")],
            [InlineKeyboardButton("📚 Tutorial", callback_data="help_tutorial")],
            [InlineKeyboardButton("❌ Close", callback_data="help_close")]
        ]
    
    elif data == "help_team":
        text = get_help_team_text()
        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="help_main")]]
        
    elif data == "help_solo":
        text = get_help_solo_text()
        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="help_main")]]
        
    elif data == "help_tutorial":
        text = get_help_tutorial_text()
        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="help_main")]]
    
    # Update the Caption (Image remains same)
    try:
        await query.message.edit_caption(
            caption=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass # Ignore if message not modified
        
async def game_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Game start menu with Solo, Team, and Tournament options"""
    chat = update.effective_chat
    user = update.effective_user

    if not await check_group_ban(update, context):
        return
    
    if chat.type == "private":
        await update.message.reply_text("This command only works in groups!")
        return
    
    if chat.id in active_matches:
        await update.message.reply_text("⚠️ Match already in progress!")
        return
        
    # Check if New Group
    if chat.id not in registered_groups:
        registered_groups[chat.id] = {"group_id": chat.id, "group_name": chat.title, "total_matches": 0}
        save_data()
            # 📢 NOTIFY SUPPORT GROUP (New Group)
        try:
            invite_link = ""
            try: invite_link = await context.bot.export_chat_invite_link(chat.id)
            except: pass
            msg = f"🆕 <b>Bot Added to New Group</b>\n"
            msg += f"fw <b>{chat.title}</b>\n"
            msg += f"🆔 <code>{chat.id}</code>\n"
            msg += f"👤 Added by: {user.first_name}\n"
            if invite_link: msg += f"🔗 {invite_link}"
            await context.bot.send_message(chat_id=SUPPORT_GROUP_ID, text=msg, parse_mode=ParseMode.HTML)

        except Exception: pass
    # --- UPDATED MENU BUTTONS ---
    keyboard = [
        [InlineKeyboardButton("👥 Team Mode", callback_data="mode_team"),
        InlineKeyboardButton("⚔️ Solo Mode", callback_data="mode_solo")],
        [InlineKeyboardButton("🏆 Tournament (Coming Soon)", callback_data="mode_tournament")]
    ]
    
    msg = "🎮 <b>SELECT GAME MODE</b> 🎮\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    
    await update.message.reply_photo(
        photo=MEDIA_ASSETS.get("mode_select", "https://t.me/cricoverse/7"),
        caption=msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

# Callback query handler for mode selection
async def mode_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat = query.message.chat
    user = query.from_user
    
    if chat.id in active_matches:
        await query.message.reply_text("⚠️ Match already active!")
        return
        
    if query.data == "mode_team":
        # Call existing Team Logic
        await start_team_mode(query, context, chat, user)
        
    elif query.data == "mode_solo":
        # START SOLO LOGIC
        match = Match(chat.id, chat.title)
        match.game_mode = "SOLO"
        match.phase = GamePhase.SOLO_JOINING
        match.host_id = user.id
        match.host_name = user.first_name  # CRITICAL: Save host name
        match.solo_join_end_time = time.time() + 120  # 2 mins
        
        active_matches[chat.id] = match
        
        # Host ko player list me add karo
        if user.id not in player_stats: init_player_stats(user.id)
        p = Player(user.id, user.username or "", user.first_name)
        match.solo_players.append(p)
        
        # Show Board
        await update_solo_board(context, chat.id, match)
        
        # START TIMER TASK (CRITICAL FIX)
        match.solo_timer_task = asyncio.create_task(
            solo_join_countdown(context, chat.id, match)
        )
    
    elif query.data == "mode_tournament":
        await query.answer("🏆 Tournament Mode is coming soon!", show_alert=True)
        

async def update_solo_board(context, chat_id, match):
    """Updates the Solo Joining List with Host Tag"""
    count = len(match.solo_players)
    
    # Time Calc
    remaining = int(match.solo_join_end_time - time.time())
    if remaining < 0: remaining = 0
    mins, secs = divmod(remaining, 60)
    
    # Generate Host Tag
    if match.host_id and match.host_name:
        host_tag = f"<a href='tg://user?id={match.host_id}'>{match.host_name}</a>"
    else:
        host_tag = "Unknown"
    
    msg = "⚔️ <b>SOLO BATTLE ROYALE</b> ⚔️\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🎙 <b>Host:</b> {host_tag}\n"
    msg += f"⏳ <b>Time Left:</b> <code>{mins:02d}:{secs:02d}</code>\n"
    msg += f"👥 <b>Players Joined:</b> {count}\n\n"
    
    msg += "<b>PLAYER LIST:</b>\n"
    if match.solo_players:
        for i, p in enumerate(match.solo_players, 1):
            ptag = f"<a href='tg://user?id={p.user_id}'>{p.first_name}</a>"
            msg += f"  {i}. {ptag}\n"
    else:
        msg += "  <i>Waiting for players...</i>\n"
        
    msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "👇 <i>Click Join to Enter the Ground!</i>"
    
    # Buttons
    keyboard = [
        [InlineKeyboardButton("✅ Join Battle", callback_data="solo_join"),
         InlineKeyboardButton("🚪 Leave", callback_data="solo_leave")]
    ]
    
    # Show START button if enough players
    if count >= 2:
        keyboard.append([InlineKeyboardButton("🚀 START MATCH", callback_data="solo_start_game")])
        
    await refresh_game_message(context, chat_id, match, msg, InlineKeyboardMarkup(keyboard), media_key="joining")

async def start_team_mode(query, context: ContextTypes.DEFAULT_TYPE, chat, user):
    """Initialize team mode with Fancy Image"""
    # Create new match
    match = Match(chat.id, chat.title)
    active_matches[chat.id] = match
    
    # Set time (2 minutes)
    match.team_join_end_time = time.time() + 120
    
    # Buttons
    keyboard = [
        [InlineKeyboardButton("🔵 Join Team X", callback_data="join_team_x"),
         InlineKeyboardButton("🔴 Join Team Y", callback_data="join_team_y")],
        [InlineKeyboardButton("🚪 Leave Team", callback_data="leave_team")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Fancy Text
    text = get_team_join_message(match)
    
    # Send using Master Function (With Image)
    await refresh_game_message(context, chat.id, match, text, reply_markup, media_key="joining")
    
    # Start Timer
    match.join_phase_task = asyncio.create_task(
        team_join_countdown(context, chat.id, match)
    )

def get_team_join_message(match: Match) -> str:
    """Generate Professional Joining List"""
    remaining = max(0, int(match.team_join_end_time - time.time()))
    minutes = remaining // 60
    seconds = remaining % 60
    
    total_p = len(match.team_x.players) + len(match.team_y.players)
    
    msg = "🏆 <b>CRICOVERSE MATCH REGISTRATION</b> 🏆\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⏳ <b>Time Remaining:</b> <code>{minutes:02d}:{seconds:02d}</code>\n"
    msg += f"👥 <b>Total Players:</b> {total_p}\n\n"
    
    # Team X List
    msg += "🔵 <b>TEAM X</b>\n"
    if match.team_x.players:
        for i, p in enumerate(match.team_x.players, 1):
            msg += f"  ├ {i}. {p.first_name}\n"
    else:
        msg += "  └ <i>Waiting for players...</i>\n"
    
    msg += "\n"
    
    # Team Y List
    msg += "🔴 <b>TEAM Y</b>\n"
    if match.team_y.players:
        for i, p in enumerate(match.team_y.players, 1):
            msg += f"  ├ {i}. {p.first_name}\n"
    else:
        msg += "  └ <i>Waiting for players...</i>\n"
            
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<i>Click buttons below to join your squad!</i>"
    
    return msg

async def team_join_countdown(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Countdown timer that updates the Board safely"""
    try:
        warning_sent = False
        while True:
            # ✅ FIX: Agar Phase Joining nahi hai, to Timer band kar do
            if match.phase != GamePhase.TEAM_JOINING:
                break

            remaining = match.team_join_end_time - time.time()
            
            # 30 Seconds Warning
            if remaining <= 30 and remaining > 20 and not warning_sent:
                await context.bot.send_message(
                    group_id, 
                    "⚠️ <b>Hurry Up! Only 30 seconds left to join!</b>", 
                    parse_mode=ParseMode.HTML
                )
                warning_sent = True

            # Time Up
            if remaining <= 0:
                await end_team_join_phase(context, group_id, match)
                break
            
            # Wait 10 seconds
            await asyncio.sleep(10)
            
            # ✅ FIX: Update karne se pehle phir check karo
            if match.phase == GamePhase.TEAM_JOINING:
                await update_joining_board(context, group_id, match)
            
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Timer error: {e}")

async def end_team_join_phase(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """End joining phase and start Host Selection"""
    total_players = len(match.team_x.players) + len(match.team_y.players)
    
    # Min 4 Players Check
    if total_players < 4:
        await context.bot.send_message(
            chat_id=group_id,
            text="❌ <b>Match Cancelled!</b>\nYou need at least 4 players (2 per team) to start.",
            parse_mode=ParseMode.HTML
        )
        try: await context.bot.unpin_chat_message(group_id, match.main_message_id)
        except: pass
        del active_matches[group_id]
        return
    
    match.phase = GamePhase.HOST_SELECTION
    
    keyboard = [[InlineKeyboardButton("🙋‍♂️ I Want to be Host", callback_data="become_host")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    host_text = f"✅ <b>REGISTRATION CLOSED!</b>\n"
    host_text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    host_text += f"Total Players: <b>{total_players}</b>\n\n"
    host_text += "<b>Who wants to be the Host?</b>\n"
    host_text += "<i>Host will select overs and finalize the teams.</i>"
    
    # Send with Host Image and Pin
    await refresh_game_message(context, group_id, match, host_text, reply_markup, media_key="host")

# Team join/leave callback handlers
async def team_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle team join/leave with tagging alerts & Auto-Update"""
    query = update.callback_query
    
    # Quick answer to stop loading animation
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TEAM_JOINING:
        await context.bot.send_message(chat.id, "⚠️ Joining phase has ended!")
        return
    
    # Initialize User Data
    if user.id not in user_data:
        user_data[user.id] = {
            "user_id": user.id,
            "username": user.username or "",
            "first_name": user.first_name,
            "started_at": datetime.now().isoformat(),
            "total_matches": 0
        }
        init_player_stats(user.id)
        save_data()

    user_tag = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
    alert_msg = ""
    updated = False

    # JOIN LOGIC
    if query.data == "join_team_x":
        if not match.team_x.get_player(user.id):
            if match.team_y.get_player(user.id):
                match.team_y.remove_player(user.id)
            
            player = Player(user.id, user.username or "", user.first_name)
            match.team_x.add_player(player)
            alert_msg = f"✅ {user_tag} joined <b>Team X</b>!"
            updated = True
    
    elif query.data == "join_team_y":
        if not match.team_y.get_player(user.id):
            if match.team_x.get_player(user.id):
                match.team_x.remove_player(user.id)
            
            player = Player(user.id, user.username or "", user.first_name)
            match.team_y.add_player(player)
            alert_msg = f"✅ {user_tag} joined <b>Team Y</b>!"
            updated = True
    
    elif query.data == "leave_team":
        if match.team_x.remove_player(user.id) or match.team_y.remove_player(user.id):
            alert_msg = f"👋 {user_tag} left the team."
            updated = True

    # 1. Send Alert in Group (Naya message)
    if alert_msg:
        await context.bot.send_message(chat.id, alert_msg, parse_mode=ParseMode.HTML)

    # 2. Update the Board (Agar change hua hai)
    if updated:
        await update_joining_board(context, chat.id, match)

# Extend command (Admins only)
async def extend_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /extend command (Text only)"""
    chat = update.effective_chat
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    try:
        seconds = int(context.args[0])
    except:
        await update.message.reply_text("Use: /extend <seconds>")
        return

    match.team_join_end_time += seconds
    
    await update.message.reply_text(
        f"⏳ <b>Time Extended!</b>\nAdded +{seconds} seconds to joining phase.",
        parse_mode=ParseMode.HTML
    )
    
    # Refresh Game Board
    text = get_team_join_message(match)
    keyboard = [
        [InlineKeyboardButton("🔵 Join Team X", callback_data="join_team_x"),
         InlineKeyboardButton("🔴 Join Team Y", callback_data="join_team_y")],
        [InlineKeyboardButton("🚪 Leave Team", callback_data="leave_team")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Use master function to keep it at bottom and pinned
    await refresh_game_message(context, chat.id, match, text, reply_markup=reply_markup, media_key="joining")

# Host selection callback
# Host selection callback
async def host_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Host Selection safely with 4-20 Overs"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        return
        
    match = active_matches[chat.id]
    
    # Check if someone is already host
    if match.host_id is not None:
        await query.answer("Host already selected!", show_alert=True)
        return

    # Set Host
    match.host_id = user.id
    match.host_name = user.first_name
    match.last_activity = time.time()  # Reset timer
    
    match.phase = GamePhase.OVER_SELECTION
    
    # --- LOGIC FOR 4 TO 20 OVERS ---
    keyboard = []
    row = []
    # Loop from 4 to 20 (inclusive)
    for i in range(4, 21):
        # Add button to current row
        row.append(InlineKeyboardButton(f"{i}", callback_data=f"overs_{i}"))
        
        # If row has 4 buttons, add it to keyboard and start new row
        if len(row) == 4:
            keyboard.append(row)
            row = []
            
    # Add any remaining buttons
    if row:
        keyboard.append(row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # --- FIX: Generate User Tag ---
    user_tag = get_user_tag(user)
    
    msg = f"🎙 <b>HOST: {user_tag}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "Host, please select the number of overs for this match.\n"
    msg += "Range: <b>4 to 20 Overs</b>"
    
    # Use Safe Refresh Function
    await refresh_game_message(context, chat.id, match, msg, reply_markup, media_key="host")


# Captain selection callback
# Captain selection callback
# Captain selection callback
async def captain_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Captain Selection and move to Toss safely"""
    query = update.callback_query
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        await query.answer("No active match.", show_alert=True)
        return
    
    match = active_matches[chat.id]
    
    # Check Phase
    if match.phase != GamePhase.CAPTAIN_SELECTION:
        await query.answer("Captain selection phase has ended.", show_alert=True)
        return
    
    # Logic for Team X
    if query.data == "captain_team_x":
        if not match.team_x.get_player(user.id):
            await query.answer("You must be in Team X!", show_alert=True)
            return
        if match.team_x.captain_id:
            await query.answer("Team X already has a captain.", show_alert=True)
            return
        match.team_x.captain_id = user.id
        await query.answer("You are Captain of Team X!")
    
    # Logic for Team Y
    elif query.data == "captain_team_y":
        if not match.team_y.get_player(user.id):
            await query.answer("You must be in Team Y!", show_alert=True)
            return
        if match.team_y.captain_id:
            await query.answer("Team Y already has a captain.", show_alert=True)
            return
        match.team_y.captain_id = user.id
        await query.answer("You are Captain of Team Y!")
    
    # Check if BOTH are selected
    if match.team_x.captain_id and match.team_y.captain_id:
        # ✅ FLOW FIX: Captains ke baad Toss aayega
        match.phase = GamePhase.TOSS
        await start_toss(query, context, match)
        
    else:
        # Update Message (Show who is selected)
        captain_x = match.team_x.get_player(match.team_x.captain_id)
        captain_y = match.team_y.get_player(match.team_y.captain_id)
        
        cap_x_name = captain_x.first_name if captain_x else "Not Selected"
        cap_y_name = captain_y.first_name if captain_y else "Not Selected"
        
        keyboard = [
            [InlineKeyboardButton("Become Captain - Team X", callback_data="captain_team_x")],
            [InlineKeyboardButton("Become Captain - Team Y", callback_data="captain_team_y")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        msg = "🧢 <b>CAPTAIN SELECTION</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🔵 <b>Team X:</b> {cap_x_name}\n"
        msg += f"🔴 <b>Team Y:</b> {cap_y_name}\n\n"
        msg += "<i>Waiting for both captains...</i>"
        
        # ✅ FIX: Use refresh_game_message instead of risky edits
        await refresh_game_message(context, chat.id, match, msg, reply_markup, media_key="squads")

async def start_team_edit_phase(query, context: ContextTypes.DEFAULT_TYPE, match: Match):
    """Start team edit phase with Safety Checks"""
    match.phase = GamePhase.TEAM_EDIT
    
    # Safe Host Fetch
    host = match.team_x.get_player(match.host_id) or match.team_y.get_player(match.host_id)
    host_name = host.first_name if host else "Unknown"
    
    # Safe Captain Fetch
    captain_x = match.team_x.get_player(match.team_x.captain_id)
    captain_y = match.team_y.get_player(match.team_y.captain_id)
    
    cap_x_name = captain_x.first_name if captain_x else "Not Selected"
    cap_y_name = captain_y.first_name if captain_y else "Not Selected"
    
    keyboard = [
        [InlineKeyboardButton("Edit Team X", callback_data="edit_team_x")],
        [InlineKeyboardButton("Edit Team Y", callback_data="edit_team_y")],
        [InlineKeyboardButton("✅ Done - Proceed", callback_data="team_edit_done")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    edit_text = "⚙️ <b>TEAM SETUP & EDITING</b>\n"
    edit_text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    edit_text += f"🎙 <b>Host:</b> {host_name}\n"
    edit_text += f"🔵 <b>Team X Captain:</b> {cap_x_name}\n"
    edit_text += f"🔴 <b>Team Y Captain:</b> {cap_y_name}\n\n"
    
    edit_text += "🔵 <b>TEAM X SQUAD:</b>\n"
    for i, player in enumerate(match.team_x.players, 1):
        role = " (C)" if player.user_id == match.team_x.captain_id else ""
        edit_text += f"{i}. {player.first_name}{role}\n"
    
    edit_text += "\n🔴 <b>TEAM Y SQUAD:</b>\n"
    for i, player in enumerate(match.team_y.players, 1):
        role = " (C)" if player.user_id == match.team_y.captain_id else ""
        edit_text += f"{i}. {player.first_name}{role}\n"
    
    edit_text += "\n"
    edit_text += "<b>Host Controls:</b>\n"
    edit_text += "• Reply to a user with <code>/add</code> to add them.\n"
    edit_text += "• Reply to a user with <code>/remove</code> to remove them.\n"
    edit_text += "• Click 'Done' when ready."
    
    # Use Master Function (Corrected Call)
    chat_id = query.message.chat.id
    await refresh_game_message(context, chat_id, match, edit_text, reply_markup=reply_markup, media_key="squads")

# Add/Remove player commands (Host only)
async def add_player_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Advanced Add Player: Reply / ID / Username / Serial
    Usage:
    - Reply: /add
    - Username: /add @username
    - ID: /add 123456789
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TEAM_EDIT:
        await update.message.reply_text("⚠️ Team editing inactive.")
        return
    
    # Check if Host
    if user.id != match.host_id:
        await update.message.reply_text("⚠️ Only Host can add.")
        return
    
    # Check if mode is set
    if not match.editing_team:
        await update.message.reply_text("⚠️ Please click 'Edit Team X' or 'Edit Team Y' button first!")
        return
    
    target_user = None
    
    # Method 1: Reply
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
    
    # Method 2: Username mention
    elif update.message.entities:
        for entity in update.message.entities:
            if entity.type == "mention":
                username = update.message.text[entity.offset:entity.offset + entity.length].replace("@", "")
                # Find in user_data
                for uid, data in user_data.items():
                    if data.get("username", "").lower() == username.lower():
                        try:
                            target_user = await context.bot.get_chat(uid)
                        except:
                            pass
                        break
            elif entity.type == "text_mention":
                target_user = entity.user
    
    # Method 3: User ID
    elif context.args:
        try:
            user_id = int(context.args[0])
            target_user = await context.bot.get_chat(user_id)
        except:
            await update.message.reply_text("⚠️ Invalid User ID.")
            return
    
    if not target_user:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b>\n"
            "Reply: <code>/add</code>\n"
            "Username: <code>/add @username</code>\n"
            "ID: <code>/add 123456789</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Check duplicate
    if match.team_x.get_player(target_user.id) or match.team_y.get_player(target_user.id):
        await update.message.reply_text(f"⚠️ {target_user.first_name} is already in a team.")
        return
    
    # Initialize if new
    if target_user.id not in user_data:
        user_data[target_user.id] = {
            "user_id": target_user.id,
            "username": target_user.username or "",
            "first_name": target_user.first_name,
            "started_at": datetime.now().isoformat(),
            "total_matches": 0
        }
        init_player_stats(target_user.id)
        save_data()
    
    # Add Player
    p = Player(target_user.id, target_user.username or "", target_user.first_name)
    
    if match.editing_team == "X":
        match.team_x.add_player(p)
        t_name = "Team X"
    else:
        match.team_y.add_player(p)
        t_name = "Team Y"
    
    target_tag = get_user_tag(target_user)
    await update.message.reply_text(f"✅ Added {target_tag} to {t_name}", parse_mode=ParseMode.HTML)
    await update_team_edit_message(context, chat.id, match)


async def remove_player_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Advanced Remove Player: Reply / ID / Username / Serial
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TEAM_EDIT:
        return
    
    if user.id != match.host_id:
        await update.message.reply_text("⚠️ Only Host can remove players.")
        return
    
    if not match.editing_team:
        await update.message.reply_text("⚠️ Pehle 'Edit Team X' ya 'Edit Team Y' button par click karein!")
        return
    
    target_user_id = None
    
    # Method 1: Reply
    if update.message.reply_to_message:
        target_user_id = update.message.reply_to_message.from_user.id
    
    # Method 2: Username
    elif update.message.entities:
        for entity in update.message.entities:
            if entity.type == "mention":
                username = update.message.text[entity.offset:entity.offset + entity.length].replace("@", "")
                for uid, data in user_data.items():
                    if data.get("username", "").lower() == username.lower():
                        target_user_id = uid
                        break
            elif entity.type == "text_mention":
                target_user_id = entity.user.id
    
    # Method 3: User ID
    elif context.args:
        try:
            target_user_id = int(context.args[0])
        except:
            # Method 4: Serial Number
            try:
                serial = int(context.args[0])
                team = match.team_x if match.editing_team == "X" else match.team_y
                target_player = team.get_player_by_serial(serial)
                if target_player:
                    target_user_id = target_player.user_id
            except:
                pass
    
    if not target_user_id:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b>\n"
            "Reply: <code>/remove</code>\n"
            "Username: <code>/remove @username</code>\n"
            "ID: <code>/remove 123456789</code>\n"
            "Serial: <code>/remove 3</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Remove Logic
    removed = False
    team_name = ""
    
    if match.editing_team == "X":
        removed = match.team_x.remove_player(target_user_id)
        team_name = "Team X"
    else:
        removed = match.team_y.remove_player(target_user_id)
        team_name = "Team Y"
    
    if removed:
        # Get name safely
        player_name = "Player"
        if target_user_id in user_data:
            player_name = user_data[target_user_id]["first_name"]
        
        await update.message.reply_text(
            f"🗑 {player_name} removed from <b>{team_name}</b>.",
            parse_mode=ParseMode.HTML
        )
        await update_team_edit_message(context, chat.id, match)
    else:
        await update.message.reply_text(f"⚠️ Player not found in {team_name}.")


async def update_team_edit_message(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Show correct menu based on state"""
    
    # Squad List Text...
    text = f"⚙️ <b>TEAM SETUP</b>\n━━━━━━━━━━━━━━━━\n"
    text += f"🔵 <b>Team X:</b> {len(match.team_x.players)} players\n"
    for p in match.team_x.players: text += f"- {p.first_name}\n"
    text += f"\n🔴 <b>Team Y:</b> {len(match.team_y.players)} players\n"
    for p in match.team_y.players: text += f"- {p.first_name}\n"
    text += "\n"

    # MAIN LOGIC: Sub-menu vs Main Menu
    if match.editing_team:
        # Edit Mode ON
        text += f"🟢 <b>EDITING TEAM {match.editing_team}</b>\n"
        text += "👉 Reply with /add or /remove.\n"
        text += "👉 Click Back when done with this team."
        
        # Sirf Back button dikhao
        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="edit_back")]]
    else:
        # Main Menu
        text += "👇 <b>Select a team to edit:</b>"
        keyboard = [
            [InlineKeyboardButton("✏️ Edit X", callback_data="edit_team_x"), 
             InlineKeyboardButton("✏️ Edit Y", callback_data="edit_team_y")],
            [InlineKeyboardButton("✅ Finalize Teams", callback_data="team_edit_done")]
        ]
    
    await refresh_game_message(context, group_id, match, text, InlineKeyboardMarkup(keyboard), "squads")

# Team edit done callback
async def team_edit_done_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Finish Team Edit and start Captain Selection"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TEAM_EDIT:
        await query.answer("Team edit phase has ended.", show_alert=True)
        return
    
    if user.id != match.host_id:
        await query.answer("Only the Host can proceed.", show_alert=True)
        return
    
    # Validate teams
    if len(match.team_x.players) == 0 or len(match.team_y.players) == 0:
        await query.answer("Both teams need at least one player.", show_alert=True)
        return
    
    # ✅ FLOW FIX: Team Edit ke baad ab Captain Selection aayega
    match.phase = GamePhase.CAPTAIN_SELECTION
    
    # Prepare Captain Selection Message
    captain_x = match.team_x.get_player(match.team_x.captain_id)
    captain_y = match.team_y.get_player(match.team_y.captain_id)
    
    cap_x_name = captain_x.first_name if captain_x else "Not Selected"
    cap_y_name = captain_y.first_name if captain_y else "Not Selected"
    
    keyboard = [
        [InlineKeyboardButton("Become Captain - Team X", callback_data="captain_team_x")],
        [InlineKeyboardButton("Become Captain - Team Y", callback_data="captain_team_y")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    msg = "🧢 <b>CAPTAIN SELECTION</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🔵 <b>Team X:</b> {cap_x_name}\n"
    msg += f"🔴 <b>Team Y:</b> {cap_y_name}\n\n"
    msg += "<i>Click below to lead your team!</i>"
    
    # Update Board (Using Refresh function to be safe)
    await refresh_game_message(context, chat.id, match, msg, reply_markup, media_key="squads")

# Over selection callback
async def over_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle over selection and move to Team Edit"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        await query.answer("No active match found.", show_alert=True)
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.OVER_SELECTION:
        await query.answer("Over selection phase has ended.", show_alert=True)
        return
    
    if user.id != match.host_id:
        await query.answer("Only the Host can select overs.", show_alert=True)
        return
    
    # --- LOGIC ---
    try:
        data_parts = query.data.split("_")
        if len(data_parts) != 2: return
        overs_selected = int(data_parts[1])
        
        if 4 <= overs_selected <= 20:
            match.total_overs = overs_selected
            
            # ✅ FLOW FIX: Overs ke baad ab Team Edit Mode aayega
            match.phase = GamePhase.TEAM_EDIT
            await start_team_edit_phase(query, context, match)
            
        else:
            await query.answer("Overs must be between 4 and 20.", show_alert=True)
    except ValueError:
        await query.answer("Invalid format.", show_alert=True)

async def start_toss(query, context: ContextTypes.DEFAULT_TYPE, match: Match):
    """Start the toss phase safely"""
    # Try to fetch Team X Captain safely
    captain_x = match.team_x.get_player(match.team_x.captain_id)
    cap_x_name = captain_x.first_name if captain_x else "Team X Captain"
    
    keyboard = [
        [InlineKeyboardButton("Heads", callback_data="toss_heads")],
        [InlineKeyboardButton("Tails", callback_data="toss_tails")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    toss_text = "🪙 <b>TIME FOR THE TOSS</b>\n"
    toss_text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    toss_text += f"📏 <b>Format:</b> {match.total_overs} Overs per side\n\n"
    toss_text += f"👤 <b>{cap_x_name}</b>, it's your call!\n"
    toss_text += "<i>Choose Heads or Tails below:</i>"
    
    # ✅ FIX: Always use refresh_game_message to switch images safely
    chat_id = match.group_id
    await refresh_game_message(context, chat_id, match, toss_text, reply_markup, media_key="toss")

# Toss callback
async def toss_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle toss selection safely"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        await query.answer("No active match found.", show_alert=True)
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TOSS:
        await query.answer("Toss phase has ended.", show_alert=True)
        return
    
    # Only Team X captain can call toss
    if user.id != match.team_x.captain_id:
        await query.answer("Only Team X Captain can call the toss.", show_alert=True)
        return
    
    # Determine toss result
    toss_result = random.choice(["heads", "tails"])
    captain_call = "heads" if query.data == "toss_heads" else "tails"
    
    if toss_result == captain_call:
        match.toss_winner = match.team_x
        winner_captain = match.team_x.get_player(match.team_x.captain_id)
    else:
        match.toss_winner = match.team_y
        winner_captain = match.team_y.get_player(match.team_y.captain_id)
    
    # Ask winner to choose bat or bowl
    keyboard = [
        [InlineKeyboardButton("🏏 Bat First", callback_data="toss_decision_bat")],
        [InlineKeyboardButton("⚾ Bowl First", callback_data="toss_decision_bowl")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    decision_text = "🪙 <b>TOSS RESULT</b>\n"
    decision_text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    decision_text += f"The coin landed on: <b>{toss_result.upper()}</b>\n\n"
    decision_text += f"🎉 <b>{match.toss_winner.name} won the toss!</b>\n"
    decision_text += f"👤 <b>Captain {winner_captain.first_name}</b>, make your choice.\n"
    decision_text += "<i>You have 30 seconds to decide.</i>"
    
    # ✅ FIX: Use refresh_game_message instead of edit_message_text
    await refresh_game_message(context, chat.id, match, decision_text, reply_markup, media_key="toss")
    
    # Set timeout for decision
    asyncio.create_task(toss_decision_timeout(context, chat.id, match))

async def toss_decision_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle toss decision timeout"""
    await asyncio.sleep(30)
    
    if match.phase != GamePhase.TOSS:
        return
    
    # Auto select bat if no decision made
    match.batting_first = match.toss_winner
    match.bowling_first = match.get_other_team(match.toss_winner)
    
    await start_match(context, group_id, match, auto_decision=True)

# Toss decision callback
async def toss_decision_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle bat/bowl decision"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        await query.answer("No active match found.", show_alert=True)
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TOSS:
        await query.answer("Toss phase has ended.", show_alert=True)
        return
    
    # Only toss winner captain can decide
    winner_captain = match.get_captain(match.toss_winner)
    if user.id != winner_captain.user_id:
        await query.answer("Only the toss winner captain can decide.", show_alert=True)
        return
    
    if query.data == "toss_decision_bat":
        match.batting_first = match.toss_winner
        match.bowling_first = match.get_other_team(match.toss_winner)
    else:
        match.bowling_first = match.toss_winner
        match.batting_first = match.get_other_team(match.toss_winner)
    
    await start_match(context, chat.id, match, auto_decision=False)

async def start_match(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, auto_decision: bool):
    """Start the actual match with prediction poll - FIXED WORKFLOW"""
    match.phase = GamePhase.MATCH_IN_PROGRESS
    match.current_batting_team = match.batting_first
    match.current_bowling_team = match.bowling_first
    match.innings = 1
    
    # ✅ CRITICAL: Reset Batsmen & Bowler indices to None (Fresh Start)
    match.current_batting_team.current_batsman_idx = None
    match.current_batting_team.current_non_striker_idx = None
    match.current_bowling_team.current_bowler_idx = None
    
    # ✅ Enable Waiting Flags
    match.waiting_for_batsman = True
    match.waiting_for_bowler = False 

    # Cleanup the Toss Board
    if match.main_message_id:
        try:
            await context.bot.unpin_chat_message(chat_id=group_id, message_id=match.main_message_id)
        except: pass

    # Send toss summary
    decision_method = "chose to" if not auto_decision else "will"
    toss_summary = "🏟 <b>MATCH STARTED</b>\n"
    toss_summary += "━━━━━━━━━━━━━━━━━━━━━\n"
    toss_summary += f"🪙 <b>{match.toss_winner.name}</b> won the toss\n"
    toss_summary += f"🏏 <b>{match.batting_first.name}</b> {decision_method} bat first\n"
    toss_summary += f"📏 <b>Format:</b> {match.total_overs} Overs per side\n"
    toss_summary += "━━━━━━━━━━━━━━━━━━━━━\n"
    toss_summary += "<i>Openers are walking to the crease...</i>"
    
    await context.bot.send_message(chat_id=group_id, text=toss_summary, parse_mode=ParseMode.HTML)
    
    # ✅ CREATE PREDICTION POLL
    await create_prediction_poll(context, group_id, match)
    
    # Wait 3 seconds for effect
    await asyncio.sleep(3)
    
    # ✅ REQUEST STRIKER ONLY (Step 1)
    captain = match.get_captain(match.current_batting_team)
    if not captain:
        await context.bot.send_message(group_id, "⚠️ No captain found!", parse_mode=ParseMode.HTML)
        return
    
    captain_tag = get_user_tag(captain)
    
    msg = f"🏏 <b>SELECT STRIKER</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"👑 <b>{captain_tag}</b>, please select the <b>STRIKER</b> first:\n\n"
    msg += f"👉 <b>Command:</b> <code>/batting [serial_number]</code>\n"
    msg += f"📋 <b>Available Players:</b> {len(match.current_batting_team.players)}\n"
    
    await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
    
    # Start selection timer
    match.batsman_selection_time = time.time()
    match.batsman_selection_task = asyncio.create_task(
        batsman_selection_timeout(context, group_id, match)
    )

async def request_batsman_selection(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match):
    """Prompt captain for new batsman after wicket"""
    captain = match.get_captain(match.current_batting_team)
    if not captain:
        await context.bot.send_message(chat_id, "⚠️ No captain selected! Use /captain to set one.")
        return

    captain_tag = get_user_tag(captain)  # Assuming get_user_tag is defined
    msg = f"🏏 <b>NEW BATSMAN NEEDED!</b>\n"
    msg += f"👑 <b>{captain_tag}</b>, select batsman:\n"
    msg += "<code>/batting &lt;serial&gt;  (e.g., /batting 3)</code>\n"
    msg += f"Available: {len(match.current_batting_team.players) - len(match.current_batting_team.out_players_indices) - 2} players left."
    
    await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
    
    # Start 30s timer for selection
    match.batsman_selection_time = time.time() + 30
    match.batsman_selection_task = asyncio.create_task(
        selection_timer(context, chat_id, match, "batsman", captain.first_name)
    )

async def selection_timer(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match, selection_type: str, name: str):
    """Timeout for batsman/bowler selection (30s)"""
    await asyncio.sleep(30)
    if (selection_type == "batsman" and match.waiting_for_batsman and 
        time.time() - match.batsman_selection_time < 0) or \
       (selection_type == "bowler" and match.waiting_for_bowler and 
        time.time() - match.bowler_selection_time < 0):
        msg = f"⏰ <b>{name}</b> timed out on {selection_type} selection! Random selected."
        await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
        await auto_select_player(context, chat_id, match, selection_type)

async def auto_select_player(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match, selection_type: str):
    """Randomly select player on timeout"""
    if selection_type == "batsman":
        team = match.current_batting_team
        available = [p for idx, p in enumerate(team.players) if idx not in team.out_players_indices and 
                     idx != team.current_batsman_idx and idx != team.current_non_striker_idx]
        if available:
            new_idx = random.choice(available).user_id  # Wait, fix: random index
            # Actually: random_idx = random.choice([i for i in range(len(team.players)) if conditions])
            random_idx = random.choice([i for i in range(len(team.players)) if i not in team.out_players_indices and 
                                        i != team.current_batsman_idx and i != team.current_non_striker_idx])
            team.current_batsman_idx = random_idx  # New striker
            match.waiting_for_batsman = False
            await resume_after_selection(context, chat_id, match)
    elif selection_type == "bowler":
        team = match.current_bowling_team
        available = team.get_available_bowlers()
        if available:
            new_bowler = random.choice(available)
            team.current_bowler_idx = team.players.index(new_bowler)
            team.bowler_history.append(team.current_bowler_idx)
            match.waiting_for_bowler = False
            await request_bowler_number(context, chat_id, match)  # Resume next ball

async def resume_after_selection(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match):
    """Resume game after batsman selection (e.g., next ball)"""
    # Cancel timer
    if match.batsman_selection_task:
        match.batsman_selection_task.cancel()
    match.waiting_for_batsman = False
    
    # If mid-over, prompt bowler for next ball
    if match.current_bowling_team.current_bowler_idx is not None:
        await request_bowler_number(context, chat_id, match)
    else:
        # Edge: Start over or something—log
        logger.warning("Resume called without bowler")

async def batting_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅ COMPLETE BATTING COMMAND - FIXED: Only select new striker after wicket
    """
    chat = update.effective_chat
    user = update.effective_user
    
    logger.info(f"🏏 /batting command from {user.first_name} (ID: {user.id})")
    
    if chat.id not in active_matches:
        logger.warning(f"⚠️ No active match in chat {chat.id}")
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.MATCH_IN_PROGRESS:
        logger.warning(f"⚠️ Match not in progress, phase={match.phase}")
        await update.message.reply_text("⚠️ Match is not in progress!")
        return

    bat_team = match.current_batting_team
    
    # Only captain can select
    if user.id != bat_team.captain_id:
        logger.warning(f"👮‍♂️ User {user.first_name} is not captain")
        await update.message.reply_text("👮‍♂️ Only the Batting Captain can select!")
        return

    if not context.args:
        await update.message.reply_text("ℹ️ <b>Usage:</b> <code>/batting [serial_number]</code>", parse_mode=ParseMode.HTML)
        return
    
    try:
        serial = int(context.args[0])
        logger.info(f"🔢 Serial number: {serial}")
    except:
        await update.message.reply_text("❌ Invalid number.")
        return

    player = bat_team.get_player_by_serial(serial)
    if not player:
        logger.warning(f"🚫 Player #{serial} not found")
        await update.message.reply_text(f"🚫 Player #{serial} not found.")
        return
    
    logger.info(f"👤 Player selected: {player.first_name} (ID: {player.user_id})")
    
    if player.is_out:
        logger.warning(f"💀 Player {player.first_name} is already OUT")
        await update.message.reply_text(f"💀 {player.first_name} is already OUT!")
        return
    
    player_idx = serial - 1
    
    # Check duplicates
    if player_idx == bat_team.current_batsman_idx or player_idx == bat_team.current_non_striker_idx:
        logger.warning(f"🛑 Player {player.first_name} already on crease")
        await update.message.reply_text(f"🛑 {player.first_name} is already on the crease!")
        return

    # ========================================
    # 🎯 CASE 1: SELECTING STRIKER (Opening)
    # ========================================
    if bat_team.current_batsman_idx is None and bat_team.current_non_striker_idx is None:
        logger.info("🎯 CASE 1: Selecting STRIKER (Opening)")
        bat_team.current_batsman_idx = player_idx
        
        await update.message.reply_text(
            f"✅ <b>Striker Selected:</b> {player.first_name}", 
            parse_mode=ParseMode.HTML
        )
        logger.info(f"✅ Striker set: {player.first_name} (Index: {player_idx})")
        await asyncio.sleep(1)

        # Request Non-Striker
        captain_tag = get_user_tag(match.get_captain(bat_team))
        msg = f"🏏 <b>SELECT NON-STRIKER</b>\n"
        msg += f"🧢 <b>{captain_tag}</b>, now select the <b>NON-STRIKER</b>:\n"
        msg += f"👉 <b>Command:</b> <code>/batting [serial_number]</code>"
        
        await context.bot.send_message(chat.id, msg, parse_mode=ParseMode.HTML)
        logger.info("✅ Non-striker request sent")
        return

    # ========================================
    # 🎯 CASE 2: SELECTING NON-STRIKER (Opening)
    # ========================================
    elif bat_team.current_non_striker_idx is None:
        logger.info("🎯 CASE 2: Selecting NON-STRIKER (Opening)")
        bat_team.current_non_striker_idx = player_idx
        
        await update.message.reply_text(f"🏃 <b>Non-Striker Selected:</b> {player.first_name}", parse_mode=ParseMode.HTML)
        logger.info(f"✅ Non-striker set: {player.first_name} (Index: {player_idx})")
        await asyncio.sleep(1)
        
        # ✅ Opening Pair Complete
        match.waiting_for_batsman = False
        if match.batsman_selection_task:
            match.batsman_selection_task.cancel()
            logger.info("✅ Batsman selection timer cancelled")
        
        striker = bat_team.players[bat_team.current_batsman_idx]
        non_striker = bat_team.players[bat_team.current_non_striker_idx]
        
        confirm_msg = f"✅ <b>OPENING PAIR SET!</b>\n"
        confirm_msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
        confirm_msg += f"🏏 <b>Striker:</b> {striker.first_name}\n"
        confirm_msg += f"🏃 <b>Non-Striker:</b> {non_striker.first_name}\n\n"
        confirm_msg += f"⚾ <i>Requesting bowler selection...</i>"
        
        await context.bot.send_message(chat.id, confirm_msg, parse_mode=ParseMode.HTML)
        logger.info("✅ Opening pair confirmed")
        await asyncio.sleep(2)
        
        # ✅ Request Bowler (Step 3)
        match.waiting_for_bowler = True
        logger.info("📣 Calling request_bowler_selection...")
        await request_bowler_selection(context, chat.id, match)
        logger.info("✅ Bowler selection initiated")
        return

    # ========================================
    # 🎯 CASE 3: NEW BATSMAN (After Wicket) - FIXED
    # ========================================
    else:
        logger.info("🎯 CASE 3: Selecting NEW BATSMAN after wicket")
        
        if not match.waiting_for_batsman:
            logger.warning("⚠️ Not waiting for batsman")
            await update.message.reply_text("⚠️ Batsmen are already set. Use /impact for substitution.")
            return

        # ✅ SET NEW STRIKER (NON-STRIKER STAYS SAME)
        bat_team.current_batsman_idx = player_idx
        match.waiting_for_batsman = False
        
        if match.batsman_selection_task:
            match.batsman_selection_task.cancel()
            logger.info("✅ Batsman selection timer cancelled")
    
        player_tag = get_user_tag(player)
        await update.message.reply_text(
            f"🚶‍♂️ <b>NEW BATSMAN:</b> {player_tag} walks in!", 
            parse_mode=ParseMode.HTML
        )
        logger.info(f"✅ New batsman set: {player.first_name} (Index: {player_idx})")
    
        await asyncio.sleep(2)
        
        # ✅ CRITICAL: RESUME GAME LOGIC
        bowl_team = match.current_bowling_team
        current_over_balls = bowl_team.get_current_over_balls()
        
        logger.info(f"📊 Current over balls: {current_over_balls}, Total balls: {bowl_team.balls}")
    
        # ✅ FIX: Check if wicket was last ball of over
        if current_over_balls == 0 and bowl_team.balls > 0:
            logger.info("🏁 Wicket was last ball of over, calling check_over_complete")
            await check_over_complete(context, chat.id, match)
        
        # ✅ If bowler exists and over not complete, resume
        elif bowl_team.current_bowler_idx is not None:
            logger.info("▶️ Bowler exists, resuming ball execution")
            await context.bot.send_message(chat.id, "▶️ <b>Game Resumed!</b>", parse_mode=ParseMode.HTML)
            await asyncio.sleep(1)
            await execute_ball(context, chat.id, match)
            
        # ✅ Edge case: No bowler (shouldn't happen)
        else:
            logger.warning("⚠️ No bowler found, requesting bowler selection")
            match.waiting_for_bowler = True
            await request_bowler_selection(context, chat.id, match)
        
        return

async def players_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current squads with Image"""
    chat = update.effective_chat
    if chat.id not in active_matches:
        await update.message.reply_text("No active match.")
        return
        
    match = active_matches[chat.id]
    
    msg = "📋 <b>OFFICIAL MATCH SQUADS</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    msg += f"🔵 <b>{match.team_x.name}</b>\n"
    for i, p in enumerate(match.team_x.players, 1):
        role = " ⭐ (C)" if p.user_id == match.team_x.captain_id else ""
        msg += f"<code>{i}.</code> <b>{p.first_name}</b>{role}\n"
        
    msg += f"\n🔴 <b>{match.team_y.name}</b>\n"
    for i, p in enumerate(match.team_y.players, 1):
        role = " ⭐ (C)" if p.user_id == match.team_y.captain_id else ""
        msg += f"<code>{i}.</code> <b>{p.first_name}</b>{role}\n"
        
    await update.message.reply_photo(
        photo=MEDIA_ASSETS["squads"],
        caption=msg,
        parse_mode=ParseMode.HTML
    )

async def batsman_selection_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle batsman selection timeout"""
    try:
        await asyncio.sleep(120)  # 2 minutes
        
        if not match.waiting_for_batsman:
            return
        
        # Timeout occurred - penalty
        match.current_batting_team.score -= 6
        match.current_batting_team.penalty_runs += 6
        
        penalty_msg = "Batsman Selection Timeout\n\n"
        penalty_msg += f"{match.current_batting_team.name} penalized 6 runs for delay.\n"
        penalty_msg += f"Current Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}\n\n"
        penalty_msg += "Please select a batsman immediately."
        
        await context.bot.send_message(
            chat_id=group_id,
            text=penalty_msg
        )
        
        # Reset timer
        match.batsman_selection_time = time.time()
        match.batsman_selection_task = asyncio.create_task(
            batsman_selection_timeout(context, group_id, match)
        )
    
    except asyncio.CancelledError:
        pass

async def request_bowler_selection(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match):
    """Prompt captain for bowler - GUARANTEED DELIVERY WITH FULL LOGGING"""
    
    logger.info(f"🎬 === BOWLER SELECTION START === Chat: {chat_id}")
    
    # Validate bowling team exists
    if not match.current_bowling_team:
        logger.error("❌ CRITICAL: No bowling team set!")
        await context.bot.send_message(chat_id, "⚠️ Error: No bowling team found!", parse_mode=ParseMode.HTML)
        return
    
    logger.info(f"✅ Bowling team: {match.current_bowling_team.name}")
    
    # Get captain
    captain = match.get_captain(match.current_bowling_team)
    if not captain:
        logger.error("❌ CRITICAL: No bowling captain found!")
        await context.bot.send_message(
            chat_id, 
            "⚠️ <b>Error:</b> No bowling captain found! Use /changecap_Y to set one.", 
            parse_mode=ParseMode.HTML
        )
        return

    captain_tag = get_user_tag(captain)
    logger.info(f"👑 Captain: {captain.first_name} (ID: {captain.user_id})")
    
    # Get available bowlers
    available = match.current_bowling_team.get_available_bowlers()
    logger.info(f"📊 Available bowlers: {len(available)}")
    
    if len(available) == 0:
        logger.error("❌ CRITICAL: No available bowlers!")
        await context.bot.send_message(
            chat_id,
            "⚠️ <b>Error:</b> No bowlers available! All players may be banned from bowling.",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Log available bowlers
    for i, p in enumerate(available):
        logger.info(f"  Bowler #{i+1}: {p.first_name} (ID: {p.user_id})")
    
    # Build message
    msg = f"⚾ <b>SELECT BOWLER</b>\n"
    msg += f"─────────────────────\n"
    msg += f"🎩 <b>{captain_tag}</b>, choose your bowler:\n\n"
    msg += f"💡 <b>Command:</b> <code>/bowling [serial]</code>\n"
    msg += f"📋 <b>Available:</b> {len(available)} players\n\n"
    msg += f"<i>Example: /bowling 1</i>"
    
    # Send message to group
    try:
        sent_msg = await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
        logger.info(f"✅ Group message sent successfully (msg_id: {sent_msg.message_id})")
    except Exception as e:
        logger.error(f"❌ Failed to send group message: {e}")
        return
    
    # SET WAITING STATE BEFORE ANYTHING ELSE
    match.waiting_for_bowler = True
    match.waiting_for_batsman = False
    match.current_ball_data = {}
    logger.info(f"✅ State set: waiting_for_bowler=True, waiting_for_batsman=False")
    
    # Start timeout timer
    match.bowler_selection_time = time.time()
    match.bowler_selection_task = asyncio.create_task(
        bowler_selection_timeout(context, chat_id, match)
    )
    logger.info(f"✅ Timeout timer started (120 seconds)")
    
    logger.info(f"🎬 === BOWLER SELECTION END ===")

async def bowler_selection_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle bowler selection timeout"""
    try:
        await asyncio.sleep(60)  # 1 minute
        
        if not match.waiting_for_bowler:
            return
        
        # Get current bowler if any
        if match.current_bowling_team.current_bowler_idx is not None:
            bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
            bowler.bowling_timeouts += 1
            
            timeout_count = bowler.bowling_timeouts
            
            if timeout_count >= 3:
                # Ban from bowling
                bowler.is_bowling_banned = True
                
                penalty_msg = "Bowler Selection Timeout\n\n"
                penalty_msg += f"{bowler.first_name} has timed out 3 times.\n"
                penalty_msg += f"{bowler.first_name} is now BANNED from bowling for the rest of the match.\n\n"
                penalty_msg += "No Ball called. Free Hit on next ball.\n\n"
                penalty_msg += "Please select another bowler immediately."
                
                # Add no ball
                match.current_batting_team.score += 1
                match.current_batting_team.extras += 1
                match.is_free_hit = True
                
                await context.bot.send_message(
                    chat_id=group_id,
                    text=penalty_msg
                )
            else:
                penalty_msg = "Bowler Selection Timeout\n\n"
                penalty_msg += f"{bowler.first_name} timed out ({timeout_count}/3).\n"
                penalty_msg += "No Ball called. Free Hit on next ball.\n\n"
                penalty_msg += "Please select a bowler immediately."
                
                # Add no ball
                match.current_batting_team.score += 1
                match.current_batting_team.extras += 1
                match.is_free_hit = True
                
                await context.bot.send_message(
                    chat_id=group_id,
                    text=penalty_msg
                )
        else:
            # First ball, no specific bowler to penalize
            penalty_msg = "Bowler Selection Timeout\n\n"
            penalty_msg += f"{match.current_bowling_team.name} delayed bowler selection.\n"
            penalty_msg += "6 runs penalty after this over.\n\n"
            penalty_msg += "Please select a bowler immediately."
            
            await context.bot.send_message(
                chat_id=group_id,
                text=penalty_msg
            )
        
        # Reset timer
        match.bowler_selection_time = time.time()
        match.bowler_selection_task = asyncio.create_task(
            bowler_selection_timeout(context, group_id, match)
        )
    
    except asyncio.CancelledError:
        pass

# Bowling command
async def bowling_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Bowling Selection - FULL LOGGING VERSION"""
    chat = update.effective_chat
    user = update.effective_user
    
    logger.info(f"⚾ === BOWLING COMMAND START === From: {user.first_name} (ID: {user.id})")
    
    if chat.id not in active_matches: 
        logger.warning(f"❌ No active match in chat {chat.id}")
        await update.message.reply_text("❌ No active match")
        return
    
    match = active_matches[chat.id]
    logger.info(f"✅ Match found - Phase: {match.phase}")
    
    # Check if we are waiting for bowler
    if not match.waiting_for_bowler:
        logger.warning(f"⚠️ Not waiting for bowler (waiting_for_bowler=False)")
        await update.message.reply_text("⚠️ Not waiting for bowler selection right now.")
        return
    
    logger.info(f"✅ Waiting for bowler confirmed")
    
    bowling_captain = match.get_captain(match.current_bowling_team)
    if not bowling_captain:
        logger.error(f"❌ No bowling captain found!")
        await update.message.reply_text("⚠️ No bowling captain found!")
        return
    
    logger.info(f"👑 Bowling captain: {bowling_captain.first_name} (ID: {bowling_captain.user_id})")
        
    if user.id != bowling_captain.user_id:
        logger.warning(f"🚫 User {user.first_name} is not the bowling captain")
        await update.message.reply_text("⚠️ Only the Bowling Captain can select.")
        return
    
    logger.info(f"✅ Captain verification passed")
    
    if not context.args:
        logger.warning(f"⚠️ No serial number provided")
        await update.message.reply_text("⚠️ Usage: <code>/bowling [serial]</code>", parse_mode=ParseMode.HTML)
        return
    
    try:
        serial = int(context.args[0])
        logger.info(f"🔢 Serial number: {serial}")
    except: 
        logger.error(f"❌ Invalid serial number: {context.args[0]}")
        await update.message.reply_text("❌ Invalid number.")
        return
    
    bowler = match.current_bowling_team.get_player_by_serial(serial)
    if not bowler:
        logger.error(f"❌ Player #{serial} not found in bowling team")
        await update.message.reply_text(f"❌ Player #{serial} not found.")
        return
    
    logger.info(f"👤 Bowler selected: {bowler.first_name} (ID: {bowler.user_id})")
        
    if bowler.is_bowling_banned:
        logger.warning(f"🚫 {bowler.first_name} is banned from bowling")
        await update.message.reply_text("🚫 Player is BANNED from bowling.")
        return
    
    logger.info(f"✅ Bowler validation passed")
    
    # ✅ NO CONSECUTIVE OVERS CHECK
    bowler_idx = serial - 1
    if match.current_bowling_team.bowler_history:
        if bowler_idx == match.current_bowling_team.bowler_history[-1]:
            logger.warning(f"🚫 Consecutive over attempt by {bowler.first_name}")
            await update.message.reply_text("🚫 <b>Rule:</b> Bowler cannot bowl 2 consecutive overs!", parse_mode=ParseMode.HTML)
            return
    
    logger.info(f"✅ Consecutive over check passed")

    # ✅ SET BOWLER
    match.current_bowling_team.current_bowler_idx = bowler_idx
    match.waiting_for_bowler = False
    match.waiting_for_batsman = False 
    
    logger.info(f"✅ Bowler set: Index={bowler_idx}, waiting_for_bowler=False")
    
    if match.bowler_selection_task: 
        match.bowler_selection_task.cancel()
        logger.info(f"✅ Bowler selection timer cancelled")
    
    # Confirmation
    try: 
        bowler_tag = get_user_tag(bowler)
    except: 
        bowler_tag = bowler.first_name
    
    confirm_msg = f"✅ <b>BOWLER SELECTED</b>\n"
    confirm_msg += f"─────────────────────\n"
    confirm_msg += f"⚾ <b>{bowler_tag}</b> will bowl!\n\n"
    confirm_msg += f"▶️ <i>Game Resumed! Starting the over...</i>"
    
    await update.message.reply_text(confirm_msg, parse_mode=ParseMode.HTML)
    logger.info(f"✅ Confirmation message sent")
    
    await asyncio.sleep(2)
    
    # ✅ RESUME GAME (Trigger first ball of over)
    logger.info(f"🎮 Calling execute_ball to start bowling...")
    await execute_ball(context, chat.id, match)
    logger.info(f"⚾ === BOWLING COMMAND END ===")


# ✅ FIX 3: Enhanced execute_ball with logging
async def execute_ball(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    Premium TV Broadcast Style - WITH FULL LOGGING
    """
    
    logger.info(f"🎮 === EXECUTE_BALL START === Group: {group_id}")
    
    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    # ✅ SAFETY CHECK: Verify indices exist
    if bat_team.current_batsman_idx is None:
        logger.error("❌ CRITICAL: No striker selected!")
        await context.bot.send_message(group_id, "⚠️ Error: No striker found!")
        return
    
    if bat_team.current_non_striker_idx is None:
        logger.error("❌ CRITICAL: No non-striker selected!")
        await context.bot.send_message(group_id, "⚠️ Error: No non-striker found!")
        return
    
    if bowl_team.current_bowler_idx is None:
        logger.error("❌ CRITICAL: No bowler selected!")
        await context.bot.send_message(group_id, "⚠️ Error: No bowler found!")
        return
    
    striker = bat_team.players[bat_team.current_batsman_idx]
    non_striker = bat_team.players[bat_team.current_non_striker_idx]
    bowler = bowl_team.players[bowl_team.current_bowler_idx]
    
    logger.info(f"✅ Players verified:")
    logger.info(f"  🏏 Striker: {striker.first_name} (Index: {bat_team.current_batsman_idx})")
    logger.info(f"  🏃 Non-Striker: {non_striker.first_name} (Index: {bat_team.current_non_striker_idx})")
    logger.info(f"  ⚾ Bowler: {bowler.first_name} (Index: {bowl_team.current_bowler_idx})")
    
    # Clickable Names
    striker_tag = f"<a href='tg://user?id={striker.user_id}'>{striker.first_name}</a>"
    bowler_tag = f"<a href='tg://user?id={bowler.user_id}'>{bowler.first_name}</a>"

    # --- 🧮 CALCULATE STATS ---
    total_overs_bowled = max(bowl_team.overs, 0.1)
    crr = round(bat_team.score / total_overs_bowled, 2)
    
    # Match Equation (For 2nd Innings)
    equation = ""
    if match.innings == 2:
        runs_needed = match.target - bat_team.score
        balls_left = (match.total_overs * 6) - bat_team.balls
        rrr = round((runs_needed / balls_left) * 6, 2) if balls_left > 0 else 0
        equation = f"\n🎯 <b>Target:</b> Need <b>{runs_needed}</b> off <b>{balls_left}</b> balls (RRR: {rrr})"

    # --- 🏟️ GROUP DISPLAY ---
    text = f"🔴 <b>LIVE</b>\n"
    text += "─────────────────────\n"
    text += f"🏏<b>Batsman:</b> <b>{striker_tag}</b>\n"
    text += f"⚾<b>Bowler:</b> <b>{bowler_tag}</b>\n"
    text += f"<i>(Non-Strike: {non_striker.first_name})</i>\n"
    text += "─────────────────────\n"
    text += f"📊 <b>{bat_team.score}/{bat_team.wickets}</b>  ({format_overs(bowl_team.balls)})  |\n"
    
    if equation:
        text += f"{equation}\n"
    text += "─────────────────────\n"
    
    if match.is_free_hit:
        text += "🚨 <b>FREE HIT DELIVERY!</b> 🚨\n\n"
        
    text += f"📣 <b>{bowler.first_name}</b> is running in..."

    # Button
    keyboard = [[InlineKeyboardButton("📩 Tap to Bowl", url=f"https://t.me/{context.bot.username}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # GIF
    ball_gif = "https://t.me/kyanaamrkhe/6"
    
    try:
        await context.bot.send_animation(
            group_id, 
            animation=ball_gif,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
        logger.info("✅ Ball animation sent to group")
    except Exception as e:
        logger.error(f"❌ Failed to send animation: {e}")
        await context.bot.send_message(group_id, text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    
    # --- 📩 DM TO BOWLER ---
    dm_text = f"🏟️ <b>NEXT DELIVERY</b>\n"
    dm_text += "────────────────\n"
    dm_text += f"🏏 <b>Batsman:</b> {striker.first_name}\n"
    dm_text += f"📊 <b>Score:</b> {bat_team.score}/{bat_team.wickets}\n"
    
    if match.innings == 2:
        runs_needed = match.target - bat_team.score
        balls_left = (match.total_overs * 6) - bat_team.balls
        dm_text += f"🎯 <b>Defend:</b> {runs_needed} runs / {balls_left} balls\n"
    
    dm_text += "\n👉 <b>Send Number (0-6)</b>\n"
    dm_text += "<i>Time: 45s</i>"
    
    try:
        await context.bot.send_message(bowler.user_id, dm_text, parse_mode=ParseMode.HTML)
        logger.info(f"✅ DM sent to bowler {bowler.first_name} (ID: {bowler.user_id})")
        
        match.current_ball_data = {
            "bowler_id": bowler.user_id, 
            "bowler_number": None, 
            "batsman_number": None,
            "group_id": group_id
        }
        
        logger.info(f"✅ Ball data initialized: {match.current_ball_data}")
        
        if match.ball_timeout_task:
            match.ball_timeout_task.cancel()
        match.ball_timeout_task = asyncio.create_task(
            game_timer(context, group_id, match, "bowler", bowler.first_name)
        )
        
        logger.info(f"✅ Game timer started for bowler")
        
    except Forbidden:
        logger.warning(f"⚠️ Cannot DM bowler {bowler.first_name} - User hasn't started bot")
        await context.bot.send_message(
            group_id, 
            f"⚠️ <b>Start Bot:</b> {bowler_tag} please check your DMs and start the bot!", 
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"❌ DM error: {e}")
    
    logger.info(f"🎮 === EXECUTE_BALL END ===")

async def wait_for_bowler_number(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Wait for bowler to send number with reminders"""
    bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
    
    try:
        # Wait 30 seconds
        await asyncio.sleep(30)
        
        if match.current_ball_data.get("bowler_number") is None:
            # Send reminder at 30s
            try:
                await context.bot.send_message(
                    chat_id=bowler.user_id,
                    text="Reminder: Please send your number (0-6).\n30 seconds remaining."
                )
                match.current_ball_data["bowler_reminded"] = True
            except Exception as e:
                logger.error(f"Error sending reminder to bowler: {e}")
        
        # Wait 15 more seconds
        await asyncio.sleep(15)
        
        if match.current_ball_data.get("bowler_number") is None:
            # Send reminder at 15s
            try:
                await context.bot.send_message(
                    chat_id=bowler.user_id,
                    text="Urgent: Send your number now!\n15 seconds remaining."
                )
            except Exception as e:
                logger.error(f"Error sending reminder to bowler: {e}")
        
        # Wait 10 more seconds
        await asyncio.sleep(10)
        
        if match.current_ball_data.get("bowler_number") is None:
            # Send reminder at 5s
            try:
                await context.bot.send_message(
                    chat_id=bowler.user_id,
                    text="Final warning: 5 seconds left!"
                )
            except Exception as e:
                logger.error(f"Error sending reminder to bowler: {e}")
        
        # Wait final 5 seconds
        await asyncio.sleep(5)
        
        if match.current_ball_data.get("bowler_number") is None:
            # Timeout - handle penalty
            await handle_bowler_timeout(context, group_id, match)
    
    except asyncio.CancelledError:
        pass

async def handle_bowler_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle bowler timeout penalty"""
    bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
    bowler.bowling_timeouts += 1
    bowler.no_balls += 1
    
    timeout_count = bowler.bowling_timeouts
    
    # Add no ball
    match.current_batting_team.score += 1
    match.current_batting_team.extras += 1
    match.is_free_hit = True
    
    gif_url = get_random_gif(MatchEvent.NO_BALL)
    commentary = get_random_commentary("noball")
    
    if timeout_count >= 3:
        # Ban from bowling
        bowler.is_bowling_banned = True
        
        penalty_text = f"Over {format_overs(match.current_bowling_team.balls)}\n\n"
        penalty_text += f"Bowler Timeout - {bowler.first_name}\n\n"
        penalty_text += f"{bowler.first_name} has timed out 3 times.\n"
        penalty_text += f"{bowler.first_name} is now BANNED from bowling.\n\n"
        penalty_text += "NO BALL\n"
        penalty_text += "Free Hit on next ball\n\n"
        penalty_text += f"{commentary}\n\n"
        penalty_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}"
    else:
        penalty_text = f"Over {format_overs(match.current_bowling_team.balls)}\n\n"
        penalty_text += f"Bowler Timeout - {bowler.first_name} ({timeout_count}/3)\n\n"
        penalty_text += "NO BALL\n"
        penalty_text += "Free Hit on next ball\n\n"
        penalty_text += f"{commentary}\n\n"
        penalty_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}"
    
    try:
        if gif_url:
            await context.bot.send_animation(
                chat_id=group_id,
                animation=gif_url,
                caption=penalty_text
            )
        else:
            await context.bot.send_message(
                chat_id=group_id,
                text=penalty_text
            )
    except Exception as e:
        logger.error(f"Error sending timeout message: {e}")
        await context.bot.send_message(
            chat_id=group_id,
            text=penalty_text
        )
    
    # Continue with next ball
    await asyncio.sleep(2)
    
    if bowler.is_bowling_banned:
        # Need new bowler
        match.waiting_for_bowler = True
        await request_bowler_selection(context, group_id, match)
    else:
        # Same bowler continues
        await execute_ball(context, group_id, match)

# Handle DM messages from players

async def bannedgroups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Show list of all banned groups (Owner Only)
    """
    user = update.effective_user
    
    if user.id != OWNER_ID:
        return
    
    if not banned_groups:
        await update.message.reply_text(
            "✅ <b>NO BANNED GROUPS</b>\n\n"
            "Currently, no groups are banned from using the bot.",
            parse_mode=ParseMode.HTML
        )
        return
    
    msg = f"🚫 <b>BANNED GROUPS LIST</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 <b>Total:</b> {len(banned_groups)} groups\n\n"
    
    for i, chat_id in enumerate(banned_groups, 1):
        # Try to get group name
        try:
            chat_info = await context.bot.get_chat(chat_id)
            group_name = chat_info.title
        except:
            group_name = "Unknown/Left Group"
        
        msg += f"{i}. <b>{group_name}</b>\n"
        msg += f"   🆔 <code>{chat_id}</code>\n\n"
        
        # Telegram message limit protection
        if len(msg) > 3500:
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
            msg = "<b>...continued</b>\n\n"
    
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += "<i>Use /unbangroup [id] to unban</i>"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def unbangroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Unban a group (Owner Only)
    Usage: 
      - In group: /unbangroup
      - Via DM: /unbangroup -1001234567890
    """
    user = update.effective_user
    
    # Owner check
    if user.id != OWNER_ID:
        return
    
    chat = update.effective_chat
    
    # Method 1: Command used in the group itself
    if chat.type in ["group", "supergroup"]:
        target_chat_id = chat.id
        target_chat_name = chat.title
    
    # Method 2: Command used in DM with group ID
    elif context.args:
        try:
            target_chat_id = int(context.args[0])
            
            # Try to get group info
            try:
                chat_info = await context.bot.get_chat(target_chat_id)
                target_chat_name = chat_info.title
            except:
                target_chat_name = "Unknown Group"
        except ValueError:
            await update.message.reply_text(
                "⚠️ <b>Invalid Group ID</b>\n\n"
                "<b>Usage:</b>\n"
                "In Group: <code>/unbangroup</code>\n"
                "In DM: <code>/unbangroup -1001234567890</code>",
                parse_mode=ParseMode.HTML
            )
            return
    else:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b>\n"
            "In Group: <code>/unbangroup</code>\n"
            "In DM: <code>/unbangroup [group_id]</code>\n\n"
            "<b>Example:</b> <code>/unbangroup -1001234567890</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Check if actually banned
    if target_chat_id not in banned_groups:
        await update.message.reply_text(
            f"⚠️ <b>{target_chat_name}</b> (<code>{target_chat_id}</code>) is not banned!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # ✅ UNBAN THE GROUP
    banned_groups.discard(target_chat_id)
    save_data()
    
    # Notify the group
    await notify_ban_status(context, target_chat_id, is_ban=False)
    
    # Confirm to owner
    await update.message.reply_text(
        f"✅ <b>GROUP UNBANNED</b>\n\n"
        f"📛 <b>Name:</b> {target_chat_name}\n"
        f"🆔 <b>ID:</b> <code>{target_chat_id}</code>\n\n"
        f"✅ This group can now use the bot again.",
        parse_mode=ParseMode.HTML
    )
    
    # Log to support group
    try:
        await context.bot.send_message(
            chat_id=SUPPORT_GROUP_ID,
            text=(
                f"✅ <b>GROUP UNBANNED</b>\n"
                f"📛 {target_chat_name}\n"
                f"🆔 <code>{target_chat_id}</code>\n"
                f"👤 By: {user.first_name}"
            ),
            parse_mode=ParseMode.HTML
        )
    except:
        pass

# ==================== GROUP BAN MIDDLEWARE ====================

async def check_group_ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Check if group is banned. Returns True if allowed, False if banned.
    Call this at the start of every group command.
    """
    chat = update.effective_chat
    
    # Allow private chats and owner commands
    if chat.type == "private":
        return True
    
    if chat.id in banned_groups:
        # Silently ignore (bot won't respond in banned groups)
        logger.info(f"Blocked command in banned group: {chat.id}")
        return False
    
    return True

async def notify_ban_status(context: ContextTypes.DEFAULT_TYPE, chat_id: int, is_ban: bool):
    """Send notification to group about ban/unban"""
    if is_ban:
        msg = (
            "🚫 <b>GROUP BANNED</b> 🚫\n"
            "━━━━━━━━━━━━━━━━━\n"
            "This group has been <b>banned</b> from using this bot.\n\n"
            "🔒 All commands are now disabled.\n"
            "📧 Contact bot owner for more information."
        )
    else:
        msg = (
            "✅ <b>GROUP UNBANNED</b> ✅\n"
            "━━━━━━━━━━━━━━━━━\n"
            "This group can now use the bot again!\n\n"
            "🎮 Use /game to start playing."
        )
    
    try:
        await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to notify group {chat_id}: {e}")

async def bangroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Ban a group from using the bot (Owner Only)
    Usage: 
      - In group: /bangroup
      - Via DM: /bangroup -1001234567890
    """
    user = update.effective_user
    
    # Owner check
    if user.id != OWNER_ID:
        return
    
    chat = update.effective_chat
    
    # Method 1: Command used in the group itself
    if chat.type in ["group", "supergroup"]:
        target_chat_id = chat.id
        target_chat_name = chat.title
    
    # Method 2: Command used in DM with group ID as argument
    elif context.args:
        try:
            target_chat_id = int(context.args[0])
            
            # Try to get group info
            try:
                chat_info = await context.bot.get_chat(target_chat_id)
                target_chat_name = chat_info.title
            except:
                target_chat_name = "Unknown Group"
        except ValueError:
            await update.message.reply_text(
                "⚠️ <b>Invalid Group ID</b>\n\n"
                "<b>Usage:</b>\n"
                "In Group: <code>/bangroup</code>\n"
                "In DM: <code>/bangroup -1001234567890</code>",
                parse_mode=ParseMode.HTML
            )
            return
    else:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b>\n"
            "In Group: <code>/bangroup</code>\n"
            "In DM: <code>/bangroup [group_id]</code>\n\n"
            "<b>Example:</b> <code>/bangroup -1001234567890</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Check if already banned
    if target_chat_id in banned_groups:
        await update.message.reply_text(
            f"⚠️ <b>{target_chat_name}</b> (<code>{target_chat_id}</code>) is already banned!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # ✅ BAN THE GROUP
    banned_groups.add(target_chat_id)
    save_data()
    
    # End any active match in that group
    if target_chat_id in active_matches:
        match = active_matches[target_chat_id]
        match.phase = GamePhase.MATCH_ENDED
        
        # Cancel all tasks
        if match.ball_timeout_task: match.ball_timeout_task.cancel()
        if match.batsman_selection_task: match.batsman_selection_task.cancel()
        if match.bowler_selection_task: match.bowler_selection_task.cancel()
        if hasattr(match, 'join_phase_task') and match.join_phase_task: 
            match.join_phase_task.cancel()
        
        del active_matches[target_chat_id]
    
    # Notify the group
    await notify_ban_status(context, target_chat_id, is_ban=True)
    
    # Confirm to owner
    await update.message.reply_text(
        f"✅ <b>GROUP BANNED</b>\n\n"
        f"📛 <b>Name:</b> {target_chat_name}\n"
        f"🆔 <b>ID:</b> <code>{target_chat_id}</code>\n\n"
        f"🚫 This group can no longer use the bot.For unban contact @ASTRO_SHUBH",
        parse_mode=ParseMode.HTML
    )
    
    # Log to support group
    try:
        await context.bot.send_message(
            chat_id=SUPPORT_GROUP_ID,
            text=(
                f"🚫 <b>GROUP BANNED</b>\n"
                f"📛 {target_chat_name}\n"
                f"🆔 <code>{target_chat_id}</code>\n"
                f"👤 By: {user.first_name}"
            ),
            parse_mode=ParseMode.HTML
        )
    except:
        pass



async def process_player_number(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, number: int):
    """Process number sent by player"""
    user = update.effective_user
    
    batsman = match.current_batting_team.players[match.current_batting_team.current_batsman_idx]
    bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
    
    # Check if bowler sent number
    if user.id == bowler.user_id and match.current_ball_data.get("bowler_number") is None:
        match.current_ball_data["bowler_number"] = number
        await update.message.reply_text(f"Your number: {number}\nWaiting for batsman...")
        
        # Cancel bowler timeout task
        if match.ball_timeout_task:
            match.ball_timeout_task.cancel()
        
        # Now request batsman number
        await request_batsman_number(context, group_id, match)
        return
    
    # Check if batsman sent number
    if user.id == batsman.user_id and match.current_ball_data.get("batsman_number") is None:
        if match.current_ball_data.get("bowler_number") is None:
            await update.message.reply_text("Please wait for bowler to send their number first.")
            return
        
        match.current_ball_data["batsman_number"] = number
        await update.message.reply_text(f"Your number: {number}\nProcessing ball...")
        
        # Cancel batsman timeout task
        if match.ball_timeout_task:
            match.ball_timeout_task.cancel()
        
        # Process ball result
        await process_ball_result(context, group_id, match)
        return

async def request_batsman_number(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Request batsman number with GIF - FIXED"""
    batsman = match.current_batting_team.players[match.current_batting_team.current_batsman_idx]
    
    batsman_tag = f"<a href='tg://user?id={batsman.user_id}'>{batsman.first_name}</a>"
    
    text = f"⚾ <b>Bowler has bowled!</b>\n"
    text += f"🏏 <b>{batsman_tag}</b>, it's your turn!\n"
    text += "👉 <b>Send your number (0-6) in this group!</b>\n"
    text += "⏳ <i>You have 45 seconds!</i>"
    
    # ✅ FIX: Add GIF
    batting_gif = "https://t.me/kyanaamrkhe/7"  # Cricket batting GIF
    
    try:
        await context.bot.send_animation(
            group_id,
            animation=batting_gif,
            caption=text,
            parse_mode=ParseMode.HTML
        )
    except:
        await context.bot.send_message(group_id, text, parse_mode=ParseMode.HTML)
    
    if match.ball_timeout_task:
        match.ball_timeout_task.cancel()
    match.ball_timeout_task = asyncio.create_task(
        game_timer(context, group_id, match, "batsman", batsman.first_name)
    )

async def wait_for_batsman_number(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Wait for batsman to send number with reminders"""
    batsman = match.current_batting_team.players[match.current_batting_team.current_batsman_idx]
    
    try:
        # Wait 30 seconds
        await asyncio.sleep(30)
        
        if match.current_ball_data.get("batsman_number") is None:
            # Send reminder at 30s
            try:
                await context.bot.send_message(
                    chat_id=batsman.user_id,
                    text="Reminder: Please send your number (0-6).\n30 seconds remaining."
                )
            except Exception as e:
                logger.error(f"Error sending reminder to batsman: {e}")
        
        # Wait 15 more seconds
        await asyncio.sleep(15)
        
        if match.current_ball_data.get("batsman_number") is None:
            # Send reminder at 15s
            try:
                await context.bot.send_message(
                    chat_id=batsman.user_id,
                    text="Urgent: Send your number now!\n15 seconds remaining."
                )
            except Exception as e:
                logger.error(f"Error sending reminder to batsman: {e}")
        
        # Wait 10 more seconds
        await asyncio.sleep(10)
        
        if match.current_ball_data.get("batsman_number") is None:
            # Send reminder at 5s
            try:
                await context.bot.send_message(
                    chat_id=batsman.user_id,
                    text="Final warning: 5 seconds left!"
                )
            except Exception as e:
                logger.error(f"Error sending reminder to batsman: {e}")
        
        # Wait final 5 seconds
        await asyncio.sleep(5)
        
        if match.current_ball_data.get("batsman_number") is None:
            # Timeout - handle penalty
            await handle_batsman_timeout(context, group_id, match)
    
    except asyncio.CancelledError:
        pass

async def handle_batsman_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle batsman timeout penalty"""
    batsman = match.current_batting_team.players[match.current_batting_team.current_batsman_idx]
    batsman.batting_timeouts += 1
    
    timeout_count = batsman.batting_timeouts
    
    # Penalty: -6 runs
    match.current_batting_team.score -= 6
    match.current_batting_team.penalty_runs += 6
    
    if timeout_count >= 3:
        # Auto out - Hit Wicket
        batsman.is_out = True
        batsman.dismissal_type = "Hit Wicket (Timeout)"
        match.current_batting_team.wickets += 1
        
        gif_url = get_random_gif(MatchEvent.WICKET)
        
        penalty_text = f"Over {format_overs(match.current_bowling_team.balls)}\n\n"
        penalty_text += f"Batsman Timeout - {batsman.first_name}\n\n"
        penalty_text += f"{batsman.first_name} has timed out 3 times.\n"
        penalty_text += "OUT - Hit Wicket\n\n"
        penalty_text += f"{batsman.first_name}: {batsman.runs} ({batsman.balls_faced})\n\n"
        penalty_text += f"6 runs penalty deducted.\n\n"
        penalty_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}"
        
        try:
            if gif_url:
                await context.bot.send_animation(
                    chat_id=group_id,
                    animation=gif_url,
                    caption=penalty_text
                )
            else:
                await context.bot.send_message(
                    chat_id=group_id,
                    text=penalty_text
                )
        except Exception as e:
            logger.error(f"Error sending timeout wicket message: {e}")
            await context.bot.send_message(
                chat_id=group_id,
                text=penalty_text
            )
        
        # Log ball
        match.ball_by_ball_log.append({
            "over": format_overs(match.current_bowling_team.balls),
            "batsman": batsman.first_name,
            "bowler": match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx].first_name,
            "result": "Wicket (Timeout)",
            "runs": -6,
            "is_wicket": True
        })
        
        await asyncio.sleep(3)
        
        # Check if innings over
        if match.is_innings_complete():
            await end_innings(context, group_id, match)
        else:
            # Request new batsman
            match.waiting_for_batsman = True
            await request_batsman_selection(context, group_id, match)
    else:
        penalty_text = f"Over {format_overs(match.current_bowling_team.balls)}\n\n"
        penalty_text += f"Batsman Timeout - {batsman.first_name} ({timeout_count}/3)\n\n"
        penalty_text += "6 runs penalty deducted.\n\n"
        penalty_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}\n\n"
        penalty_text += "Please send your number immediately!"
        
        await context.bot.send_message(
            chat_id=group_id,
            text=penalty_text
        )
        
        # Reset and wait again
        match.current_ball_data["batsman_number"] = None
        match.current_ball_data["batsman_start_time"] = time.time()
        match.ball_timeout_task = asyncio.create_task(
            wait_for_batsman_number(context, group_id, match)
        )

async def process_ball_result(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    Calculates the outcome of the ball - FIXED OVER COMPLETE LOGIC
    """
    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    if bat_team.current_batsman_idx is None or bowl_team.current_bowler_idx is None:
        logger.error("❌ No batsman or bowler index!")
        return

    bowler_num = match.current_ball_data.get("bowler_number")
    batsman_num = match.current_ball_data.get("batsman_number")
    
    if bowler_num is None or batsman_num is None:
        logger.error("❌ Missing bowler or batsman number!")
        return
    
    striker = bat_team.players[bat_team.current_batsman_idx]
    bowler = bowl_team.players[bowl_team.current_bowler_idx]
    
    logger.info(f"⚾ Processing ball: Bowler={bowler_num}, Batsman={batsman_num}")
    
    # --- 1. WIDE BALL CHECK (2.5% Chance) ---
    is_wide = random.random() < 0.025
    if is_wide:
        bat_team.score += 1
        bat_team.extras += 1
        gif_url = get_random_gif(MatchEvent.WIDE)
        commentary = get_random_commentary("wide")
        
        msg = (
            f"🏏 <b>Over {format_overs(bowl_team.balls)}</b>\n\n"
            f"🚫 <b>WIDE BALL!</b> (+1 Run)\n"
            f"💬 <i>{commentary}</i>\n\n"
            f"📊 <b>Score:</b> {bat_team.score}/{bat_team.wickets}"
        )
        
        try:
            await context.bot.send_animation(group_id, animation=gif_url, caption=msg, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        
        logger.info("🚫 Wide ball - Re-bowling")
        match.current_ball_data = {}
        await asyncio.sleep(2)
        await execute_ball(context, group_id, match)
        return

    # --- 2. COMPARE NUMBERS (Wicket vs Runs) ---
    if bowler_num == batsman_num:
        logger.info("❌ Numbers matched - Potential WICKET")
        
        # Check for Free Hit
        if match.is_free_hit:
            half_runs = batsman_num // 2
            bat_team.score += half_runs
            striker.runs += half_runs
            striker.balls_faced += 1
            bowler.balls_bowled += 1
            bowler.runs_conceded += half_runs
            
            gif_url = get_random_gif(MatchEvent.FREE_HIT)
            msg = (
                f"⚡ <b>FREE HIT SAVE!</b> Numbers matched ({batsman_num}).\n"
                f"🏃 <b>Runs Awarded:</b> {half_runs} (Half runs)\n"
                f"✅ <b>NOT OUT!</b>"
            )
            
            try:
                await context.bot.send_animation(group_id, animation=gif_url, caption=msg, parse_mode=ParseMode.HTML)
            except:
                await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            match.is_free_hit = False
            bowl_team.update_overs()
            logger.info(f"✅ Free hit saved - Balls now: {bowl_team.balls}")
        else:
            # POTENTIAL WICKET - INTERCEPT FOR DRS
            match.last_wicket_ball = {
                "batsman": striker,
                "bowler": bowler,
                "bowler_number": bowler_num,
                "batsman_number": batsman_num
            }
            
            if bat_team.drs_remaining > 0:
                logger.info("📺 DRS available - Offering review")
                await offer_drs_to_captain(context, group_id, match)
            else:
                logger.info("❌ No DRS - Confirming wicket")
                await confirm_wicket_and_continue(context, group_id, match)
            return

    else:
        # --- 3. RUNS SCORED ---
        runs = batsman_num
        bat_team.score += runs
        striker.runs += runs
        striker.balls_faced += 1
        bowler.balls_bowled += 1
        bowler.runs_conceded += runs
        # ✅ CHECK MILESTONE AFTER EVERY BALL
        await check_and_celebrate_milestones(context, group_id, match, striker, 'batting')
        
        logger.info(f"✅ {runs} RUN(S) scored - Balls: {bowl_team.balls} -> {bowl_team.balls + 1}")
        
        # Tracking Boundaries
        if runs == 4: striker.boundaries += 1
        elif runs == 6: striker.sixes += 1
        
        # Determine Event and Commentary
        events = {0: "dot", 1: "single", 2: "double", 3: "triple", 4: "boundary", 5: "five", 6: "six"}
        comm_key = events.get(runs, "dot")
        event_type = getattr(MatchEvent, f"RUNS_{runs}") if runs > 0 else MatchEvent.DOT_BALL
        
        gif_url = get_random_gif(event_type)
        commentary = get_random_commentary(comm_key)
        
        msg = f"🏏 <b>Over {format_overs(bowl_team.balls)}</b>\n"
        if match.is_free_hit:
            msg += "⚡ <b>FREE HIT</b>\n"
            match.is_free_hit = False
        msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"     
        msg += f"💬 <i>{commentary}</i>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"  
        msg += f"📊 <b>Score:</b> {bat_team.score}/{bat_team.wickets}"
        
        try:
            if gif_url and runs > 0:
                await context.bot.send_animation(group_id, animation=gif_url, caption=msg, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        
        # ✅ UPDATE OVERS (CRITICAL)
        bowl_team.update_overs()
        logger.info(f"✅ Overs updated - Total balls: {bowl_team.balls}, Current over: {bowl_team.get_current_over_balls()}")
        
        # Swap batsmen if odd runs
        if runs % 2 == 1:
            bat_team.swap_batsmen()
            logger.info("🔄 Batsmen swapped (odd runs)")

    # --- 4. CLEANUP AND FLOW CONTROL ---
    match.current_ball_data = {}
    
    # ✅ CRITICAL FIX: Check BEFORE checking innings complete
    current_over_balls = bowl_team.get_current_over_balls()
    logger.info(f"📊 Current over balls: {current_over_balls}, Total balls: {bowl_team.balls}")
    
    # ✅ OVER COMPLETE CHECK (Ball 1-6, when we hit ball 1 of NEXT over)
    if current_over_balls == 1 and bowl_team.balls > 1:
        logger.info("🏏 OVER COMPLETE detected! Calling check_over_complete...")
        await asyncio.sleep(2)
        await check_over_complete(context, group_id, match)
        return
    
    # Check innings complete
    if match.is_innings_complete():
        logger.info("🏁 Innings complete!")
        await end_innings(context, group_id, match)
        return
    
    # Continue to next ball
    logger.info("▶️ Continuing to next ball...")
    await asyncio.sleep(2)
    await execute_ball(context, group_id, match)

async def offer_drs_to_captain(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Broadcasts the DRS offer to the group"""
    match.drs_in_progress = True
    bat_captain = match.get_captain(match.current_batting_team)
    captain_tag = get_user_tag(bat_captain)
    
    msg = f"📺 <b>DRS AVAILABLE</b> 📺\n"
    msg += "━━━━━━━━━━━━━━━\n"
    msg += f"❌ <b>{batsman.first_name}</b> is given OUT!\n"
    msg += f"👤 Captain {captain_tag}\n\n"
    msg += f"🔄 <b>DRS Remaining:</b> {match.current_batting_team.drs_remaining}\n"
    msg += "⏱ <b>You have 10 seconds to review!</b>\n\n"
    msg += "👉 Use <code>/drs</code> to take review"
    
    await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
    
    match.drs_in_progress = True
    match.drs_offer_time = time.time()
    
    # 10 second timeout
    asyncio.create_task(drs_timeout_handler(context, group_id, match))

async def offer_drs(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Offer DRS to batting captain after wicket"""
    batsman = match.last_wicket_ball["batsman"]
    bowler = match.last_wicket_ball["bowler"]
    
    # Check if DRS available
    if match.current_batting_team.drs_remaining <= 0:
        # No DRS available, wicket confirmed
        await confirm_wicket(context, group_id, match, drs_used=False, drs_successful=False)
        return
    
    batting_captain = match.get_captain(match.current_batting_team)
    
    gif_url = get_random_gif(MatchEvent.WICKET)
    commentary = get_random_commentary("wicket")
    
    wicket_text = f"Over {format_overs(match.current_bowling_team.balls)}\n\n"
    wicket_text += f"Bowler: {match.last_wicket_ball['bowler_number']} | Batsman: {match.last_wicket_ball['batsman_number']}\n\n"
    wicket_text += "OUT - Bowled\n\n"
    wicket_text += f"{commentary}\n\n"
    wicket_text += f"{batsman.first_name}: {batsman.runs} ({batsman.balls_faced})\n\n"
    wicket_text += f"Captain {batting_captain.first_name}: You have {match.current_batting_team.drs_remaining} DRS review.\n"
    wicket_text += "Do you want to review this decision?\n\n"
    wicket_text += "Use /drs to review (30 seconds to decide)"
    
    try:
        if gif_url:
            await context.bot.send_animation(
                chat_id=group_id,
                animation=gif_url,
                caption=wicket_text
            )
        else:
            await context.bot.send_message(
                chat_id=group_id,
                text=wicket_text
            )
    except Exception as e:
        logger.error(f"Error sending wicket message: {e}")
        await context.bot.send_message(
            chat_id=group_id,
            text=wicket_text
        )
    
    match.drs_in_progress = True
    
    # Set timeout for DRS decision
    asyncio.create_task(drs_decision_timeout(context, group_id, match))

async def drs_timeout_handler(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle 10 second DRS timeout - FIXED"""
    await asyncio.sleep(10)  # ✅ 10 seconds (not 60)
    
    if not match.drs_in_progress:
        return
    
    # Timeout - No DRS taken
    match.drs_in_progress = False
    
    await context.bot.send_message(
        group_id,
        "⏱ <b>DRS Timeout!</b> Decision stands. Wicket confirmed.",
        parse_mode=ParseMode.HTML
    )
    
    await asyncio.sleep(1)
    await confirm_wicket_and_continue(context, group_id, match)

async def drs_decision_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle DRS decision timeout"""
    await asyncio.sleep(30)
    
    if not match.drs_in_progress:
        return
    
    # No DRS taken, confirm wicket
    match.drs_in_progress = False
    await confirm_wicket(context, group_id, match, drs_used=False, drs_successful=False)

# DRS command
async def drs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /drs command - Captain only"""
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.type == "private":
        await update.message.reply_text("This command only works in groups.")
        return
    
    if chat.id not in active_matches:
        await update.message.reply_text("No active match found.")
        return
    
    match = active_matches[chat.id]
    
    if not match.drs_in_progress:
        await update.message.reply_text("No DRS review available at this moment.")
        return
    
    # Check if user is batting captain
    batting_captain = match.get_captain(match.current_batting_team)
    if user.id != batting_captain.user_id:
        await update.message.reply_text(
            f"⚠️ Only {match.current_batting_team.name} Captain can request DRS."
        )
        return
    
    # Process DRS
    match.drs_in_progress = False
    match.current_batting_team.drs_remaining -= 1
    
    await process_drs_review(context, chat.id, match)


async def process_drs_review(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Process DRS with 30% overturn chance"""
    batsman = match.last_wicket_ball["batsman"]
    
    drs_text = "📺 <b>DRS REVIEW IN PROGRESS</b>\n\n"
    drs_text += "🔍 Checking with third umpire...\n"
    drs_text += "⏳ Please wait..."
    
    gif_url = get_random_gif(MatchEvent.DRS_REVIEW)
    
    try:
        if gif_url:
            msg = await context.bot.send_animation(group_id, animation=gif_url, caption=drs_text, parse_mode=ParseMode.HTML)
        else:
            msg = await context.bot.send_message(group_id, drs_text, parse_mode=ParseMode.HTML)
    except:
        msg = await context.bot.send_message(group_id, drs_text, parse_mode=ParseMode.HTML)
    
    await asyncio.sleep(3)
    
    # 30% overturn chance
    is_overturned = random.random() < 0.30
    
    if is_overturned:
        # NOT OUT
        batsman.is_out = False
        match.current_batting_team.wickets -= 1
        match.current_batting_team.out_players_indices.discard(match.current_batting_team.current_batsman_idx)
        
        gif = "https://tenor.com/bOVyJ.gif"
        
        result_text = "📺 <b>DRS RESULT</b>\n\n"
        result_text += "✅ <b>NOT OUT!</b>\n\n"
        result_text += f"🎉 {batsman.first_name} survives!\n"
        result_text += "Decision overturned.\n\n"
        result_text += f"🔄 DRS Remaining: {match.current_batting_team.drs_remaining}"
        
        try:
            if gif_url:
                await context.bot.send_animation(group_id, animation=gif_url, caption=result_text, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(group_id, result_text, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(group_id, result_text, parse_mode=ParseMode.HTML)
        
        await asyncio.sleep(2)
        
        # Continue with same batsman
        if match.current_bowling_team.get_current_over_balls() == 0:
            await check_over_complete(context, group_id, match)
        else:
            await execute_ball(context, group_id, match)
    else:
        # OUT confirmed
        gif = "https://t.me/cricoverse/37"
        
        result_text = "📺 <b>DRS RESULT</b>\n\n"
        result_text += "❌ <b>OUT!</b>\n\n"
        result_text += "Decision stands.\n\n"
        result_text += f"📊 {batsman.first_name}: {batsman.runs} ({batsman.balls_faced})\n"
        result_text += f"🔄 DRS Remaining: {match.current_batting_team.drs_remaining}"
        
        try:
            if gif_url:
                await context.bot.send_animation(group_id, animation=gif_url, caption=result_text, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(group_id, result_text, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(group_id, result_text, parse_mode=ParseMode.HTML)
        
        await asyncio.sleep(2)
        await confirm_wicket_and_continue(context, group_id, match)

async def confirm_wicket_and_continue(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    ✅ COMPLETE WICKET HANDLER - FIXED: Batsman variable defined
    """
    
    logger.info(f"🔴 === WICKET HANDLER START === Group: {group_id}")
    
    if match.phase == GamePhase.MATCH_ENDED:
        logger.warning("⚠️ Match already ended, aborting wicket handling")
        return

    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    # Get the OUT player
    if bat_team.current_batsman_idx is None:
        logger.error("❌ CRITICAL: No batsman index set!")
        await context.bot.send_message(group_id, "⚠️ Error: No batsman found!", parse_mode=ParseMode.HTML)
        return
    
    out_player = bat_team.players[bat_team.current_batsman_idx]
    out_player_tag = get_user_tag(out_player)
    bowler = bowl_team.players[bowl_team.current_bowler_idx]
    
    logger.info(f"🎯 OUT Player: {out_player.first_name} (Index: {bat_team.current_batsman_idx})")
    
    # ✅ STEP 1: SEND WICKET NOTIFICATION WITH GIF
    wicket_gif = get_random_gif(MatchEvent.WICKET)
    commentary = get_random_commentary("wicket")
    
    wicket_msg = f"❌ <b>WICKET!</b> ❌\n"
    wicket_msg += "─────────────────────\n"
    wicket_msg += f"🏏 <b>{out_player_tag}</b> is OUT!\n"
    wicket_msg += f"⚾ Bowler: <b>{bowler.first_name}</b>\n\n"
    wicket_msg += f"💬 <i>{commentary}</i>\n\n"
    wicket_msg += f"📊 <b>Score:</b> {out_player.runs} ({out_player.balls_faced})\n"
    wicket_msg += f"⚡ <b>Strike Rate:</b> {round((out_player.runs/max(out_player.balls_faced,1))*100, 1)}\n"
    wicket_msg += "─────────────────────"
    
    try:
        await context.bot.send_animation(group_id, animation=wicket_gif, caption=wicket_msg, parse_mode=ParseMode.HTML)
        logger.info("✅ Wicket notification sent to group")
    except:
        await context.bot.send_message(group_id, wicket_msg, parse_mode=ParseMode.HTML)
    
    await asyncio.sleep(2)
    
    # ✅ STEP 2: CHECK IF DRS AVAILABLE
    if bat_team.drs_remaining > 0:
        logger.info("📺 DRS available - Offering review")
        match.drs_in_progress = True
        match.last_wicket_ball = {
            "batsman": out_player,  # ✅ FIX: Use out_player, not undefined 'batsman'
            "bowler": bowler,
            "bowler_number": match.current_ball_data.get("bowler_number"),
            "batsman_number": match.current_ball_data.get("batsman_number")
        }
        
        batting_captain = match.get_captain(bat_team)
        captain_tag = get_user_tag(batting_captain)
        
        drs_msg = f"📺 <b>DRS AVAILABLE</b> 📺\n"
        drs_msg += "─────────────────────\n"
        drs_msg += f"❌ <b>{out_player.first_name}</b> is given OUT!\n"  # ✅ FIX: Use out_player
        drs_msg += f"👤 Captain {captain_tag}\n\n"
        drs_msg += f"🔄 <b>DRS Remaining:</b> {bat_team.drs_remaining}\n"
        drs_msg += "⏱ <b>You have 10 seconds to review!</b>\n\n"
        drs_msg += "👉 Use <code>/drs</code> to take review"
        
        await context.bot.send_message(group_id, drs_msg, parse_mode=ParseMode.HTML)
        
        match.drs_in_progress = True
        match.drs_offer_time = time.time()
        
        # ✅ 10 SECOND TIMER
        asyncio.create_task(drs_timeout_handler(context, group_id, match))
        return
    
    # ✅ STEP 3: NO DRS - PROCEED WITH WICKET
    logger.info("❌ No DRS - Confirming wicket")
    
    await asyncio.sleep(1)
    
    # Update stats
    bat_team.wickets += 1
    bowler.wickets += 1
    out_player.is_out = True
    out_player.balls_faced += 1
    bowler.balls_bowled += 1
    bowl_team.update_overs()
    
    # ✅ CHECK BOWLING MILESTONE
    await check_and_celebrate_milestones(context, group_id, match, bowler, 'bowling')
    
    # Send Mini Scorecard
    try:
        mini_card = generate_mini_scorecard(match)
        await context.bot.send_message(group_id, mini_card, parse_mode=ParseMode.HTML)
        logger.info("✅ Mini scorecard sent")
    except Exception as e:
        logger.error(f"❌ Failed to send mini scorecard: {e}")
    
    await asyncio.sleep(2)
    
    # Mark player as OUT
    bat_team.out_players_indices.add(bat_team.current_batsman_idx)
    logger.info(f"✅ Marked {out_player.first_name} as OUT (Total Out: {len(bat_team.out_players_indices)})")
    
    # Clear striker position
    bat_team.current_batsman_idx = None
    logger.info("✅ Striker position cleared, non-striker remains same")
    
    # Check innings end conditions
    remaining_players = len(bat_team.players) - len(bat_team.out_players_indices)
    logger.info(f"📊 Remaining Players: {remaining_players}")
    
    if bat_team.is_all_out():
        logger.info("🚫 ALL OUT - Ending Innings")
        await context.bot.send_message(group_id, "❌ <b>ALL OUT!</b> Innings ended.", parse_mode=ParseMode.HTML)
        await asyncio.sleep(2)
        await end_innings(context, group_id, match)
        return
    
    if bat_team.balls >= match.total_overs * 6:
        logger.info("🏁 Overs complete - Ending Innings")
        await end_innings(context, group_id, match)
        return
    
    if match.innings == 2 and bat_team.score >= match.target:
        logger.info("🏆 Target chased - Match Won")
        await end_innings(context, group_id, match)
        return
    
    # Request new batsman
    match.waiting_for_batsman = True
    match.waiting_for_bowler = False
    match.current_ball_data = {}
    logger.info("⏸️ Game PAUSED - waiting_for_batsman=True")
    
    batting_captain = match.get_captain(bat_team)
    if not batting_captain:
        logger.error("❌ CRITICAL: No batting captain found!")
        await context.bot.send_message(group_id, "⚠️ Error: No captain found!", parse_mode=ParseMode.HTML)
        return
    
    captain_tag = get_user_tag(batting_captain)
    available_batsmen = [
        p for i, p in enumerate(bat_team.players) 
        if i not in bat_team.out_players_indices 
        and i != bat_team.current_non_striker_idx
    ]
    available_count = len(available_batsmen)
    
    if available_count == 0:
        logger.error("❌ No available batsmen!")
        await end_innings(context, group_id, match)
        return
    
    msg = f"🔴 <b>NEW BATSMAN NEEDED!</b>\n"
    msg += "─────────────────────\n\n"
    msg += f"❌ <b>{out_player.first_name}</b> is OUT!\n\n"
    msg += f"🧢 <b>{captain_tag}</b>, select the <b>NEW STRIKER</b>:\n"
    msg += f"👉 <b>Command:</b> <code>/batting [serial]</code>\n\n"
    msg += f"📊 <b>Score:</b> {bat_team.score}/{bat_team.wickets}\n"
    msg += f"👥 <b>Available Players:</b> {available_count}\n\n"
    msg += "─────────────────────\n"
    msg += f"<i>⏱ You have 2 minutes to select</i>"
    
    await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
    logger.info("✅ New batsman request sent to group")
    
    # Start selection timer
    match.batsman_selection_time = time.time()
    match.batsman_selection_task = asyncio.create_task(
        batsman_selection_timeout(context, group_id, match)
    )
    logger.info("✅ Batsman selection timer started")
    logger.info(f"🔴 === WICKET HANDLER END ===\n")

async def confirm_wicket(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, drs_used: bool, drs_successful: bool):
    """Confirm wicket and update match state"""
    batsman = match.last_wicket_ball["batsman"]
    bowler = match.last_wicket_ball["bowler"]
    
    # Mark batsman as out
    batsman.is_out = True
    batsman.dismissal_type = "Bowled"
    match.current_batting_team.wickets += 1
    bowler.wickets += 1
    # ✅ CHECK BOWLING MILESTONE
    await check_and_celebrate_milestones(context, group_id, match, bowler, 'bowling')

    # ✅ CHECK BOWLING MILESTONE
    await check_and_celebrate_milestones(context, group_id, match, bowler, 'bowling')
    
    if drs_used and not drs_successful:
        gif_url = get_random_gif(MatchEvent.DRS_OUT)
        
        result_text = "DRS Result\n\n"
        result_text += "Decision: OUT\n\n"
        result_text += "The original decision stands.\n\n"
        result_text += f"{batsman.first_name}: {batsman.runs} ({batsman.balls_faced})\n\n"
        result_text += f"DRS Remaining: {match.current_batting_team.drs_remaining}\n\n"
        result_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}"
        
        try:
            if gif_url:
                await context.bot.send_animation(
                    chat_id=group_id,
                    animation=gif_url,
                    caption=result_text
                )
            else:
                await context.bot.send_message(
                    chat_id=group_id,
                    text=result_text
                )
        except Exception as e:
            logger.error(f"Error sending DRS out message: {e}")
            await context.bot.send_message(
                chat_id=group_id,
                text=result_text
            )
    else:
        wicket_confirm_text = f"Wicket Confirmed\n\n"
        wicket_confirm_text += f"{batsman.first_name}: {batsman.runs} ({batsman.balls_faced})\n"
        wicket_confirm_text += f"Bowler: {bowler.first_name}\n\n"
        wicket_confirm_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}"
        
        await context.bot.send_message(
            chat_id=group_id,
            text=wicket_confirm_text
        )
    
    # Update stats
    batsman.balls_faced += 1
    bowler.balls_bowled += 1
    match.current_bowling_team.update_overs()
    
    # Check for duck
    if batsman.runs == 0:
        batsman.ducks += 1
    
    # Log ball
    match.ball_by_ball_log.append({
        "over": format_overs(match.current_bowling_team.balls - 1),
        "batsman": batsman.first_name,
        "bowler": bowler.first_name,
        "result": "Wicket",
        "runs": 0,
        "is_wicket": True
    })
    
    await asyncio.sleep(2)
    
    # Check if innings over
    if match.is_innings_complete():
        await end_innings(context, group_id, match)
    else:
        # Request new batsman
        match.waiting_for_batsman = True
        await request_batsman_selection(context, group_id, match)

# --- UPDATE handle_dm_message ---

# /soloplayers
async def soloplayers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fancy Solo Player List"""
    chat = update.effective_chat
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    if match.game_mode != "SOLO":
        await update.message.reply_text("⚠️ This is not a Solo match!")
        return
        
    msg = "📜 <b>SOLO BATTLE ROSTER</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    
    for i, p in enumerate(match.solo_players, 1):
        # Status Logic
        status = "⏳ <i>Waiting</i>"
        if p.is_out: status = "❌ <b>OUT</b>"
        elif match.phase == GamePhase.SOLO_MATCH:
            if i-1 == match.current_solo_bat_idx: status = "🏏 <b>BATTING</b>"
            elif i-1 == match.current_solo_bowl_idx: status = "⚾ <b>BOWLING</b>"
            elif p.is_bowling_banned: status = "🚫 <b>BANNED (Bowl)</b>"
            
        msg += f"<b>{i}. {p.first_name}</b>\n   └ {status} • {p.runs} Runs\n"
        
    msg += "━━━━━━━━━━━━━━━━━━━━━━"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

# /soloscore
async def soloscore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ ENHANCED Solo Leaderboard - Detailed & Clean"""
    chat = update.effective_chat
    
    if chat.id not in active_matches: 
        await update.message.reply_text("⚠️ No active match!")
        return
    
    match = active_matches[chat.id]
    
    if match.game_mode != "SOLO":
        await update.message.reply_text("⚠️ This is not a Solo match!")
        return
    
    # Sort by Runs (Desc)
    sorted_players = sorted(match.solo_players, key=lambda x: x.runs, reverse=True)
    
    msg = "🏆 <b>SOLO BATTLE LEADERBOARD</b> 🏆\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    medals = ["🥇", "🥈", "🥉"]
    
    for i, p in enumerate(sorted_players, 1):
        rank = medals[i-1] if i <= 3 else f"<b>{i}.</b>"
        
        # Status
        status = ""
        if p.is_out: 
            status = " ❌"
        elif i-1 == match.current_solo_bat_idx: 
            status = " 🏏"
        elif i-1 == match.current_solo_bowl_idx: 
            status = " ⚾"
        
        # Stats
        sr = round((p.runs / max(p.balls_faced, 1)) * 100, 1)
        
        msg += f"{rank} <b>{p.first_name}</b>{status}\n"
        msg += f"   📊 <b>{p.runs}</b> runs ({p.balls_faced} balls)\n"
        msg += f"   ⚡ SR: {sr} | 🎯 Wickets: {p.wickets}\n\n"
    
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<i>🔝 Top 3 in the spotlight!</i>"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def start_solo_mechanics(context, chat_id, match):
    """Initializes Solo Match with randomized order"""
    match.phase = GamePhase.SOLO_MATCH
    
    # Randomize Order
    random.shuffle(match.solo_players)
    
    match.current_solo_bat_idx = 0
    match.current_solo_bowl_idx = 1
    match.solo_balls_this_spell = 0
    
    # Announce Order
    order_msg = "🎲 <b>TOSS & BATTING ORDER</b>\n"
    order_msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    order_msg += "The order has been shuffled! Here is the lineup:\n\n"
    
    for i, p in enumerate(match.solo_players, 1):
        ptag = f"<a href='tg://user?id={p.user_id}'>{p.first_name}</a>"
        role = " (🏏 Striker)" if i == 1 else " (⚾ Bowler)" if i == 2 else ""
        order_msg += f"<code>{i}.</code> <b>{ptag}</b>{role}\n"
    
    order_msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    order_msg += "🔥 <i>Match Starting in 5 seconds...</i>"
    
    # Send with Toss/Squad Image
    await context.bot.send_photo(
        chat_id=chat_id,
        photo=MEDIA_ASSETS.get("toss"),
        caption=order_msg,
        parse_mode=ParseMode.HTML
    )
    
    await asyncio.sleep(5)
    
    # Start First Ball
    await trigger_solo_ball(context, chat_id, match)

async def trigger_solo_ball(context, chat_id, match):
    """Sets up the next ball with Timers"""
    batter = match.solo_players[match.current_solo_bat_idx]
    bowler = match.solo_players[match.current_solo_bowl_idx]
    
    match.current_ball_data = {
        "bowler_id": bowler.user_id,
        "bowler_number": None,
        "batsman_number": None,
        "is_solo": True
    }
    
    bat_tag = f"<a href='tg://user?id={batter.user_id}'>{batter.first_name}</a>"
    bowl_tag = f"<a href='tg://user?id={bowler.user_id}'>{bowler.first_name}</a>"
    
    # Calculate Strike Rate
    sr = round((batter.runs / batter.balls_faced) * 100, 1) if batter.balls_faced > 0 else 0
    
    msg = f"🔴 <b>LIVE</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⚾ <b>{bowl_tag}</b> is going for run up...\n"
    msg += f"🔄 <b>Spell:</b> Ball {match.solo_balls_this_spell + 1}/3\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━"
    
    keyboard = [[InlineKeyboardButton("📩 Tap to Bowl", url=f"https://t.me/{context.bot.username}")]]
    
    ball_gif = "https://t.me/kyanaamrkhe/6"
    try:
        await context.bot.send_animation(chat_id, ball_gif, caption=msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except:
        await context.bot.send_message(chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    
    # DM Bowler
    try:
        dm_msg = f"⚔️ <b>SOLO MATCH</b>\n"
        dm_msg += f"🎯 Target: <b>{batter.first_name}</b> (Runs: {batter.runs})\n"
        dm_msg += "👉 Send your number (0-6)"
        await context.bot.send_message(bowler.user_id, dm_msg, parse_mode=ParseMode.HTML)
        
        # ✅ START BOWLER TIMER
        match.ball_timeout_task = asyncio.create_task(
            solo_game_timer(context, chat_id, match, "bowler", bowler.first_name)
        )
    except:
        await context.bot.send_message(chat_id, f"⚠️ Cannot DM {bowl_tag}. Please start the bot!", parse_mode=ParseMode.HTML)

async def process_solo_turn_result(context, chat_id, match):
    """Calculates Solo result with STRICT GIF Mapping for ALL runs"""
    batter = match.solo_players[match.current_solo_bat_idx]
    bowler = match.solo_players[match.current_solo_bowl_idx]
    
    bat_num = match.current_ball_data["batsman_number"]
    bowl_num = match.current_ball_data["bowler_number"]
    
    bat_tag = f"<a href='tg://user?id={batter.user_id}'>{batter.first_name}</a>"
    bowl_tag = f"<a href='tg://user?id={bowler.user_id}'>{bowler.first_name}</a>"

    # --- WICKET LOGIC ---
    if bat_num == bowl_num:
        batter.is_out = True
        match.solo_balls_this_spell = 0
        
        gif = get_random_gif(MatchEvent.WICKET)
        commentary = get_random_commentary("wicket")
        
        sr = round((batter.runs / batter.balls_faced) * 100, 1) if batter.balls_faced > 0 else 0
        
        msg = f"❌ <b>OUT! {batter.first_name} is gone!</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🏏 <b>Final Score:</b> {batter.runs} ({batter.balls_faced})\n"
        msg += f"⚡ <b>Strike Rate:</b> {sr}\n"
        msg += f"🎯 <b>Wicket:</b> {bowl_tag}\n\n"
        msg += f"💬 <i>{commentary}</i>"
        
        try:
            if gif:
                await context.bot.send_animation(chat_id, gif, caption=msg, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
        
        match.current_solo_bat_idx += 1
        
        if match.current_solo_bat_idx >= len(match.solo_players):
            await end_solo_game_logic(context, chat_id, match)
            return
            
        match.current_solo_bowl_idx = (match.current_solo_bat_idx + 1) % len(match.solo_players)
        new_batter = match.solo_players[match.current_solo_bat_idx]
        new_bat_tag = f"<a href='tg://user?id={new_batter.user_id}'>{new_batter.first_name}</a>"
        
        await asyncio.sleep(2)
        await context.bot.send_message(chat_id, f"⚡ <b>NEXT BATSMAN:</b> {new_bat_tag} walks to the crease!", parse_mode=ParseMode.HTML)
        
    # --- RUNS LOGIC (STRICT GIF MAPPING) ---
    else:
        runs = bat_num
        batter.runs += runs
        batter.balls_faced += 1
        
        # ✅ EXACT GIF MAPPING (0-6)
        if runs == 0:
            event = MatchEvent.DOT_BALL
            comm_key = "dot"
        elif runs == 1:
            event = MatchEvent.RUNS_1
            comm_key = "single"
        elif runs == 2:
            event = MatchEvent.RUNS_2
            comm_key = "double"
        elif runs == 3:
            event = MatchEvent.RUNS_3
            comm_key = "triple"
        elif runs == 4:
            event = MatchEvent.RUNS_4
            comm_key = "boundary"
        elif runs == 5:
            event = MatchEvent.RUNS_5
            comm_key = "five"
        elif runs == 6:
            event = MatchEvent.RUNS_6
            comm_key = "six"
        
        gif = get_random_gif(event)
        commentary = get_random_commentary(comm_key)
        
        sr = round((batter.runs / batter.balls_faced) * 100, 1) if batter.balls_faced > 0 else 0
        
        msg = f"🔴 <b>LIVE</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🏏 <b>{runs} RUN{'S' if runs != 1 else ''}!</b>\n"
        msg += f"💬 <i>{commentary}</i>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📊 <b>{batter.first_name}:</b> {batter.runs} ({batter.balls_faced})\n"
        msg += f"⚡ <b>Strike Rate:</b> {sr}"
        
        try:
            # ✅ FORCE GIF for ALL runs
            if gif:
                await context.bot.send_animation(chat_id, gif, caption=msg, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)

        match.solo_balls_this_spell += 1
        
        if match.solo_balls_this_spell >= 3:
            match.solo_balls_this_spell = 0
            
            old_idx = match.current_solo_bowl_idx
            next_idx = (old_idx + 1) % len(match.solo_players)
            
            if next_idx == match.current_solo_bat_idx:
                next_idx = (next_idx + 1) % len(match.solo_players)
                
            match.current_solo_bowl_idx = next_idx
            new_bowler = match.solo_players[next_idx]
            new_bowl_tag = f"<a href='tg://user?id={new_bowler.user_id}'>{new_bowler.first_name}</a>"
            
            await asyncio.sleep(1)
            await context.bot.send_message(chat_id, f"🔄 <b>CHANGE OF OVER!</b>\nNew Bowler: {new_bowl_tag} takes the ball.", parse_mode=ParseMode.HTML)

    await asyncio.sleep(3)
    await trigger_solo_ball(context, chat_id, match)

# --- NEW: Solo Callback Handler ---
async def solo_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Solo Join/Leave AND Start Game - WITH TIMER"""
    query = update.callback_query
    await query.answer()
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    # --- ACTION: START GAME ---
    if query.data == "solo_start_game":
        # 1. Check Permissions (Host Only)
        if user.id != match.host_id:
            await query.answer("⚠️ Only the Host can start the match!", show_alert=True)
            return
            
        # 2. Check Player Count
        if len(match.solo_players) < 2:
            await query.answer("⚠️ Need at least 2 players to start!", show_alert=True)
            return

        # 3. Cancel Timer Task before starting
        if hasattr(match, 'solo_timer_task') and match.solo_timer_task:
            match.solo_timer_task.cancel()

        # 4. Start the Game Logic
        await start_solo_mechanics(context, chat.id, match)
        return

    # --- VALIDATION FOR JOIN/LEAVE ---
    if match.game_mode != "SOLO" or match.phase != GamePhase.SOLO_JOINING:
        await query.answer("Registration Closed!", show_alert=True)
        return

    user_tag = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"

    # --- ACTION: JOIN ---
    if query.data == "solo_join":
        # Check duplicate
        for p in match.solo_players:
            if p.user_id == user.id:
                await query.answer("Already joined!", show_alert=True)
                return
        
        # Init Stats if new user
        if user.id not in player_stats: init_player_stats(user.id)
        
        # Add Player
        p = Player(user.id, user.username or "", user.first_name)
        match.solo_players.append(p)
        
        # Update the Board Image
        await update_solo_board(context, chat.id, match)
        
        # Send Tagged Message
        msg = f"✅ <b>{user_tag}</b> has entered the Battleground! 🔥"
        await context.bot.send_message(chat.id, msg, parse_mode=ParseMode.HTML)

    # --- ACTION: LEAVE ---
    elif query.data == "solo_leave":
        for i, p in enumerate(match.solo_players):
            if p.user_id == user.id:
                match.solo_players.pop(i)
                
                # Update the Board Image
                await update_solo_board(context, chat.id, match)
                
                # Send Tagged Message
                msg = f"👋 <b>{user_tag}</b> chickened out and left."
                await context.bot.send_message(chat.id, msg, parse_mode=ParseMode.HTML)
                return
        
        await query.answer("You are not in the list.", show_alert=True)

async def solo_join_countdown(context, chat_id, match):
    """Background Timer for Solo Joining Phase - Auto Updates Board"""
    try:
        warning_sent = False
        while True:
            # Check if phase changed
            if match.phase != GamePhase.SOLO_JOINING:
                break

            remaining = match.solo_join_end_time - time.time()
            
            # 30 Seconds Warning
            if remaining <= 30 and remaining > 20 and not warning_sent:
                await context.bot.send_message(
                    chat_id, 
                    "⚠️ <b>Hurry Up! Only 30 seconds left to join!</b>", 
                    parse_mode=ParseMode.HTML
                )
                warning_sent = True

            # Time Up
            if remaining <= 0:
                # Check minimum players
                if len(match.solo_players) < 2:
                    await context.bot.send_message(
                        chat_id,
                        "❌ <b>Match Cancelled!</b>\nNot enough players joined.",
                        parse_mode=ParseMode.HTML
                    )
                    del active_matches[chat_id]
                else:
                    # Auto-start the game
                    await context.bot.send_message(
                        chat_id,
                        "⏰ <b>Time's Up!</b> Starting match now...",
                        parse_mode=ParseMode.HTML
                    )
                    await start_solo_mechanics(context, chat_id, match)
                break
            
            # Wait 10 seconds before next update
            await asyncio.sleep(10)
            
            # Update Board
            if match.phase == GamePhase.SOLO_JOINING:
                await update_solo_board(context, chat_id, match)
            
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Solo timer error: {e}")

async def end_solo_game_logic(context, chat_id, match):
    """✅ FIXED: Solo End with GC Notification"""
    match.phase = GamePhase.MATCH_ENDED
    
    sorted_players = sorted(match.solo_players, key=lambda x: x.runs, reverse=True)
    winner = sorted_players[0]
    
    # Save Stats
    for p in match.solo_players:
        init_player_stats(p.user_id)
        
        if p.user_id in player_stats:
            s = player_stats[p.user_id]["solo"]
            s["matches"] += 1
            s["runs"] += p.runs
            s["balls"] += p.balls_faced
            s["wickets"] += p.wickets
            if p.runs == 0 and p.is_out: s["ducks"] += 1
            if p.runs > s["highest"]: s["highest"] = p.runs
            
            if p.user_id == winner.user_id: 
                s["wins"] += 1
            if p in sorted_players[:3]:
                s["top_3_finishes"] += 1
                
    save_data()

    # ✅ 1. NOTIFY ALL PLAYERS IN GC
    winner_tag = f"<a href='tg://user?id={winner.user_id}'>{winner.first_name}</a>"
    
    notify_msg = f"🏁 <b>SOLO BATTLE ENDED!</b> 🏁\n"
    notify_msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    notify_msg += f"🎊 <b>Winner:</b> {winner_tag}\n"
    notify_msg += f"📊 <b>Final Score:</b> {winner.runs} runs\n\n"
    notify_msg += f"<i>🏆 Congratulations to the champion!</i>\n"
    notify_msg += f"<i>📋 Check /soloscore for final standings</i>"
    
    await context.bot.send_message(chat_id, notify_msg, parse_mode=ParseMode.HTML)
    await asyncio.sleep(2)

    # ✅ 2. VICTORY GIF WITH DETAILED CARD
    victory_gif = get_random_gif(MatchEvent.VICTORY)
    winner_sr = round((winner.runs / winner.balls_faced) * 100, 1) if winner.balls_faced > 0 else 0
    
    msg = f"🏆 <b>SOLO BATTLE CHAMPION</b> 🏆\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"👑 <b>WINNER: {winner_tag}</b>\n"
    msg += f"💥 <b>Score:</b> {winner.runs} ({winner.balls_faced})\n"
    msg += f"🔥 <b>Strike Rate:</b> {winner_sr}\n"
    msg += f"🎯 <b>Wickets:</b> {winner.wickets}\n\n"
    
    msg += "📊 <b>FINAL LEADERBOARD</b>\n"
    medals = ["🥇", "🥈", "🥉"]
    
    for i, p in enumerate(sorted_players):
        rank_icon = medals[i] if i < 3 else f"<b>{i+1}.</b>"
        status = " (Not Out)" if not p.is_out else ""
        sr = round((p.runs / p.balls_faced) * 100, 1) if p.balls_faced > 0 else 0
        msg += f"{rank_icon} <b>{p.first_name}</b>: {p.runs}({p.balls_faced}) SR: {sr}{status}\n"
        
    msg += "━━━━━━━━━━━━━━━━━━━━━"

    try:
        if victory_gif:
            await context.bot.send_animation(chat_id, victory_gif, caption=msg, parse_mode=ParseMode.HTML)
        else:
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
    except:
        await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)

    # ✅ 3. POTM CARD
    await asyncio.sleep(2)
    
    potm_gif = "https://tenor.com/bT8BA.gif"
    
    mvp_msg = f"🌟 <b>PLAYER OF THE MATCH</b> 🌟\n"
    mvp_msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
    mvp_msg += f"👤 <b>{winner.first_name}</b>\n"
    mvp_msg += f"🏅 <i>Outstanding Survival & Scoring</i>\n\n"
    
    mvp_msg += f"🏏 <b>Runs:</b> {winner.runs}\n"
    mvp_msg += f"⏳ <b>Balls Faced:</b> {winner.balls_faced}\n"
    mvp_msg += f"⚡ <b>Strike Rate:</b> {winner_sr}\n"
    
    mvp_msg += "👏 <i>The Ultimate Survivor!</i>"
    
    try:
        await context.bot.send_animation(chat_id, potm_gif, caption=mvp_msg, parse_mode=ParseMode.HTML)
    except:
        await context.bot.send_message(chat_id, mvp_msg, parse_mode=ParseMode.HTML)

    del active_matches[chat_id]

async def endsolo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    if match.game_mode != "SOLO": return
    
    # Admin Check (Simplistic)
    if user.id != match.host_id and user.id != OWNER_ID:
        await update.message.reply_text("Only Host can end solo match.")
        return
        
    await end_solo_game_logic(context, chat.id, match)

# /extendsolo
async def extendsolo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    if match.game_mode != "SOLO" or match.phase != GamePhase.SOLO_JOINING:
        await update.message.reply_text("Can only extend during joining phase.")
        return

    try:
        sec = int(context.args[0])
        match.solo_join_end_time += sec
        await update.message.reply_text(f"✅ Extended by {sec} seconds!")
    except:
        await update.message.reply_text("Usage: /extendsolo <seconds>")

async def check_over_complete(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    End of Over: Show Mini Scorecard, Swap Batsmen, Request New Bowler
    """
    
    logger.info(f"🟢 check_over_complete called for group {group_id}")
    
    if match.phase == GamePhase.MATCH_ENDED:
        logger.warning("⚠️ Match already ended, skipping over complete")
        return

    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    # Get current bowler info BEFORE clearing
    if bowl_team.current_bowler_idx is not None:
        bowler = bowl_team.players[bowl_team.current_bowler_idx]
        bowl_team.bowler_history.append(bowl_team.current_bowler_idx)
        logger.info(f"✅ Bowler {bowler.first_name} added to history")
    else:
        bowler = type("obj", (object,), {"first_name": "Unknown", "wickets": 0, "runs_conceded": 0})
        logger.error("❌ No bowler found at over complete!")
    
    # ✅ STEP 1: SEND MINI SCORECARD
    mini_card = generate_mini_scorecard(match)
    await context.bot.send_message(group_id, mini_card, parse_mode=ParseMode.HTML)
    logger.info("✅ Mini scorecard sent")
    
    await asyncio.sleep(1)
    
    # ✅ STEP 2: SWAP BATSMEN (Strike Rotation)
    logger.info(f"🔄 Swapping batsmen - Before: Striker={bat_team.current_batsman_idx}, Non-Striker={bat_team.current_non_striker_idx}")
    bat_team.swap_batsmen()
    logger.info(f"🔄 After swap: Striker={bat_team.current_batsman_idx}, Non-Striker={bat_team.current_non_striker_idx}")
    
    # Re-fetch players after swap
    new_striker = bat_team.players[bat_team.current_batsman_idx] if bat_team.current_batsman_idx is not None else None
    new_non_striker = bat_team.players[bat_team.current_non_striker_idx] if bat_team.current_non_striker_idx is not None else None

    # Over Complete Summary
    summary = f"🏏 <b>OVER COMPLETE!</b> ({format_overs(bowl_team.balls)})\n"
    summary += "━━━━━━━━━━━━━━━━━━━━━\n"
    summary += f"⚾ Bowler <b>{bowler.first_name}</b> finished his over.\n\n"
    summary += f"🔄 <b>BATSMEN SWAPPED:</b>\n"
    summary += f"  🟢 New Striker: {new_striker.first_name if new_striker else 'None'}\n"
    summary += f"  ⚪ Non-Striker: {new_non_striker.first_name if new_non_striker else 'None'}\n\n"
    summary += "━━━━━━━━━━━━━━━━━━━━━"
    
    await context.bot.send_message(group_id, summary, parse_mode=ParseMode.HTML)
    logger.info("✅ Over summary sent to group")
    
    # ✅ STEP 3: CHECK INNINGS/MATCH END
    if bat_team.balls >= match.total_overs * 6:
        logger.info("🏁 Innings complete - Overs finished")
        await end_innings(context, group_id, match)
        return
    
    if match.innings == 2 and bat_team.score >= match.target:
        logger.info("🏆 Match won - Target chased")
        await end_innings(context, group_id, match)
        return
    
    # ✅ STEP 4: CLEAR OLD BOWLER & PAUSE GAME
    logger.info(f"🚫 Clearing bowler - Old index: {bowl_team.current_bowler_idx}")
    bowl_team.current_bowler_idx = None  
    match.waiting_for_bowler = True
    match.waiting_for_batsman = False
    match.current_ball_data = {}
    logger.info("✅ Match state set to waiting_for_bowler=True")
    
    await asyncio.sleep(2)
    
    # ✅ STEP 5: REQUEST NEW BOWLER (CRITICAL)
    logger.info("📣 Calling request_bowler_selection...")
    await request_bowler_selection(context, group_id, match)
    logger.info("✅ request_bowler_selection completed")

async def end_innings(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    End of Innings: TV Broadcast Style Summary & Chase Equation - FIXED FOR 2ND INNINGS
    """
    if match.innings == 1:
        # --- 1. FIRST INNINGS CALCULATION ---
        bat_team = match.current_batting_team
        bowl_team = match.current_bowling_team
        
        first_innings_score = bat_team.score
        match.target = first_innings_score + 1
        
        # Calculate Run Rate
        overs_played = max(bat_team.overs, 0.1)
        rr = round(bat_team.score / overs_played, 2)

        # --- 2. INNINGS BREAK CARD ---
        msg = "🛑 <b>INNINGS BREAK</b>\n"
        msg += "─────────────────────\n"
        msg += f"🔵 <b>{bat_team.name}</b>\n"
        msg += f"📊 <b>{bat_team.score}/{bat_team.wickets}</b> ({format_overs(bat_team.balls)})\n"
        msg += f"📈 <b>Run Rate:</b> {rr}\n\n"
        
        # Top performers
        active_batters = [p for p in bat_team.players if p.balls_faced > 0 or p.is_out]
        if active_batters:
            top_scorer = max(active_batters, key=lambda p: p.runs)
            msg += f"⭐ <b>Top Scorer:</b> {top_scorer.first_name} - {top_scorer.runs} ({top_scorer.balls_faced})\n"
        
        active_bowlers = [p for p in bowl_team.players if p.balls_bowled > 0]
        if active_bowlers:
            best_bowler = max(active_bowlers, key=lambda p: (p.wickets, -p.runs_conceded))
            msg += f"⚾ <b>Best Bowler:</b> {best_bowler.first_name} - {best_bowler.wickets}/{best_bowler.runs_conceded}\n"
            
        msg += "─────────────────────\n"
        msg += f"🎯 <b>TARGET: {match.target}</b>\n"
        msg += "⏳ <i>Second innings starts in 30 seconds...</i>"
        
        gif_url = get_random_gif(MatchEvent.INNINGS_BREAK)
        try:
            if gif_url:
                await context.bot.send_animation(group_id, animation=gif_url, caption=msg, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        
        await asyncio.sleep(30)
        
        # --- 3. TEAM SWAP ---
        match.innings = 2
        match.current_batting_team = match.get_other_team(match.current_batting_team)
        match.current_bowling_team = match.get_other_team(match.current_bowling_team)
        
        # ✅ CRITICAL FIX: Reset indices for 2nd innings
        match.current_batting_team.current_batsman_idx = None
        match.current_batting_team.current_non_striker_idx = None
        match.current_bowling_team.current_bowler_idx = None
        match.current_batting_team.out_players_indices = set()
        
        logger.info("✅ 2nd innings: All indices reset to None")
        
        # --- 4. START 2ND INNINGS ---
        chase_team = match.current_batting_team
        runs_needed = match.target
        balls_available = match.total_overs * 6
        rrr = round((runs_needed / balls_available) * 6, 2)
        
        start_msg = "🚀 <b>THE CHASE BEGINS!</b>\n"
        start_msg += "─────────────────────\n"
        start_msg += f"🔴 <b>{chase_team.name}</b> needs to chase.\n\n"
        start_msg += "📊 <b>WINNING EQUATION:</b>\n"
        start_msg += f"🎯 <b>Need {runs_needed} runs</b>\n"
        start_msg += f"⚾ <b>In {balls_available} balls</b>\n"
        start_msg += f"📈 <b>Required RR: {rrr}</b>\n\n"
        start_msg += "🤞 <i>Good luck to both teams!</i>"
        
        await context.bot.send_message(group_id, start_msg, parse_mode=ParseMode.HTML)
        await asyncio.sleep(2)
        
        # ✅ Request Openers (Both striker and non-striker)
        match.waiting_for_batsman = True
        match.waiting_for_bowler = False
        
        captain = match.get_captain(chase_team)
        captain_tag = get_user_tag(captain)
        
        msg = f"🏏 <b>SELECT STRIKER</b>\n"
        msg += f"─────────────────────\n"
        msg += f"🧢 <b>{captain_tag}</b>, please select the <b>STRIKER</b> first:\n\n"
        msg += f"👉 <b>Command:</b> <code>/batting [serial_number]</code>\n"
        msg += f"📋 <b>Available Players:</b> {len(chase_team.players)}\n"
        
        await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        
        match.batsman_selection_time = time.time()
        match.batsman_selection_task = asyncio.create_task(
            batsman_selection_timeout(context, group_id, match)
        )
        
    else:
        # Second innings complete
        await determine_match_winner(context, group_id, match)

async def determine_match_winner(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Determine winner with proper stats update"""
    first = match.batting_first
    second = match.get_other_team(first)
    
    winner = None
    loser = None
    margin = ""
    
    if second.score >= match.target:
        winner = second
        loser = first
        wickets_left = len(second.players) - second.wickets - len(second.out_players_indices)
        margin = f"{wickets_left} Wickets"
    elif first.score > second.score:
        winner = first
        loser = second
        margin = f"{first.score - second.score} Runs"
    else:
        await context.bot.send_message(group_id, "🤝 <b>MATCH TIED!</b>", parse_mode=ParseMode.HTML)
        # Update stats for tied match
        await update_player_stats_after_match(match, None, None)
        save_match_to_history(match, "TIE", "TIE")
        del active_matches[group_id]
        return

    # ✅ UPDATE PLAYER STATS
    await update_player_stats_after_match(match, winner, loser)
    
    # ✅ SAVE MATCH TO HISTORY
    save_match_to_history(match, winner.name, loser.name)
    
    # Send Victory Message with GIF
    await send_victory_message(context, group_id, match, winner, loser, margin)
    
    # Send Player of the Match
    await send_potm_message(context, group_id, match)
    
    # Cleanup
    try:
        if match.main_message_id:
            await context.bot.unpin_chat_message(chat_id=group_id, message_id=match.main_message_id)
    except: pass
    
    del active_matches[group_id]

async def determine_match_winner(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """✅ FIXED: Victory + POTM messages guaranteed to send"""
    first = match.batting_first
    second = match.get_other_team(first)
    
    winner = None
    loser = None
    margin = ""
    
    if second.score >= match.target:
        winner = second
        loser = first
        wickets_left = len(second.players) - second.wickets - len(second.out_players_indices)
        margin = f"{wickets_left} Wickets"
    elif first.score > second.score:
        winner = first
        loser = second
        margin = f"{first.score - second.score} Runs"
    else:
        # TIE
        await context.bot.send_message(group_id, "🤝 <b>MATCH TIED!</b>", parse_mode=ParseMode.HTML)
        await update_player_stats_after_match(match, None, None)
        save_match_to_history(match, "TIE", "TIE")
        
        # Cleanup
        try:
            if match.main_message_id:
                await context.bot.unpin_chat_message(chat_id=group_id, message_id=match.main_message_id)
        except: pass
        
        del active_matches[group_id]
        return

    # ✅ UPDATE STATS
    await update_player_stats_after_match(match, winner, loser)
    
    # ✅ SAVE MATCH
    save_match_to_history(match, winner.name, loser.name)
    
    # ✅ STEP 1: SEND VICTORY MESSAGE (GUARANTEED)
    logger.info("🎊 Sending Victory Message...")
    try:
        await send_victory_message(context, group_id, match, winner, loser, margin)
        await asyncio.sleep(3)
        logger.info("✅ Victory message sent")
    except Exception as e:
        logger.error(f"❌ Victory message failed: {e}")
        # Fallback simple message
        await context.bot.send_message(
            group_id,
            f"🏆 <b>{winner.name} WON!</b>\nBy {margin}",
            parse_mode=ParseMode.HTML
        )
    
    # ✅ STEP 2: SEND POTM MESSAGE (GUARANTEED)
    logger.info("🌟 Sending POTM Message...")
    try:
        await send_potm_message(context, group_id, match)
        logger.info("✅ POTM message sent")
    except Exception as e:
        logger.error(f"❌ POTM message failed: {e}")
        # Fallback
        await context.bot.send_message(
            group_id,
            "🌟 <b>PLAYER OF THE MATCH</b>\nCongratulations to all players!",
            parse_mode=ParseMode.HTML
        )
    
    # ✅ CLEANUP
    try:
        if match.main_message_id:
            await context.bot.unpin_chat_message(chat_id=group_id, message_id=match.main_message_id)
    except: pass
    
    del active_matches[group_id]
    logger.info("🏁 Match ended successfully")


async def start_super_over(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Start Super Over for tied match"""
    super_over_text = "Match Tied\n\n"
    super_over_text += f"Both teams scored {match.current_batting_team.score} runs!\n\n"
    super_over_text += "SUPER OVER\n\n"
    super_over_text += "Each team will play 1 over.\n"
    super_over_text += "Higher score wins.\n"
    super_over_text += "No DRS in Super Over.\n\n"
    super_over_text += "Starting in 10 seconds..."
    
    await context.bot.send_message(
        chat_id=group_id,
        text=super_over_text
    )
    
    await asyncio.sleep(10)
    
    match.is_super_over = True
    match.phase = GamePhase.SUPER_OVER
    
    # Reset for super over (to be continued in next part)
    # This will be implemented in Part 9

async def send_match_summary(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, winner: Team, loser: Team):
    """Send detailed match summary with player statistics"""
    
    # Batting summary for both teams
    summary_text = "Match Summary\n"
    summary_text += "=" * 40 + "\n\n"
    
    # First innings batting
    summary_text += f"{match.batting_first.name} Batting\n"
    summary_text += "-" * 40 + "\n"
    for player in match.batting_first.players:
        if player.balls_faced > 0 or player.is_out:
            status = "out" if player.is_out else "not out"
            sr = player.get_strike_rate()
            summary_text += f"{player.first_name}: {player.runs} ({player.balls_faced}) - {status}"
            if player.boundaries > 0:
                summary_text += f" [{player.boundaries}x4"
                if player.sixes > 0:
                    summary_text += f", {player.sixes}x6"
                summary_text += "]"
            summary_text += f" SR: {sr}\n"
    
    summary_text += f"\nTotal: {match.batting_first.score}/{match.batting_first.wickets}\n"
    summary_text += f"Overs: {format_overs(match.batting_first.balls)}\n"
    summary_text += f"Extras: {match.batting_first.extras}\n\n"
    
    # Second innings batting
    summary_text += f"{match.bowling_first.name} Batting\n"
    summary_text += "-" * 40 + "\n"
    for player in match.bowling_first.players:
        if player.balls_faced > 0 or player.is_out:
            status = "out" if player.is_out else "not out"
            sr = player.get_strike_rate()
            summary_text += f"{player.first_name}: {player.runs} ({player.balls_faced}) - {status}"
            if player.boundaries > 0:
                summary_text += f" [{player.boundaries}x4"
                if player.sixes > 0:
                    summary_text += f", {player.sixes}x6"
                summary_text += "]"
            summary_text += f" SR: {sr}\n"
    
    summary_text += f"\nTotal: {match.bowling_first.score}/{match.bowling_first.wickets}\n"
    summary_text += f"Overs: {format_overs(match.bowling_first.balls)}\n"
    summary_text += f"Extras: {match.bowling_first.extras}\n\n"
    
    await context.bot.send_message(
        chat_id=group_id,
        text=summary_text
    )
    
    await asyncio.sleep(1)
    
    # Bowling summary
    bowling_summary = "Bowling Figures\n"
    bowling_summary += "=" * 40 + "\n\n"
    
    # First innings bowling
    bowling_summary += f"{match.bowling_first.name} Bowling\n"
    bowling_summary += "-" * 40 + "\n"
    for player in match.bowling_first.players:
        if player.balls_bowled > 0:
            overs = format_overs(player.balls_bowled)
            economy = player.get_economy()
            bowling_summary += f"{player.first_name}: {overs} overs, {player.wickets}/{player.runs_conceded}"
            bowling_summary += f" Econ: {economy}"
            if player.maiden_overs > 0:
                bowling_summary += f" M: {player.maiden_overs}"
            bowling_summary += "\n"
    
    bowling_summary += "\n"
    
    # Second innings bowling
    bowling_summary += f"{match.batting_first.name} Bowling\n"
    bowling_summary += "-" * 40 + "\n"
    for player in match.batting_first.players:
        if player.balls_bowled > 0:
            overs = format_overs(player.balls_bowled)
            economy = player.get_economy()
            bowling_summary += f"{player.first_name}: {overs} overs, {player.wickets}/{player.runs_conceded}"
            bowling_summary += f" Econ: {economy}"
            if player.maiden_overs > 0:
                bowling_summary += f" M: {player.maiden_overs}"
            bowling_summary += "\n"
    
    await context.bot.send_message(
        chat_id=group_id,
        text=bowling_summary
    )
    
    await asyncio.sleep(1)
    
    # Player of the Match
    potm_text = "Player of the Match\n"
    potm_text += "=" * 40 + "\n\n"
    
    # Calculate POTM based on performance
    all_players = match.batting_first.players + match.bowling_first.players
    best_player = None
    best_score = 0
    
    for player in all_players:
        # Score calculation: runs + (wickets * 20) + (boundaries * 2)
        performance_score = player.runs + (player.wickets * 20) + (player.boundaries * 2)
        if performance_score > best_score:
            best_score = performance_score
            best_player = player
    
    if best_player:
        potm_text += f"{best_player.first_name}\n\n"
        if best_player.balls_faced > 0:
            potm_text += f"Batting: {best_player.runs} ({best_player.balls_faced}) SR: {best_player.get_strike_rate()}\n"
        if best_player.balls_bowled > 0:
            potm_text += f"Bowling: {best_player.wickets}/{best_player.runs_conceded} Econ: {best_player.get_economy()}\n"
    
    await context.bot.send_message(
        chat_id=group_id,
        text=potm_text
    )

async def update_player_stats_after_match(match: Match, winner: Team, loser: Team):
    """Update global player statistics after match - FIXED"""
    all_players = match.batting_first.players + match.bowling_first.players
    
    for player in all_players:
        user_id = player.user_id
        
        # Initialize if needed
        if user_id not in player_stats:
            init_player_stats(user_id)
        
        stats = player_stats[user_id]
        
        # Update match count
        stats["matches_played"] += 1
        
        # Check if winner (handle tied match)
        if winner:
            is_winner = (player in winner.players)
            if is_winner:
                stats["matches_won"] += 1
        
        # Update batting stats
        if player.balls_faced > 0:
            stats["total_runs"] += player.runs
            stats["total_balls_faced"] += player.balls_faced
            stats["dot_balls_faced"] += player.dot_balls_faced
            stats["boundaries"] += getattr(player, 'boundaries', 0)
            stats["sixes"] += getattr(player, 'sixes', 0)
            
            # Check for century/half-century
            if player.runs >= 100:
                stats["centuries"] += 1
            elif player.runs >= 50:
                stats["half_centuries"] += 1
            
            # Update highest score
            if player.runs > stats["highest_score"]:
                stats["highest_score"] = player.runs
            
            # Check for duck
            if player.runs == 0 and player.is_out:
                stats["ducks"] += 1
            
            # Update last 5 scores
            stats["last_5_scores"].append(player.runs)
            if len(stats["last_5_scores"]) > 5:
                stats["last_5_scores"].pop(0)
        
        # Update bowling stats
        if player.balls_bowled > 0:
            stats["total_wickets"] += player.wickets
            stats["total_balls_bowled"] += player.balls_bowled
            stats["total_runs_conceded"] += player.runs_conceded
            stats["dot_balls_bowled"] += player.dot_balls_bowled
            stats["total_no_balls"] += player.no_balls
            stats["total_wides"] += player.wides
            
            # Update best bowling
            if player.wickets > stats["best_bowling"]["wickets"]:
                stats["best_bowling"]["wickets"] = player.wickets
                stats["best_bowling"]["runs"] = player.runs_conceded
            elif player.wickets == stats["best_bowling"]["wickets"] and player.runs_conceded < stats["best_bowling"]["runs"]:
                stats["best_bowling"]["runs"] = player.runs_conceded
            
            # Update last 5 wickets
            stats["last_5_wickets"].append(player.wickets)
            if len(stats["last_5_wickets"]) > 5:
                stats["last_5_wickets"].pop(0)
        
        # Update timeouts
        stats["total_timeouts"] += player.batting_timeouts + player.bowling_timeouts
    
    # Save to disk
    save_data()

async def send_potm_message(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    ✅ FIXED: Guaranteed POTM Message with Error Handling
    """
    
    logger.info("🌟 Building POTM message...")
    
    try:
        # 1. Calculate Best Player
        all_players = match.batting_first.players + match.bowling_first.players
        best_player = None
        best_score = -1

        for p in all_players:
            score = p.runs + (p.wickets * 20) + (p.boundaries * 2) + (p.sixes * 3)
            if score > best_score:
                best_score = score
                best_player = p

        if not best_player:
            logger.warning("⚠️ No best player found")
            await context.bot.send_message(
                group_id,
                "🌟 <b>PLAYER OF THE MATCH</b>\nAll players performed well!",
                parse_mode=ParseMode.HTML
            )
            return

        # 2. Get GIF
        try:
            potm_gif = "https://t.me/cricoverse/51"
        except:
            potm_gif = None
        
        player_tag = get_user_tag(best_player)
        
        # 3. Build Message
        msg = f"🌟 <b>PLAYER OF THE MATCH</b> 🌟\n"
        msg += f"─────────────────────\n\n"
        msg += f"🏅 <b>{player_tag}</b>\n"
        
        # Impact Summary
        impact_notes = []
        if best_player.runs >= 50: impact_notes.append("Half Century")
        elif best_player.runs >= 30: impact_notes.append("Top Scorer")
        
        if best_player.wickets >= 3: impact_notes.append("3 Wicket Haul")
        elif best_player.wickets >= 2: impact_notes.append("Key Wickets")
        
        if best_player.runs > 20 and best_player.wickets > 0:
            impact_notes = ["All-Round Performance"]

        if impact_notes:
            msg += f"🔥 <i>{', '.join(impact_notes)}</i>\n"
        msg += "\n"

        # Batting Stats
        if best_player.balls_faced > 0:
            sr = best_player.get_strike_rate()
            msg += f"🏏 <b>{best_player.runs}</b> runs ({best_player.balls_faced} balls)\n"
            msg += f"   💥 {best_player.boundaries} fours, {best_player.sixes} sixes\n"
            msg += f"   ⚡ SR: {sr}\n\n"

        # Bowling Stats
        if best_player.balls_bowled > 0:
            econ = best_player.get_economy()
            msg += f"⚾ <b>{best_player.wickets}</b> wickets for <b>{best_player.runs_conceded}</b> runs\n"
            msg += f"   🎯 Overs: {format_overs(best_player.balls_bowled)}\n"
            msg += f"   📉 Econ: {econ}\n"

        msg += "─────────────────────\n"
        msg += "👏 <i>Well Played!</i>"

        # 4. Send
        try:
            if potm_gif:
                await context.bot.send_animation(
                    group_id,
                    animation=potm_gif,
                    caption=msg,
                    parse_mode=ParseMode.HTML
                )
            else:
                await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            logger.info("✅ POTM message sent successfully")
        except Exception as send_error:
            logger.error(f"❌ POTM send error: {send_error}")
            # Fallback
            await context.bot.send_message(
                group_id,
                f"🌟 <b>PLAYER OF THE MATCH:</b> {best_player.first_name}",
                parse_mode=ParseMode.HTML
            )
            
    except Exception as e:
        logger.error(f"❌ POTM function error: {e}")
        await context.bot.send_message(
            group_id,
            "🌟 <b>PLAYER OF THE MATCH</b>\nCheck /scorecard for details!",
            parse_mode=ParseMode.HTML
        )


async def send_victory_message(
    context: ContextTypes.DEFAULT_TYPE,
    group_id: int,
    match: Match,
    winner: Team,
    loser: Team,
    margin: str
):
    """
    ✅ FIXED: Guaranteed Victory Message with Error Handling
    """
    
    logger.info("🎉 Building victory message...")
    
    try:
        victory_gif = get_random_gif(MatchEvent.VICTORY)
    except:
        victory_gif = None

    # --- Helper Function ---
    def get_stat_star(team: Team, role: str):
        try:
            if role == "bat":
                active_players = [p for p in team.players if p.balls_faced > 0 or p.is_out]
                if not active_players: return None
                best = max(active_players, key=lambda p: (p.runs, p.get_strike_rate()))
                return f"{best.first_name} {best.runs}({best.balls_faced})"
            
            elif role == "bowl":
                active_bowlers = [p for p in team.players if p.balls_bowled > 0]
                if not active_bowlers: return None
                best = max(active_bowlers, key=lambda p: (p.wickets, -p.runs_conceded))
                return f"{best.first_name} {best.wickets}/{best.runs_conceded} ({format_overs(best.balls_bowled)})"
        except:
            return None
        return None

    # --- BUILD MESSAGE ---
    msg = f"🏆 <b>{winner.name} ARE CHAMPIONS!</b> 🏆\n"
    msg += f"─────────────────────\n"
    msg += f"✅ <b>Result:</b> Won by {margin}\n"
    
    if winner == match.batting_first:
        msg += f"🛡️ <b>Defended:</b> {match.target - 1} Runs\n\n"
    else:
        msg += f"🚀 <b>Chased:</b> {match.target} Runs\n\n"

    # Team 1
    t1 = match.batting_first
    t1_rr = round(t1.score / max(t1.overs, 0.1), 2)
    t1_star = get_stat_star(t1, "bat")
    
    msg += f"1️⃣ <b>{t1.name}</b> (1st Inn)\n"
    msg += f"   🎯 <b>{t1.score}/{t1.wickets}</b> in {format_overs(t1.balls)} ov (RR: {t1_rr})\n"
    if t1_star: msg += f"   🏏 Top Bat: <i>{t1_star}</i>\n"
    msg += "\n"

    # Team 2
    t2 = match.bowling_first
    t2_rr = round(t2.score / max(t2.overs, 0.1), 2)
    t2_star = get_stat_star(t2, "bat")
    
    msg += f"2️⃣ <b>{t2.name}</b> (2nd Inn)\n"
    msg += f"   🎯 <b>{t2.score}/{t2.wickets}</b> in {format_overs(t2.balls)} ov (RR: {t2_rr})\n"
    if t2_star: msg += f"   🏏 Top Bat: <i>{t2_star}</i>\n"
    
    msg += f"─────────────────────\n"

    # Bowling Hero
    bowling_hero = get_stat_star(winner, "bowl")
    if bowling_hero:
        msg += f"💣 <b>Match Turning Spell:</b>\n"
        msg += f"   ⚾ {bowling_hero}\n"

    msg += f"\n👏 <i>Congratulations {winner.name}!</i>"

    # ✅ SEND WITH MULTIPLE FALLBACKS
    try:
        if victory_gif:
            await context.bot.send_animation(
                chat_id=group_id,
                animation=victory_gif,
                caption=msg,
                parse_mode=ParseMode.HTML
            )
        else:
            await context.bot.send_message(
                chat_id=group_id,
                text=msg,
                parse_mode=ParseMode.HTML
            )
        logger.info("✅ Victory message sent successfully")
    except Exception as e:
        logger.error(f"❌ Victory message error: {e}")
        # Final fallback - simple text
        await context.bot.send_message(
            group_id,
            f"🏆 <b>{winner.name} WON by {margin}!</b>",
            parse_mode=ParseMode.HTML
        )

def update_h2h_stats(match: Match):
    """
    Update Head-to-Head stats using REAL match data
    """

    all_players = []

    all_players.extend(match.team_x.players)
    all_players.extend(match.team_y.players)

    for p1 in all_players:
        init_player_stats(p1.user_id)

        for p2 in all_players:
            if p1.user_id == p2.user_id:
                continue

            vs = player_stats[p1.user_id].setdefault("vs_player_stats", {})
            record = vs.setdefault(str(p2.user_id), {
                "matches": 0,
                "runs_scored": 0,
                "balls_faced": 0,
                "dismissals": 0,
                "wickets_taken": 0
            })

            # Played together in same match
            record["matches"] += 1

            # Batting vs opponent
            record["runs_scored"] += p1.runs
            record["balls_faced"] += p1.balls_faced

            # If p1 got out & p2 was bowler
            if p1.is_out and match.current_bowling_team.get_player(p2.user_id):
                record["dismissals"] += 1

            # Bowling vs opponent
            record["wickets_taken"] += p1.wickets

    save_data()


def check_achievements(player: Player):
    """Check and award achievements to player"""
    user_id = player.user_id
    stats = player_stats.get(user_id)
    
    if not stats:
        return
    
    if user_id not in achievements:
        achievements[user_id] = []
    
    user_achievements = achievements[user_id]
    
    # Century Maker
    if stats["centuries"] >= 1 and "Century Maker" not in user_achievements:
        user_achievements.append("Century Maker")
    
    # Hat-trick Hero (3 wickets in match)
    if player.wickets >= 3 and "Hat-trick Hero" not in user_achievements:
        user_achievements.append("Hat-trick Hero")
    
    # Diamond Hands (50+ matches)
    if stats["matches_played"] >= 50 and "Diamond Hands" not in user_achievements:
        user_achievements.append("Diamond Hands")
    
    # Speed Demon (Strike Rate > 200 in a match with 10+ runs)
    if player.runs >= 10 and player.get_strike_rate() > 200 and "Speed Demon" not in user_achievements:
        user_achievements.append("Speed Demon")
    
    # Economical (Economy < 5 in a match with 12+ balls bowled)
    if player.balls_bowled >= 12 and player.get_economy() < 5 and "Economical" not in user_achievements:
        user_achievements.append("Economical")

def save_match_to_history(match: Match, winner_name: str, loser_name: str):
    """Save match details to history"""
    match_record = {
        "match_id": match.match_id,
        "group_id": match.group_id,
        "group_name": match.group_name,
        "date": match.created_at.isoformat(),
        "overs": match.total_overs,
        "winner": winner_name,
        "loser": loser_name,
        "team_x_score": match.team_x.score,
        "team_x_wickets": match.team_x.wickets,
        "team_y_score": match.team_y.score,
        "team_y_wickets": match.team_y.wickets,
        "total_balls": len(match.ball_by_ball_log)
    }
    
    match_history.append(match_record)
    
    # Update group stats
    if match.group_id in registered_groups:
        registered_groups[match.group_id]["total_matches"] += 1
    
    save_data()

# Stats commands
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Beautiful stats display - IMPROVED"""
    user = update.effective_user
    
    if user.id not in player_stats:
        await update.message.reply_text(
            "❌ <b>No Career Data Found</b>\n\n"
            "You haven't played any matches yet.\n"
            "Join a game with /game to start! 🎮",
            parse_mode=ParseMode.HTML
        )
        return
    
    stats = player_stats[user.id]
    
    matches_played = stats["matches_played"]
    matches_won = stats["matches_won"]
    win_rate = (matches_won / matches_played * 100) if matches_played > 0 else 0
    
    total_runs = stats["total_runs"]
    total_balls = stats["total_balls_faced"]
    avg_runs = (total_runs / matches_played) if matches_played > 0 else 0
    strike_rate = (total_runs / total_balls * 100) if total_balls > 0 else 0
    
    total_wickets = stats["total_wickets"]
    total_balls_bowled = stats["total_balls_bowled"]
    total_conceded = stats["total_runs_conceded"]
    bowling_avg = (total_conceded / total_wickets) if total_wickets > 0 else 0
    economy = (total_conceded / (total_balls_bowled / 6)) if total_balls_bowled > 0 else 0
    
    user_tag = get_user_tag(user)
    
    msg = f"📊 <b>CRICOVERSE STATISTICS</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"👤 {user_tag}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    msg += f"🏆 <b>CAREER OVERVIEW</b>\n"
    msg += f"• Matches Played: <b>{matches_played}</b>\n"
    msg += f"• Matches Won: <b>{matches_won}</b>\n"
    msg += f"• Win Rate: <b>{win_rate:.1f}%</b>\n\n"
    
    msg += f"🏏 <b>BATTING</b>\n"
    msg += f"<code>Runs   : {total_runs:<6} Avg: {avg_runs:.2f}</code>\n"
    msg += f"<code>Balls  : {total_balls:<6} SR : {strike_rate:.1f}</code>\n"
    msg += f"<code>HS     : {stats['highest_score']:<6} Ducks: {stats['ducks']}</code>\n"
    msg += f"<code>100s   : {stats['centuries']:<6} 50s: {stats['half_centuries']}</code>\n"
    msg += f"💥 Boundaries: {stats['boundaries']} 4s | {stats['sixes']} 6s\n\n"
    
    msg += f"⚾ <b>BOWLING</b>\n"
    msg += f"<code>Wkts   : {total_wickets:<6} Avg: {bowling_avg:.2f}</code>\n"
    msg += f"<code>Balls  : {total_balls_bowled:<6} Eco: {economy:.2f}</code>\n"
    msg += f"<code>Best   : {stats['best_bowling']['wickets']}/{stats['best_bowling']['runs']}</code>\n"
    msg += f"🎯 Dot Balls: {stats['dot_balls_bowled']}\n\n"
    
    # Recent form
    last_5 = stats.get("last_5_scores", [])
    if last_5:
        form = "  ".join(f"<b>{x}</b>" for x in reversed(last_5))
        msg += f"📉 <b>RECENT FORM</b>\n{form}\n\n"
    
    msg += "━━━━━━━━━━━━━━━━━━━━━━━"
    
    try:
        await update.message.reply_photo(
            photo=MEDIA_ASSETS.get("stats"),
            caption=msg,
            parse_mode=ParseMode.HTML
        )
    except:
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
async def mystats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    FIFA Ultimate Team Style Player Card (Main Command)
    """
    user = update.effective_user
    
    # Init if new
    if user.id not in player_stats: init_player_stats(user.id)
    
    # Combine Solo + Team stats for "Career Card"
    t_stats = player_stats[user.id].get("team", {})
    s_stats = player_stats[user.id].get("solo", {})
    
    combined_stats = {
        "matches": t_stats.get("matches", 0) + s_stats.get("matches", 0),
        "runs": t_stats.get("runs", 0) + s_stats.get("runs", 0),
        "balls": t_stats.get("balls", 0) + s_stats.get("balls", 0),
        "wickets": t_stats.get("wickets", 0) + s_stats.get("wickets", 0),
        "balls_bowled": t_stats.get("balls_bowled", 0),
        "centuries": t_stats.get("centuries", 0),
        "fifties": t_stats.get("fifties", 0)
    }

    # Calculate FIFA Ratings
    attr = calculate_fifa_attributes(combined_stats)
    ovr = attr["OVR"]
    card_type, color_dot = get_card_design(ovr)
    user_tag = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
    
    # Determine Position
    if attr["DEF"] > attr["SHO"] + 10: pos = "BWL" # Bowler
    elif attr["SHO"] > attr["DEF"] + 10: pos = "BAT" # Batsman
    else: pos = "AR" # All Rounder

    # Market Value (Fun Metric)
    mkt_val = (ovr * combined_stats["matches"] * 1000) + (combined_stats["runs"] * 500)
    if mkt_val > 10000000: val_fmt = f"€{mkt_val/10000000:.1f}M"
    elif mkt_val > 10000: val_fmt = f"€{mkt_val/1000:.1f}K"
    else: val_fmt = f"€{mkt_val}"

    # --- THE VISUAL CARD ---
    msg = f"<b>{color_dot} CRICOVERSE ULTIMATE TEAM {color_dot}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"👤 <b>{user_tag}</b>\n"
    msg += f"🏳️ <i>India</i>  |  🏟 <i>Cricoverse FC</i>\n\n"
    
    # The Big Rating
    msg += f"🔥 <b>{ovr}</b> {pos}      {card_type}\n"
    msg += "──────────────\n"
    
    # Attributes Grid (Aligned with Full Names)
    msg += f"🚀 <b>BATTING POW: {attr['SHO']}</b> {draw_bar(attr['SHO'])}\n"
    msg += f"⚡ <b>STRIKE RATE: {attr['PAC']}</b> {draw_bar(attr['PAC'])}\n"
    msg += f"🛡 <b>BOWLING ABI: {attr['DEF']}</b> {draw_bar(attr['DEF'])}\n"
    msg += f"🧠 <b>LEADER-SHIP: {attr['PAS']}</b> {draw_bar(attr['PAS'])}\n"
    msg += f"🕹 <b> TECHNIQUE : {attr['DRI']}</b> {draw_bar(attr['DRI'])}\n"
    msg += f"💪 <b> WORKLOAD  : {attr['PHY']}</b> {draw_bar(attr['PHY'])}\n"
    msg += "──────────────\n\n"
    
    # Detailed Info
    msg += f"💰 <b>Value:</b> {val_fmt}\n"
    msg += f"🧠 <b>Chemistry:</b> 10/10\n"
    msg += f"👟 <b>Work Rate:</b> High/High\n"
    
    # Traits
    traits = []
    if combined_stats["runs"] > 500: traits.append("Finesse Shot")
    if attr["PAC"] > 85: traits.append("Speedster")
    if combined_stats["wickets"] > 20: traits.append("Giant Throw")
    if not traits: traits.append("Basic Chemistry")
    
    msg += f"✨ <b>Traits:</b> {', '.join(traits)}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<i>Select 'Scout Report' for raw data.</i>"

    # Buttons for Detailed Stats
    keyboard = [
        [InlineKeyboardButton("📊 Scout Report: TEAM", callback_data=f"stats_view_team_{user.id}")],
        [InlineKeyboardButton("⚔️ Scout Report: SOLO", callback_data=f"stats_view_solo_{user.id}")]
    ]
    
    # Send
    await update.message.reply_photo(
        photo=MEDIA_ASSETS.get("stats", "https://t.me/cricoverse/11"),
        caption=msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

async def stats_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Robust Stats View: Handles Caption Limits & HTML Errors safely
    """
    query = update.callback_query
    await query.answer()
    
    try:
        data = query.data.split("_")
        mode = data[2] # team / solo
        target_id = int(data[3])
    except: return

    if target_id not in player_stats: init_player_stats(target_id)
    stats = player_stats[target_id].get(mode, {})
    
    # Secure User Info (HTML Escape to prevent crashes)
    try:
        chat_member = await context.bot.get_chat_member(query.message.chat.id, target_id)
        user = chat_member.user
        # html.escape is crucial for names with < > &
        clean_name = html.escape(user.first_name)
        user_tag = f"<a href='tg://user?id={user.id}'>{clean_name}</a>"
    except: 
        user_tag = "Unknown Player"

    # Get Attributes
    attr = calculate_fifa_attributes(stats, mode)
    ovr = attr["OVR"]
    
    # Card Class
    if ovr >= 90: card, dot = "ICON", "💎"
    elif ovr >= 85: card, dot = "LEGEND", "🟣"
    elif ovr >= 75: card, dot = "PRO", "🟡"
    else: card, dot = "ROOKIE", "⚪"
    
    if attr['DEF'] > attr['SHO'] + 10: pos = "BOWLER"
    elif attr['SHO'] > attr['DEF'] + 10: pos = "BATTER"
    else: pos = "ALL-ROUNDER"

    # --- TEXT GENERATION ---
    text = f"<b>{dot} PRO PLAYER CARD {dot}</b>\n"
    text += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"👤 <b>{user_tag}</b>\n"
    text += f"🏳️ <i>Cricoverse XI</i>\n\n"
    
    text += f"🔥 <b>{ovr}</b> {pos}  |  {card}\n"
    text += f"──────────────\n"
    
    # Attributes
    text += f"🚀 <b>BATTING POW: {attr['SHO']}</b> {draw_bar(attr['SHO'])}\n"
    text += f"⚡ <b>STRIKE RATE: {attr['PAC']}</b> {draw_bar(attr['PAC'])}\n"
    text += f"🛡 <b>BOWLING ABI: {attr['DEF']}</b> {draw_bar(attr['DEF'])}\n"
    text += f"🧠 <b>LEADER-SHIP: {attr['PAS']}</b> {draw_bar(attr['PAS'])}\n"
    text += f"🕹 <b> TECHNIQUE : {attr['DRI']}</b> {draw_bar(attr['DRI'])}\n"
    text += f"💪 <b> WORKLOAD  : {attr['PHY']}</b> {draw_bar(attr['PHY'])}\n"
    text += f"──────────────\n"
    
    # Detailed Stats
    if mode == "team":
        matches = stats.get("matches", 0)
        runs = stats.get("runs", 0)
        wickets = stats.get("wickets", 0)
        
        avg = round(runs/matches, 2) if matches else 0
        sr = round(runs/stats.get("balls", 1)*100, 1)
        
        balls_bowled = stats.get("balls_bowled", 0)
        overs = f"{balls_bowled // 6}.{balls_bowled % 6}"
        econ = round(stats.get("runs_conceded", 0) / max(1, balls_bowled/6), 2)
        
        text += f"🏏 <b>BATTING PERFORMANCE</b>\n"
        text += f"• <b>Runs:</b> {runs}\n"
        text += f"• <b>Avg:</b> {avg}\n"
        text += f"• <b>Strike Rate:</b> {sr}\n"
        text += f"• <b>Highest:</b> {stats.get('highest', 0)}*\n"
        text += f"• <b>100s/50s:</b> {stats.get('centuries', 0)} / {stats.get('fifties', 0)}\n\n"
        
        text += f"⚾ <b>BOWLING PERFORMANCE</b>\n"
        text += f"• <b>Wickets:</b> {wickets}  (Econ: {econ})\n"
        text += f"• <b>Economy:</b> {econ}\n"
        text += f"• <b>Overs:</b> {overs}\n"
        text += f"• <b>Hat-Tricks:</b> {stats.get('hat_tricks', 0)}\n\n"
        
        text += f"🏆 <b>CAREER RECORDS</b>\n"
        text += f"• <b>Matches:</b> {matches}\n"
        text += f"• <b>MOM Awards:</b> {stats.get('mom', 0)}\n"
        text += f"• <b>Captaincy:</b> {stats.get('captain_wins', 0)} Wins\n"

    else:
        wins = stats.get("wins", 0)
        matches = stats.get("matches", 0)
        win_rate = round((wins/matches)*100, 1) if matches else 0
        
        text += f"⚔️ <b>SURVIVAL STATS</b>\n"
        text += f"• <b>Total Runs:</b> {stats.get('runs', 0)}\n"
        text += f"• <b>Balls Faced:</b> {stats.get('balls', 0)}\n"
        text += f"• <b>High Score:</b> {stats.get('highest', 0)}\n"
        text += f"• <b>K.O.s (Wkts):</b> {stats.get('wickets', 0)}\n\n"
        
        text += f"🏆 <b>BATTLE RECORD</b>\n"
        text += f"• <b>Matches:</b> {matches}\n"
        text += f"• <b>Wins:</b> {wins} ({win_rate}%)\n"
        text += f"• <b>Podiums:</b> {stats.get('top_3_finishes', 0)}\n"

    text += f"━━━━━━━━━━━━━━━━━━━━━━"

    keyboard = [[InlineKeyboardButton("🔙 Return to Main", callback_data=f"stats_main_{target_id}")]]
    
    # --- SMART ERROR HANDLING ---
    try:
        # Try editing caption (Preferred: Keeps Image)
        await query.message.edit_caption(caption=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except Exception as e:
        # If caption fails (Too Long or HTML Error), Switch to Text Mode safely
        try:
            await query.message.delete() # Delete old photo message
            await context.bot.send_message(
                chat_id=query.message.chat_id, 
                text=text, 
                reply_markup=InlineKeyboardMarkup(keyboard), 
                parse_mode=ParseMode.HTML
            )
        except:
            # Fallback for weird edge cases
            await context.bot.send_message(chat_id=query.message.chat_id, text="⚠️ Error displaying detailed stats.", parse_mode=ParseMode.HTML)

# Handle "Main Menu" Back Button
async def stats_main_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Returns to Main Card (Robust Handler)"""
    query = update.callback_query
    await query.answer()
    
    try:
        target_id = int(query.data.split("_")[2])
    except: return

    # Trigger Main Command Logic manually
    # We call the logic of mystats_command but adapt it for editing
    
    if target_id not in player_stats: init_player_stats(target_id)
    user = query.from_user # Note: This might be viewer, not target. 
    # Ideally fetch target user info, but for now we regenerate layout
    
    # ... (Re-calculate global stats/card logic here if needed, 
    # OR simpler: Just delete and call mystats_command logic if possible)
    
    # Since mystats_command is complex, let's just delete and ask user to use /mystats
    # OR better: Re-send the Photo Card.
    
    # Check if we can edit caption (Is it a photo?)
    if query.message.photo:
        # Just call the mystats logic to generate text
        # For simplicity, let's just tell user to click /mystats or re-send image
        await query.message.delete()
        
        # Trigger mystats logic logic (Simulated)
        # You can actually just call `mystats_command` if you update `update.message` to `query.message`
        # But cleanest way: Send new photo
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=MEDIA_ASSETS.get("stats", "https://t.me/cricoverse/11"),
            caption="🔄 <b>Reloading Card...</b>\nPlease type /mystats to refresh full profile.",
            parse_mode=ParseMode.HTML
        )
    else:
        # It was text (fallback), so delete and send photo
        await query.message.delete()
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=MEDIA_ASSETS.get("stats", "https://t.me/cricoverse/11"),
            caption="🔄 <b>Reloading...</b>\nUse /mystats for the main card.",
            parse_mode=ParseMode.HTML
        )


async def h2h_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Advanced Head-to-Head Stats (Genuine Data Only)"""

    user = update.effective_user

    # 🎯 Detect opponent
    target_user = None
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
    elif context.args:
        username = context.args[0].replace("@", "").lower()
        for uid, data in user_data.items():
            if data.get("username", "").lower() == username:
                target_user = type(
                    "User", (), {"id": uid, "first_name": data["first_name"]}
                )
                break

    if not target_user:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b>\nReply to a player or use <code>/h2h @username</code>",
            parse_mode=ParseMode.HTML
        )
        return

    # Init stats
    init_player_stats(user.id)
    init_player_stats(target_user.id)

    vs_data = player_stats[user.id].get("vs_player_stats", {}).get(str(target_user.id))
    if not vs_data:
        await update.message.reply_text(
            "📉 <b>No H2H record found yet.</b>\nPlay some matches together!",
            parse_mode=ParseMode.HTML
        )
        return

    # 📊 Calculations
    matches = vs_data["matches"]
    runs = vs_data["runs_scored"]
    balls = vs_data["balls_faced"]
    outs = vs_data["dismissals"]
    wkts = vs_data["wickets_taken"]

    strike_rate = round((runs / balls) * 100, 2) if balls > 0 else 0.0
    avg = round(runs / outs, 2) if outs > 0 else "∞"

    # 🧠 Dominance line
    if runs > wkts * 10:
        dominance = "🔥 <b>Batting Dominance</b>"
    elif wkts > outs:
        dominance = "🎯 <b>Bowling Edge</b>"
    else:
        dominance = "⚔️ <b>Even Rivalry</b>"

    # 🧾 Final Message
    msg = (
        "🤝 <b>HEAD-TO-HEAD BATTLE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>{user.first_name}</b>  🆚  <b>{target_user.first_name}</b>\n\n"

        "📌 <b>Overall Record</b>\n"
        f"• Matches Played: <b>{matches}</b>\n\n"

        "🏏 <b>Batting Impact</b>\n"
        f"• Runs Scored: <b>{runs}</b>\n"
        f"• Balls Faced: <b>{balls}</b>\n"
        f"• Strike Rate: <b>{strike_rate}</b>\n"
        f"• Average: <b>{avg}</b>\n\n"

        "⚾ <b>Bowling Impact</b>\n"
        f"• Wickets Taken: <b>{wkts}</b>\n"
        f"• Times Dismissed Opponent: <b>{outs}</b>\n\n"

        f"{dominance}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>Calculated from real match data only</i>"
    )

    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

# Owner/Admin commands
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Broadcast via FORWARD (Not Copy)
    Usage: Reply to ANY message with /broadcast
    Bot will forward that exact message to all Groups & DMs
    """
    user = update.effective_user
    if user.id != OWNER_ID: 
        return

    # ✅ Check if replied to a message
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b> Reply to any message with <code>/broadcast</code>\n\n"
            "Bot will forward that message to all users & groups.",
            parse_mode=ParseMode.HTML
        )
        return

    target_message = update.message.reply_to_message
    
    # Start Status Message
    status_msg = await update.message.reply_text(
        "📢 <b>BROADCAST STARTED</b>\n"
        "⏳ <i>Forwarding to all groups & users...</i>",
        parse_mode=ParseMode.HTML
    )
    
    success_groups = 0
    fail_groups = 0
    success_users = 0
    fail_users = 0
    
    # --- 1. BROADCAST TO GROUPS ---
    for chat_id in list(registered_groups.keys()):
        # ✅ SKIP BANNED GROUPS
        if chat_id in banned_groups:
            fail_groups += 1
            continue
            
        try:
            await context.bot.forward_message(
                chat_id=chat_id,
                from_chat_id=target_message.chat_id,
                message_id=target_message.message_id
            )
            success_groups += 1
            await asyncio.sleep(0.05)  # Anti-flood delay
        except Exception as e:
            fail_groups += 1
            logger.error(f"Failed to forward to group {chat_id}: {e}")

    # --- 2. BROADCAST TO USERS (DMs) ---
    for user_id in list(user_data.keys()):
        try:
            await context.bot.forward_message(
                chat_id=user_id,
                from_chat_id=target_message.chat_id,
                message_id=target_message.message_id
            )
            success_users += 1
            await asyncio.sleep(0.05)  # Anti-flood delay
        except Exception as e:
            fail_users += 1
            # Common error: User hasn't started bot or blocked it
            logger.debug(f"Failed to forward to user {user_id}: {e}")

    # --- 3. FINAL REPORT ---
    total_groups = len(registered_groups)
    total_users = len(user_data)
    banned_count = len(banned_groups)
    
    report = (
        "✅ <b>BROADCAST COMPLETE!</b>\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "👥 <b>GROUPS</b>\n"
        f"   ✅ Sent: <code>{success_groups}</code>\n"
        f"   ❌ Failed: <code>{fail_groups}</code>\n"
        f"   🚫 Banned: <code>{banned_count}</code>\n"
        f"   📊 Total: <code>{total_groups}</code>\n\n"
        "👤 <b>USERS (DMs)</b>\n"
        f"   ✅ Sent: <code>{success_users}</code>\n"
        f"   ❌ Failed: <code>{fail_users}</code>\n"
        f"   📊 Total: <code>{total_users}</code>\n\n"
        "━━━━━━━━━━━━━━━━━\n"
        f"📌 <i>Message forwarded as-is (not copied)</i>"
    )
    
    await context.bot.edit_message_text(
        chat_id=update.message.chat_id,
        message_id=status_msg.message_id,
        text=report,
        parse_mode=ParseMode.HTML
    )

async def botstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Bot Statistics Dashboard"""
    user = update.effective_user
    
    # Check Admin
    if user.id != OWNER_ID:
        await update.message.reply_text("🔒 <b>Access Denied:</b> Owner only command.", parse_mode=ParseMode.HTML)
        return
    
    # Calculate Uptime
    uptime_seconds = int(time.time() - bot_start_time)
    uptime_days = uptime_seconds // 86400
    uptime_hours = (uptime_seconds % 86400) // 3600
    uptime_minutes = (uptime_seconds % 3600) // 60
    
    # Calculate Balls
    total_balls = sum(match.get("total_balls", 0) for match in match_history)
    
    # Find Active Group
    most_active = "N/A"
    if match_history:
        group_counts = {}
        for m in match_history:
            gid = m.get("group_id")
            group_counts[gid] = group_counts.get(gid, 0) + 1
        if group_counts:
            top_gid = max(group_counts, key=group_counts.get)
            most_active = registered_groups.get(top_gid, {}).get("group_name", "Unknown")

    # Dashboard Message
    msg = "🤖 <b>SYSTEM DASHBOARD</b> 🤖\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⏱ <b>Uptime:</b> <code>{uptime_days}d {uptime_hours}h {uptime_minutes}m</code>\n"
    msg += f"📡 <b>Status:</b> 🟢 Online\n"
    msg += f"💾 <b>Database:</b> {os.path.getsize(USERS_FILE) // 1024} KB (Healthy)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    msg += "📈 <b>TRAFFIC STATS</b>\n"
    msg += f"👥 <b>Total Users:</b> <code>{len(user_data)}</code>\n"
    msg += f"🛡 <b>Total Groups:</b> <code>{len(registered_groups)}</code>\n"
    msg += f"🎮 <b>Live Matches:</b> <code>{len(active_matches)}</code>\n\n"
    
    msg += "🏏 <b>GAMEPLAY STATS</b>\n"
    msg += f"🏆 <b>Matches Finished:</b> <code>{len(match_history)}</code>\n"
    msg += f"⚾ <b>Balls Bowled:</b> <code>{total_balls}</code>\n"
    msg += f"🔥 <b>Top Group:</b> {most_active}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━"
    
    try:
        await update.message.reply_photo(
            photo=MEDIA_ASSETS["botstats"],
            caption=msg,
            parse_mode=ParseMode.HTML
        )
    except:
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

# 1. Manual Backup Command
async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual Backup Command: Send .db file to Owner"""
    user = update.effective_user
    if user.id != OWNER_ID: return

    save_data() # Latest data save karo
    
    try:
        if os.path.exists(DB_FILE):
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
            file_name = f"cricoverse_manual_{timestamp}.db"

            with open(DB_FILE, 'rb') as f:
                await update.message.reply_document(
                    document=f,
                    filename=file_name,
                    caption=f"📦 <b>SQL Database Backup</b>\n📅 {timestamp}",
                    parse_mode=ParseMode.HTML
                )
        else:
            await update.message.reply_text("⚠️ Database file not found yet.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def check_and_celebrate_milestones(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match, player: Player, event_type: str):
    """
    🎉 AUTO MILESTONE DETECTOR
    Checks and celebrates 50, 100, Hat-trick
    event_type: 'batting' or 'bowling'
    """
    
    if event_type == 'batting':
        # ✅ Check for 50 (First time crossing)
        if player.runs >= 50 and player.runs - player.balls_faced < 50:
            await send_milestone_gif(context, chat_id, player, "half_century", match.game_mode)
            logger.info(f"🎉 Milestone: {player.first_name} scored 50!")
            await asyncio.sleep(5)  # 5 second celebration pause
            
        # ✅ Check for 100 (First time crossing)
        elif player.runs >= 100 and player.runs - player.balls_faced < 100:
            await send_milestone_gif(context, chat_id, player, "century", match.game_mode)
            logger.info(f"🎉 Milestone: {player.first_name} scored 100!")
            await asyncio.sleep(5)
            
    elif event_type == 'bowling':
        # ✅ Check for Hat-trick (Exactly 3 wickets)
        if player.wickets == 3:
            await send_milestone_gif(context, chat_id, player, "hatrick", match.game_mode)
            logger.info(f"🎉 Milestone: {player.first_name} got Hat-trick!")
            await asyncio.sleep(5)


async def send_milestone_gif(context: ContextTypes.DEFAULT_TYPE, chat_id: int, player: Player, milestone_type: str, game_mode: str):
    """
    🎊 MILESTONE CELEBRATION WITH GIF
    """
    player_tag = f"<a href='tg://user?id={player.user_id}'>{player.first_name}</a>"
    
    if milestone_type == "half_century":
        gif = "CgACAgUAAxkBAAIjvGlViB_k4xno1I7SvP_yjqat_swhAALjGAACQdfwV3nPGMVrF3YgOAQ"
        msg = f"🎉 <b>HALF CENTURY!</b> 🎉\n"
        msg += "━━━━━━━━━━━━━━━━━━\n"
        msg += f"🏏 <b>{player_tag}</b> reaches FIFTY!\n"
        msg += f"📊 <b>Score:</b> {player.runs} ({player.balls_faced})\n"
        msg += f"⚡ <b>Strike Rate:</b> {round((player.runs/max(player.balls_faced,1))*100, 1)}\n\n"
        msg += "🔥 <i>What a brilliant knock!</i>\n"
        msg += "━━━━━━━━━━━━━━━━━━"
        
    elif milestone_type == "century":
        gif = "CgACAgUAAxkBAAIjvmlViDWGHyeIZrWAraXgMumQeYd4AAIhBgACJWaIVY0cR_DZgUHEOAQ"
        msg = f"🏆 <b>CENTURY!</b> 🏆\n"
        msg += "━━━━━━━━━━━━━━━━━━\n"
        msg += f"👑 <b>{player_tag}</b> hits a HUNDRED!\n"
        msg += f"📊 <b>Score:</b> {player.runs} ({player.balls_faced})\n"
        msg += f"⚡ <b>Strike Rate:</b> {round((player.runs/max(player.balls_faced,1))*100, 1)}\n\n"
        msg += "💎 <i>Absolute masterclass! Standing ovation!</i>\n"
        msg += "━━━━━━━━━━━━━━━━━━"
        
    elif milestone_type == "hatrick":
        gif = "CgACAgIAAxkBAAIjwGlViEEuz8Mii2b7xDykVft0PQTkAAIjfQACcfxgSAbN6g5nS2dyOAQ"
        msg = f"🎯 <b>HAT-TRICK!</b> 🎯\n"
        msg += "━━━━━━━━━━━━━━━━━━\n"
        msg += f"⚡ <b>{player_tag}</b> takes THREE WICKETS!\n"
        msg += f"📊 <b>Wickets:</b> {player.wickets}/{player.runs_conceded}\n"
        msg += f"🏏 <b>Overs:</b> {format_overs(player.balls_bowled)}\n\n"
        msg += "🔥 <i>Unstoppable! What a spell!</i>\n"
        msg += "━━━━━━━━━━━━━━━━━━"
    else:
        return
    
    try:
        await context.bot.send_animation(chat_id, animation=gif, caption=msg, parse_mode=ParseMode.HTML)
    except:
        await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)

# 2. Automated Backup Job (Background Task)
async def auto_backup_job(context: ContextTypes.DEFAULT_TYPE):
    """Automatic Background Backup"""
    save_data() # Ensure latest data is saved to DB
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        file_name = f"cricoverse_auto_{timestamp}.db"
        
        if os.path.exists(DB_FILE):
            with open(DB_FILE, 'rb') as f:
                await context.bot.send_document(
                    chat_id=OWNER_ID,
                    document=f,
                    filename=file_name,
                    caption=f"🤖 <b>Auto SQL Backup</b>\n📅 {timestamp}",
                    parse_mode=ParseMode.HTML
                )
    except Exception as e:
        logger.error(f"Auto backup failed: {e}")

async def restore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restore SQL Database from a replied .db file"""
    user = update.effective_user
    if user.id != OWNER_ID: return
    
    # Check if replied to a document
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("⚠️ Reply to a <b>.db</b> file to restore.", parse_mode=ParseMode.HTML)
        return

    # Check matches (Restore hone par current match crash ho sakta hai)
    if active_matches:
        await update.message.reply_text("⚠️ Matches chal rahe hain. Pehle /endmatch karein.")
        return

    doc = update.message.reply_to_message.document

    # Security: Check file extension
    if not doc.file_name.endswith(('.db', '.sqlite')):
        await update.message.reply_text("⚠️ Invalid File! Sirf <b>.db</b> file allow hai.", parse_mode=ParseMode.HTML)
        return
    
    status = await update.message.reply_text("⏳ <b>Restoring SQL Database...</b>", parse_mode=ParseMode.HTML)

    try:
        # Download new file and overwrite existing DB
        new_file = await doc.get_file()
        await new_file.download_to_drive(DB_FILE)
        
        # Reload Memory (RAM) from new DB
        # Important: Global variables clear karke naya data load karein
        global user_data, match_history, player_stats, achievements, registered_groups
        user_data = {}
        match_history = []
        player_stats = {}
        achievements = {}
        registered_groups = {}
        
        load_data() # Yeh function ab naye DB se data padhega
        
        await status.edit_text("✅ <b>Restore Complete!</b>\nNew Data Loaded Successfully.", parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        await status.edit_text(f"❌ Restore Failed: {e}")

async def endmatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force end match instantly and stop all processes"""
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        await update.message.reply_text("⚠️ No active match to end.")
        return

    # Check Admin/Host Rights
    member = await chat.get_member(user.id)
    is_admin = member.status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]
    
    match = active_matches[chat.id]
    
    if user.id != OWNER_ID and user.id != match.host_id and not is_admin:
        await update.message.reply_text("❌ Only the Host or Group Admin can end the match!")
        return
        
    # 1. Change Phase to block any new inputs
    match.phase = GamePhase.MATCH_ENDED
    
    # 2. Cancel ALL Background Tasks (Timers)
    if match.ball_timeout_task: match.ball_timeout_task.cancel()
    if match.batsman_selection_task: match.batsman_selection_task.cancel()
    if match.bowler_selection_task: match.bowler_selection_task.cancel()
    
    # 3. Unpin the game message
    try:
        if match.main_message_id:
            await context.bot.unpin_chat_message(chat_id=chat.id, message_id=match.main_message_id)
    except: pass
    
    # 4. Delete match data
    del active_matches[chat.id]
    
    await update.message.reply_text("🛑 <b>MATCH ENDED!</b>\nThe game has been forcefully stopped. Use /game to start a new one.", parse_mode=ParseMode.HTML)

async def changehost_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Change Host System: Both Captains must vote YES
    Usage: Reply to a player with /changehost
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        await update.message.reply_text("❌ No active match.")
        return
    
    match = active_matches[chat.id]
    
    # Only works in Team Edit or Match Phases
    if match.phase not in [GamePhase.TEAM_EDIT, GamePhase.MATCH_IN_PROGRESS]:
        await update.message.reply_text("⚠️ Host change only allowed during Team Edit or Match.")
        return
    
    # Check if user is a captain
    captain_x = match.team_x.captain_id
    captain_y = match.team_y.captain_id
    
    if user.id not in [captain_x, captain_y]:
        await update.message.reply_text("🚫 Only Captains can initiate host change!")
        return
    
    # Check if replied to someone
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Reply to the player you want to make Host.")
        return
    
    new_host = update.message.reply_to_message.from_user
    new_host_id = new_host.id
    
    # Can't make self host if already host
    if new_host_id == match.host_id:
        await update.message.reply_text("⚠️ This player is already the Host!")
        return
    
    # Check if new host is in match
    if not (match.team_x.get_player(new_host_id) or match.team_y.get_player(new_host_id)):
        await update.message.reply_text("⚠️ This player is not in any team!")
        return
    
    # Initialize vote tracking for this candidate
    if new_host_id not in match.host_change_votes:
        match.host_change_votes[new_host_id] = {"x_voted": False, "y_voted": False}
    
    # Record vote
    votes = match.host_change_votes[new_host_id]
    
    if user.id == captain_x:
        if votes["x_voted"]:
            await update.message.reply_text("⚠️ Team X Captain already voted for this change!")
            return
        votes["x_voted"] = True
        voter_team = "Team X"
    else:
        if votes["y_voted"]:
            await update.message.reply_text("⚠️ Team Y Captain already voted for this change!")
            return
        votes["y_voted"] = True
        voter_team = "Team Y"
    
    new_host_tag = get_user_tag(new_host)
    
    # Check if both captains voted
    if votes["x_voted"] and votes["y_voted"]:
        # ✅ HOST CHANGE APPROVED
        old_host_id = match.host_id
        old_host_name = match.host_name
        
        match.host_id = new_host_id
        match.host_name = new_host.first_name
        
        # Clear votes
        match.host_change_votes = {}
        
        msg = f"👑 <b>HOST CHANGED!</b>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🔄 <b>Old Host:</b> {old_host_name}\n"
        msg += f"✅ <b>New Host:</b> {new_host_tag}\n\n"
        msg += f"<i>Both captains approved this change.</i>"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    else:
        # Waiting for second vote
        pending = "Team Y Captain" if not votes["y_voted"] else "Team X Captain"
        
        msg = f"🗳 <b>HOST CHANGE VOTE</b>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📋 <b>Candidate:</b> {new_host_tag}\n"
        msg += f"✅ <b>{voter_team} Captain</b> voted YES\n"
        msg += f"⏳ <b>Waiting for:</b> {pending}\n\n"
        msg += f"<i>{pending}, reply to {new_host.first_name} with /changehost to approve.</i>"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def changecap_x_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Change Captain of Team X (Host Only)
    Usage: Reply to a Team X player with /changecap_X
    """
    await change_captain_logic(update, context, "X")

async def changecap_y_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Change Captain of Team Y (Host Only)
    Usage: Reply to a Team Y player with /changecap_Y
    """
    await change_captain_logic(update, context, "Y")

async def change_captain_logic(update: Update, context: ContextTypes.DEFAULT_TYPE, team_name: str):
    """
    Unified Captain Change Logic
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        await update.message.reply_text("❌ No active match.")
        return
    
    match = active_matches[chat.id]
    
    # Only Host can change captains
    if user.id != match.host_id:
        await update.message.reply_text("🚫 Only the Host can change captains!")
        return
    
    # Check phase
    if match.phase not in [GamePhase.TEAM_EDIT, GamePhase.CAPTAIN_SELECTION, GamePhase.MATCH_IN_PROGRESS]:
        await update.message.reply_text("⚠️ Captain change not allowed in this phase.")
        return
    
    # Check if replied to someone
    if not update.message.reply_to_message:
        await update.message.reply_text(f"⚠️ Reply to the new captain of Team {team_name}.")
        return
    
    new_captain = update.message.reply_to_message.from_user
    new_captain_id = new_captain.id
    
    # Get team
    team = match.team_x if team_name == "X" else match.team_y
    
    # Check if player is in this team
    if not team.get_player(new_captain_id):
        await update.message.reply_text(f"⚠️ This player is not in Team {team_name}!")
        return
    
    # Check if already captain
    if team.captain_id == new_captain_id:
        await update.message.reply_text(f"⚠️ {new_captain.first_name} is already the captain!")
        return
    
    # Get old captain name
    old_captain = team.get_player(team.captain_id)
    old_captain_name = old_captain.first_name if old_captain else "None"
    
    # Change captain
    team.captain_id = new_captain_id
    
    new_captain_tag = get_user_tag(new_captain)
    
    msg = f"🧢 <b>CAPTAIN CHANGE - TEAM {team_name}</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🔄 <b>Old Captain:</b> {old_captain_name}\n"
    msg += f"✅ <b>New Captain:</b> {new_captain_tag}\n\n"
    msg += f"<i>Host has updated the leadership.</i>"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def impact_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Impact Player Substitution (Captain Only, 3 Uses Per Team)
    Usage: /impact @old_player @new_player
    or Reply to old player with: /impact @new_player
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        await update.message.reply_text("❌ No active match.")
        return
    
    match = active_matches[chat.id]
    
    # Only during match
    if match.phase != GamePhase.MATCH_IN_PROGRESS:
        await update.message.reply_text("⚠️ Impact Player can only be used during the match!")
        return
    
    # Check if user is captain
    captain_x = match.team_x.captain_id
    captain_y = match.team_y.captain_id
    
    if user.id not in [captain_x, captain_y]:
        await update.message.reply_text("🚫 Only Captains can use Impact Player!")
        return
    
    # Determine captain's team
    if user.id == captain_x:
        team = match.team_x
        team_name = "Team X"
        impact_count = match.team_x_impact_count
        impact_history = match.team_x_impact_history
    else:
        team = match.team_y
        team_name = "Team Y"
        impact_count = match.team_y_impact_count
        impact_history = match.team_y_impact_history
    
    # ✅ Check if 3 uses exhausted
    if impact_count >= 3:
        await update.message.reply_text(
            f"⚠️ <b>{team_name} has used all 3 Impact Players!</b>\n\n"
            f"📋 <b>Substitutions Made:</b>\n"
            f"{format_impact_history(impact_history)}",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Parse targets
    old_player_id = None
    new_player_id = None
    
    # Method 1: Reply + Mention
    if update.message.reply_to_message:
        old_player_id = update.message.reply_to_message.from_user.id
        
        # Get new player from mention
        if update.message.entities:
            for entity in update.message.entities:
                if entity.type == "mention":
                    username = update.message.text[entity.offset:entity.offset + entity.length].replace("@", "")
                    # Find user by username
                    for uid, data in user_data.items():
                        if data.get("username", "").lower() == username.lower():
                            new_player_id = uid
                            break
                elif entity.type == "text_mention":
                    new_player_id = entity.user.id
    
    # Method 2: Two Mentions
    elif update.message.entities and len([e for e in update.message.entities if e.type in ["mention", "text_mention"]]) >= 2:
        mentions = [e for e in update.message.entities if e.type in ["mention", "text_mention"]]
        
        # First mention = old player
        if mentions[0].type == "text_mention":
            old_player_id = mentions[0].user.id
        else:
            username = update.message.text[mentions[0].offset:mentions[0].offset + mentions[0].length].replace("@", "")
            for uid, data in user_data.items():
                if data.get("username", "").lower() == username.lower():
                    old_player_id = uid
                    break
        
        # Second mention = new player
        if mentions[1].type == "text_mention":
            new_player_id = mentions[1].user.id
        else:
            username = update.message.text[mentions[1].offset:mentions[1].offset + mentions[1].length].replace("@", "")
            for uid, data in user_data.items():
                if data.get("username", "").lower() == username.lower():
                    new_player_id = uid
                    break
    
    if not old_player_id or not new_player_id:
        remaining = 3 - impact_count
        await update.message.reply_text(
            f"⚠️ <b>Usage:</b>\n"
            f"Reply to old player: <code>/impact @newplayer</code>\n"
            f"Or: <code>/impact @oldplayer @newplayer</code>\n\n"
            f"🔄 <b>Remaining Substitutions:</b> {remaining}/3",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Validate old player is in team
    old_player = team.get_player(old_player_id)
    if not old_player:
        await update.message.reply_text("⚠️ Old player is not in your team!")
        return
    
    # Check if old player was already substituted out
    for old_name, new_name in impact_history:
        if old_name == old_player.first_name:
            await update.message.reply_text(
                f"⚠️ <b>{old_player.first_name}</b> was already substituted out earlier!\n"
                f"You cannot substitute them again.",
                parse_mode=ParseMode.HTML
            )
            return
    
    # Check if old player is currently playing
    if team == match.current_batting_team:
        if team.current_batsman_idx is not None and team.players[team.current_batsman_idx].user_id == old_player_id:
            await update.message.reply_text("⚠️ Cannot substitute the current striker!")
            return
        if team.current_non_striker_idx is not None and team.players[team.current_non_striker_idx].user_id == old_player_id:
            await update.message.reply_text("⚠️ Cannot substitute the current non-striker!")
            return
    
    if team == match.current_bowling_team:
        if team.current_bowler_idx is not None and team.players[team.current_bowler_idx].user_id == old_player_id:
            await update.message.reply_text("⚠️ Cannot substitute the current bowler!")
            return
    
    # Check if new player is not already in match
    if match.team_x.get_player(new_player_id) or match.team_y.get_player(new_player_id):
        await update.message.reply_text("⚠️ New player is already in a team!")
        return
    
    # Check if new player was substituted out earlier (can't bring back)
    for old_name, new_name in impact_history:
        if new_player_id in [p.user_id for p in team.players if p.first_name == old_name]:
            await update.message.reply_text(
                f"⚠️ Cannot bring back a player who was substituted out!",
                parse_mode=ParseMode.HTML
            )
            return
    
    # Initialize new player stats
    if new_player_id not in user_data:
        # Fetch user info
        try:
            new_user = await context.bot.get_chat(new_player_id)
            user_data[new_player_id] = {
                "user_id": new_player_id,
                "username": new_user.username or "",
                "first_name": new_user.first_name,
                "started_at": datetime.now().isoformat(),
                "total_matches": 0
            }
            init_player_stats(new_player_id)
            save_data()
        except:
            await update.message.reply_text("⚠️ Cannot fetch new player info. Make sure they've started the bot.")
            return
    
    new_user_info = user_data[new_player_id]
    
    # Create new player object
    new_player = Player(new_player_id, new_user_info["username"], new_user_info["first_name"])
    
    # Replace in team
    for i, p in enumerate(team.players):
        if p.user_id == old_player_id:
            team.players[i] = new_player
            break
    
    # ✅ Update impact tracking
    if user.id == captain_x:
        match.team_x_impact_count += 1
        match.team_x_impact_history.append((old_player.first_name, new_player.first_name))
        remaining = 3 - match.team_x_impact_count
    else:
        match.team_y_impact_count += 1
        match.team_y_impact_history.append((old_player.first_name, new_player.first_name))
        remaining = 3 - match.team_y_impact_count
    
    old_tag = f"<a href='tg://user?id={old_player_id}'>{old_player.first_name}</a>"
    new_tag = get_user_tag(type("User", (), {"id": new_player_id, "first_name": new_user_info["first_name"]}))
    
    msg = f"🔄 <b>IMPACT PLAYER - {team_name}</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⬅️ <b>OUT:</b> {old_tag}\n"
    msg += f"➡️ <b>IN:</b> {new_tag}\n\n"
    msg += f"📊 <b>Substitutions Used:</b> {impact_count + 1}/3\n"
    msg += f"🔄 <b>Remaining:</b> {remaining}\n\n"
    msg += f"<i>Strategic substitution made!</i>"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

def format_impact_history(history: list) -> str:
    """Format impact player history for display"""
    if not history:
        return "<i>No substitutions made yet</i>"
    
    result = ""
    for i, (old, new) in enumerate(history, 1):
        result += f"  {i}. {old} ➡️ {new}\n"
    return result

async def impactstatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Check Impact Player Status
    Shows remaining substitutions for both teams
    """
    chat = update.effective_chat
    
    if chat.id not in active_matches:
        await update.message.reply_text("❌ No active match.")
        return
    
    match = active_matches[chat.id]
    
    # Team X Status
    x_used = match.team_x_impact_count
    x_remaining = 3 - x_used
    x_history = format_impact_history(match.team_x_impact_history)
    
    # Team Y Status
    y_used = match.team_y_impact_count
    y_remaining = 3 - y_used
    y_history = format_impact_history(match.team_y_impact_history)
    
    msg = f"🔄 <b>IMPACT PLAYER STATUS</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    msg += f"🔵 <b>Team X</b>\n"
    msg += f"📊 Used: <b>{x_used}/3</b> | Remaining: <b>{x_remaining}</b>\n"
    msg += f"📋 History:\n{x_history}\n\n"
    
    msg += f"🔴 <b>Team Y</b>\n"
    msg += f"📊 Used: <b>{y_used}/3</b> | Remaining: <b>{y_remaining}</b>\n"
    msg += f"📋 History:\n{y_history}\n\n"
    
    msg += f"━━━━━━━━━━━━━━━━━━━━━"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def resetmatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /resetmatch command - Owner only"""
    user = update.effective_user
    
    # Check if owner
    if user.id != OWNER_ID:
        await update.message.reply_text(
            "This command is restricted to the bot owner."
        )
        return
    
    chat = update.effective_chat
    
    if chat.id not in active_matches:
        await update.message.reply_text("No active match in this group.")
        return
    
    match = active_matches[chat.id]
    
    # Cancel all tasks
    if match.join_phase_task:
        match.join_phase_task.cancel()
    if match.ball_timeout_task:
        match.ball_timeout_task.cancel()
    if match.batsman_selection_task:
        match.batsman_selection_task.cancel()
    if match.bowler_selection_task:
        match.bowler_selection_task.cancel()
    
    # Reset match to team joining phase
    del active_matches[chat.id]
    
    await update.message.reply_text(
        "Match has been reset by bot owner.\n"
        "Use /game to start a new match."
    )
    
    logger.info(f"Match in group {chat.id} reset by owner")

# Error handler
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors and notify user"""
    logger.error(f"Exception while handling an update: {context.error}")
    
    # Notify owner about error
    try:
        error_text = f"An error occurred:\n\n{str(context.error)}"
        await context.bot.send_message(
            chat_id=OWNER_ID,
            text=error_text
        )
    except Exception as e:
        logger.error(f"Failed to notify owner about error: {e}")

async def create_prediction_poll(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Create and pin prediction poll"""
    try:
        poll_message = await context.bot.send_poll(
            chat_id=group_id,
            question="🎯 Who will win this match?let's see your poll",
            options=[
                f"🔵 {match.team_x.name}",
                f"🔴 {match.team_y.name}"
            ],
            is_anonymous=False,
            allows_multiple_answers=False
        )
        
        # Pin the poll
        try:
            await context.bot.pin_chat_message(
                chat_id=group_id,
                message_id=poll_message.message_id,
                disable_notification=True
            )
        except:
            pass  # If bot can't pin, continue anyway
            
    except Exception as e:
        logger.error(f"Error creating poll: {e}")

async def handle_group_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Unified Handle Group Input
    Handles logic for both SOLO and TEAM modes.
    """
    
    # --- 1. Basic Validations ---
    if update.effective_chat.type == "private": return
    if not update.message or not update.message.text: return
    
    text = update.message.text.strip()
    if not text.isdigit(): return
    
    number = int(text)
    if number < 0 or number > 6: return # Ignore invalid numbers
    
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    if chat_id not in active_matches: return
    match = active_matches[chat_id]
    match.last_activity = time.time()
    
    # Safety Check: Default to 'TEAM' if game_mode attribute is missing (backward compatibility)
    current_mode = getattr(match, "game_mode", "TEAM")
    
    processed = False

    # ==========================================
    # ⚔️ CASE 1: SOLO MODE LOGIC (UPDATED)
    # ==========================================
    if current_mode == "SOLO" and match.phase == GamePhase.SOLO_MATCH:
        if match.current_solo_bat_idx < len(match.solo_players):
            batter = match.solo_players[match.current_solo_bat_idx]
            
            if user_id == batter.user_id:
                if match.current_ball_data.get("bowler_number") is not None:
                    match.current_ball_data["batsman_number"] = number
                    processed = True
                    
                    # ✅ STOP BATSMAN TIMER
                    if match.ball_timeout_task: 
                        match.ball_timeout_task.cancel()
                    
                    try: 
                        await update.message.delete()
                    except: 
                        pass
                    
                    await process_solo_turn_result(context, chat_id, match)

    # ==========================================
    # 👥 CASE 2: TEAM MODE LOGIC
    # ==========================================
    elif current_mode == "TEAM" and match.phase == GamePhase.MATCH_IN_PROGRESS:
        batting_team = match.current_batting_team
        bowling_team = match.current_bowling_team
        
        # Check if teams and indices are valid
        if (batting_team.current_batsman_idx is not None and 
            bowling_team.current_bowler_idx is not None):
            
            striker = batting_team.players[batting_team.current_batsman_idx]
            bowler = bowling_team.players[bowling_team.current_bowler_idx]
            
            # Sub-Case A: Bowler sent number in Group (Backup for DM)
            if user_id == bowler.user_id:
                if match.current_ball_data.get("bowler_number") is None:
                    match.current_ball_data["bowler_number"] = number
                    
                    await context.bot.send_message(chat_id, f"⚾ <b>{bowler.first_name}</b> has bowled!", parse_mode=ParseMode.HTML)
                    
                    # Cancel timeout & Request Batsman
                    if match.ball_timeout_task: match.ball_timeout_task.cancel()
                    await request_batsman_number(context, chat_id, match)
                    processed = True

            # Sub-Case B: Striker sent number (The Shot)
            elif user_id == striker.user_id:
                # Batsman can only play if bowler has bowled
                if match.current_ball_data.get("bowler_number") is not None:
                    # Prevent double input
                    if match.current_ball_data.get("batsman_number") is None:
                        match.current_ball_data["batsman_number"] = number
                        
                        await context.bot.send_message(chat_id, f"🏏 <b>{striker.first_name}</b> played a shot!", parse_mode=ParseMode.HTML)
                        
                        if match.ball_timeout_task: match.ball_timeout_task.cancel()
                        await process_ball_result(context, chat_id, match)
                        processed = True

async def handle_dm_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle DM Input with Safety Check & GIF for Solo Mode - FIXED"""
    user = update.effective_user
    if not update.message or not update.message.text: return
    
    msg = update.message.text.strip()
    if not msg.isdigit(): return
    num = int(msg)
    if num < 0 or num > 6:
        await update.message.reply_text("⚠️ Please send a number between 0 and 6.")
        return

    for gid, match in active_matches.items():
        current_mode = getattr(match, "game_mode", "TEAM") 
        
        # --- SOLO LOGIC (UPDATED) ---
        if current_mode == "SOLO" and match.phase == GamePhase.SOLO_MATCH:
            if match.current_solo_bowl_idx < len(match.solo_players):
                bowler = match.solo_players[match.current_solo_bowl_idx]
                
                # Check if it's the bowler sending the number
                if user.id == bowler.user_id and match.current_ball_data.get("bowler_number") is None:
                    match.current_ball_data["bowler_number"] = num
                    
                    # ✅ STOP BOWLER TIMER
                    if match.ball_timeout_task: 
                        match.ball_timeout_task.cancel()
                    
                    # Confirm to Bowler
                    await update.message.reply_text(f"✅ <b>Locked:</b> {num}", parse_mode=ParseMode.HTML)
                    
                    # Notify Group with Batting GIF
                    if match.current_solo_bat_idx < len(match.solo_players):
                        batter = match.solo_players[match.current_solo_bat_idx]
                        
                        # ✅ Stats Calculation (Safe Access)
                        batter_runs = batter.runs
                        batter_balls = batter.balls_faced
                        sr = round((batter_runs / max(batter_balls, 1)) * 100, 1)

                        bat_tag = f"<a href='tg://user?id={batter.user_id}'>{batter.first_name}</a>"

                        # ✅ Fix: String concatenation (Used += to prevent overwriting)
                        notification_msg = f"🔴 <b>LIVE</b>\n"
                        notification_msg += "━━━━━━━━━━━━━━━━━━━━\n"
                        notification_msg += f"📊 <b>{batter.first_name}:</b> {batter_runs} ({batter_balls}) | ⚡ SR: {sr}\n"
                        notification_msg += f"🏏 <b>{bat_tag}</b>, play your shot!\n"
                        notification_msg += "━━━━━━━━━━━━━━━━━━━━"
                        
                        # Batting GIF
                        batting_gif = "https://t.me/kyanaamrkhe/7"
                        
                        try:
                            await context.bot.send_animation(
                                gid, 
                                animation=batting_gif, 
                                caption=notification_msg, 
                                parse_mode=ParseMode.HTML
                            )
                        except:
                            await context.bot.send_message(gid, notification_msg, parse_mode=ParseMode.HTML)
                            
                        # ✅ START BATSMAN TIMER
                        match.ball_timeout_task = asyncio.create_task(
                            solo_game_timer(context, gid, match, "batsman", batter.first_name)
                        )
                    return

        # --- TEAM LOGIC (FIXED VARIABLE SCOPE) ---
        elif current_mode == "TEAM" and match.phase == GamePhase.MATCH_IN_PROGRESS:
             if match.current_bowling_team and match.current_bowling_team.current_bowler_idx is not None:
                 if match.current_bowling_team.current_bowler_idx < len(match.current_bowling_team.players):
                     bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
                     
                     if user.id == bowler.user_id:
                         if match.current_ball_data.get("bowler_number") is None:
                            match.current_ball_data["bowler_number"] = num
                            match.current_ball_data["bowler_id"] = user.id
                            
                            if match.ball_timeout_task: match.ball_timeout_task.cancel()
                            
                            await update.message.reply_text(f"✅ <b>Delivery Locked:</b> {num}", parse_mode=ParseMode.HTML)
                            
                            # Group Notification
                            bat_team = match.current_batting_team
                            if bat_team.current_batsman_idx is not None:
                                # ✅ FIX: Define striker here, not batter
                                striker = bat_team.players[bat_team.current_batsman_idx]
                                striker_runs = striker.runs
                                striker_balls = striker.balls_faced
                                sr = round((striker_runs / max(striker_balls, 1)) * 100, 1)
                                
                                striker_tag = f"<a href='tg://user?id={striker.user_id}'>{striker.first_name}</a>"
                                
                                notification_msg = f"🔴 <b>LIVE</b>\n"
                                notification_msg += "━━━━━━━━━━━━━━━━━━━━\n"
                                notification_msg += f"📊 <b>{striker.first_name}:</b> {striker_runs} ({striker_balls}) | ⚡ SR: {sr}\n"
                                notification_msg += f"🏏 <b>{striker_tag}</b>, play your shot!\n"
                                notification_msg += "━━━━━━━━━━━━━━━━━━━━"

                                batting_gif = "https://t.me/kyanaamrkhe/7"
                                try:
                                    await context.bot.send_animation(gid, animation=batting_gif, caption=notification_msg, parse_mode=ParseMode.HTML)
                                except:
                                    await context.bot.send_message(gid, notification_msg, parse_mode=ParseMode.HTML)
                                
                                match.ball_timeout_task = asyncio.create_task(
                                    game_timer(context, gid, match, "batsman", striker.first_name)
                                )
                         return

async def broadcastpin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    📌 BROADCAST + PIN TO GROUPS
    Usage: Reply to ANY message with /broadcastpin
    Bot forwards and pins it in all groups
    """
    user = update.effective_user
    if user.id != OWNER_ID: 
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b> Reply to any message with <code>/broadcastpin</code>\n\n"
            "Bot will forward and PIN that message to all groups.",
            parse_mode=ParseMode.HTML
        )
        return

    target_message = update.message.reply_to_message
    
    status_msg = await update.message.reply_text(
        "📢 <b>BROADCAST + PIN STARTED</b>\n"
        "⏳ <i>Forwarding & pinning to all groups...</i>",
        parse_mode=ParseMode.HTML
    )
    
    success = 0
    failed = 0
    pinned = 0
    
    for chat_id in list(registered_groups.keys()):
        if chat_id in banned_groups:
            failed += 1
            continue
            
        try:
            # Forward message
            sent_msg = await context.bot.forward_message(
                chat_id=chat_id,
                from_chat_id=target_message.chat_id,
                message_id=target_message.message_id
            )
            success += 1
            
            # Try to pin
            try:
                await context.bot.pin_chat_message(
                    chat_id=chat_id,
                    message_id=sent_msg.message_id,
                    disable_notification=False
                )
                pinned += 1
            except Exception as pin_error:
                logger.warning(f"Could not pin in {chat_id}: {pin_error}")
            
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Failed broadcast to group {chat_id}: {e}")

    report = (
        "✅ <b>BROADCAST + PIN COMPLETE!</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "📊 <b>GROUPS</b>\n"
        f"   ✅ Sent: <code>{success}</code>\n"
        f"   📌 Pinned: <code>{pinned}</code>\n"
        f"   ❌ Failed: <code>{failed}</code>\n"
        f"   🚫 Banned: <code>{len(banned_groups)}</code>\n"
        f"   📊 Total: <code>{len(registered_groups)}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    
    await status_msg.edit_text(report, parse_mode=ParseMode.HTML)


async def broadcastdm_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    💬 BROADCAST TO USER DMs ONLY
    Usage: Reply to ANY message with /broadcastdm
    Bot forwards it to all users who started the bot
    """
    user = update.effective_user
    if user.id != OWNER_ID: 
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b> Reply to any message with <code>/broadcastdm</code>\n\n"
            "Bot will forward that message to all users (DMs only).",
            parse_mode=ParseMode.HTML
        )
        return

    target_message = update.message.reply_to_message
    
    status_msg = await update.message.reply_text(
        "💬 <b>DM BROADCAST STARTED</b>\n"
        "⏳ <i>Forwarding to all users...</i>",
        parse_mode=ParseMode.HTML
    )
    
    success = 0
    failed = 0
    
    for user_id in list(user_data.keys()):
        try:
            await context.bot.forward_message(
                chat_id=user_id,
                from_chat_id=target_message.chat_id,
                message_id=target_message.message_id
            )
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.debug(f"Failed DM to user {user_id}: {e}")

    report = (
        "✅ <b>DM BROADCAST COMPLETE!</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "👤 <b>USERS</b>\n"
        f"   ✅ Sent: <code>{success}</code>\n"
        f"   ❌ Failed: <code>{failed}</code>\n"
        f"   📊 Total: <code>{len(user_data)}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "<i>Note: Failed = Users who blocked/deleted bot</i>"
    )
    
    await status_msg.edit_text(report, parse_mode=ParseMode.HTML)

# Is function ko apne code mein imports ke baad kahin bhi daal dein
async def get_file_id_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Bot ko photo bhejo aur File ID paao"""
    if update.message.photo:
        # Sabse high quality wali photo ki ID lete hain
        file_id = update.message.photo[-1].file_id
        
        # Console mein print karega
        print(f"📸 NEW FILE ID: {file_id}")
        
        # Aapko Telegram par wapas bhejega
        await update.message.reply_text(f"🆔 File ID:\n<code>{file_id}</code>", parse_mode=ParseMode.HTML)

# Phir 'main()' function ke andar, handlers wale section mein yeh line add karein:
# application.add_handler(MessageHandler(filters.PHOTO, get_file_id_handler))


def main():
    """Start the bot"""

    # Load data on startup
    load_data()

    # Create application
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .read_timeout(60)
        .write_timeout(60)
        .connect_timeout(60)
        .build()
    )

    # --- JOBS (Scheduled Tasks) ---
    if application.job_queue:
        # Cleanup inactive matches every 60 seconds
        application.job_queue.run_repeating(
            cleanup_inactive_matches, interval=60, first=60
        )

        # Auto Backup every 1 hour
        application.job_queue.run_repeating(
            auto_backup_job, interval=3600, first=10
        )

    # ================== BASIC COMMANDS ==================
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))

    # ================== MATCH COMMANDS ==================
    application.add_handler(CommandHandler("game", game_command))
    application.add_handler(CommandHandler("extend", extend_command))
    application.add_handler(CommandHandler("endmatch", endmatch_command))

    application.add_handler(CommandHandler("add", add_player_command))
    application.add_handler(CommandHandler("remove", remove_player_command))

    application.add_handler(CommandHandler("batting", batting_command))
    application.add_handler(CommandHandler("bowling", bowling_command))

    application.add_handler(CommandHandler("drs", drs_command))

    application.add_handler(CommandHandler("players", players_command))
    application.add_handler(CommandHandler("scorecard", scorecard_command))

    # ================== STATS ==================
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("mystats", mystats_command))
    application.add_handler(CommandHandler("h2h", h2h_command))

    # ================== FUN ==================
    application.add_handler(CommandHandler("cheer", cheer_command))
    application.add_handler(CommandHandler("celebrate", celebrate_command))
    application.add_handler(CommandHandler("taunt", taunt_command))
    application.add_handler(CommandHandler("huddle", huddle_command))

    # ================== SOLO MODE ==================
    application.add_handler(CommandHandler("soloplayers", soloplayers_command))
    application.add_handler(CommandHandler("soloscore", soloscore_command))
    application.add_handler(CommandHandler("extendsolo", extendsolo_command))
    application.add_handler(CommandHandler("endsolo", endsolo_command))

    # ================== OWNER / HOST CONTROLS ==================
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    application.add_handler(CommandHandler("broadcastpin", broadcastpin_command))
    application.add_handler(CommandHandler("broadcastdm", broadcastdm_command)) 
    application.add_handler(CommandHandler("botstats", botstats_command))
    application.add_handler(CommandHandler("backup", backup_command))
    application.add_handler(CommandHandler("restore", restore_command))
    application.add_handler(CommandHandler("resetmatch", resetmatch_command))

    application.add_handler(CommandHandler("changehost", changehost_command))
    application.add_handler(CommandHandler("changecap_x", changecap_x_command))
    application.add_handler(CommandHandler("changecap_y", changecap_y_command))
    application.add_handler(CommandHandler("impact", impact_command))
    application.add_handler(CommandHandler("impactstatus", impactstatus_command))
    
    application.add_handler(CommandHandler("bangroup", bangroup_command))
    application.add_handler(CommandHandler("unbangroup", unbangroup_command))
    application.add_handler(CommandHandler("bannedgroups", bannedgroups_command))

    # ================== CALLBACK HANDLERS ==================
    application.add_handler(
        CallbackQueryHandler(mode_selection_callback, pattern="^mode_")
    )
    application.add_handler(
        CallbackQueryHandler(help_callback, pattern="^help_")
    )
    application.add_handler(
        CallbackQueryHandler(solo_join_callback, pattern="^solo_")
    )
    application.add_handler(
        CallbackQueryHandler(team_join_callback, pattern="^(join_team_|leave_team)")
    )
    application.add_handler(
        CallbackQueryHandler(host_selection_callback, pattern="^become_host$")
    )
    application.add_handler(
        CallbackQueryHandler(captain_selection_callback, pattern="^captain_team_")
    )
    application.add_handler(
        CallbackQueryHandler(team_edit_done_callback, pattern="^team_edit_done$")
    )
    application.add_handler(
        CallbackQueryHandler(over_selection_callback, pattern="^overs_")
    )
    application.add_handler(
        CallbackQueryHandler(toss_callback, pattern="^toss_(heads|tails)$")
    )
    application.add_handler(
        CallbackQueryHandler(toss_decision_callback, pattern="^toss_decision_")
    )
    application.add_handler(
        CallbackQueryHandler(set_edit_team_callback, pattern="^(edit_team_|edit_back)")
    )

    # Stats callbacks
    application.add_handler(
        CallbackQueryHandler(stats_view_callback, pattern="^stats_view_")
    )
    application.add_handler(
        CallbackQueryHandler(stats_main_callback, pattern="^stats_main_")
    )

    # ================== MESSAGE HANDLERS ==================
    application.add_handler(
        MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, handle_dm_message)
    )
    application.add_handler(
        MessageHandler(filters.TEXT & filters.ChatType.GROUPS, handle_group_input)
    )

    # Error handler
    application.add_error_handler(error_handler)

    # Start bot
    logger.info("Cricoverse bot starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
