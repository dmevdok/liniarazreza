from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
import yaml
import feedparser
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import re
from collections import Counter
import requests
import asyncio
import time
from datetime import datetime
import uvicorn
from stop_words import get_stop_words
import pymorphy2
from langdetect import detect
from fuzzywuzzy import fuzz
from scipy.sparse.csgraph import connected_components
from scipy.sparse import csr_matrix


stop_words_en = get_stop_words('en')
stop_words_ru = get_stop_words('ru')
stop_words_ua = get_stop_words('ukrainian')

morph_ru = pymorphy2.MorphAnalyzer(lang="ru")
morph_ua = pymorphy2.MorphAnalyzer(lang="uk")

STOP_WORDS = stop_words_en + stop_words_ru + stop_words_ua

SOURCE_CHANNELS = ["rss", "telegram"]

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

UPDATE_INTERVAL = 10 # in minutes
HOURS_TO_KEEP_HISTORY = 3

HTML_TO_TEXT = re.compile("<.*?>")
DOUBLE_SPACES = re.compile("[ \\t]+")
NON_WORD = re.compile("[\,\.\-\?]+")
LETTERS_ONLY = re.compile("[^іІїЇєЄґҐ0-9а-яА-ЯёËa-zA-Z\-\s\.,\!\?]+")

SIMILARITY_THRESHOLD = 70

def read_config():
    with open("config.yaml") as f:
        return yaml.load(f, Loader=yaml.FullLoader)

config = read_config()
buffer = {}
global_wordstat = {}
global_similarity = {}

def html_to_text(raw_html):
  text = raw_html.replace("<br/>", "\n")
  text = re.sub(HTML_TO_TEXT, "  ", text)
  text = text.replace("&nbsp;", " ")
  text = text.replace("&quot;", '"')
  text = text.replace('\"','"')
  return text

app = FastAPI()

def filter_text(text, to_remove):
    text = beautify_text(html_to_text(text))
    for r in to_remove:
        text = text.replace(r, "")
    return beautify_text(text)

def beautify_text(text):
    text = re.sub(DOUBLE_SPACES, " ", text)
    return text.strip()

def letters_only(text):
    text = re.sub(LETTERS_ONLY, "", text)
    return text.strip()

@app.get("/config")
async def config():
    """
        Get the actual config
    """
    return config


@app.get("/parse/{group_id}/{medium_id}/{source_channel}")
async def parse(group_id, medium_id, source_channel):
    """
        Parse the specific source via specific channel
    """
    response = []
    medium = config[group_id][medium_id]
    if source_channel == "rss":
        for link in medium["rss"]:
            for entry in feedparser.parse(link).entries:
                if "remove" in medium:
                    to_remove = medium["remove"]
                else:
                    to_remove = []
                filtered_text = filter_text(entry.summary, to_remove)
                response.append((filtered_text, entry.link))
    elif source_channel == "telegram":
        for link in medium["telegram"]:
            tme_response = requests.get(f"https://t.me/s/{link}")
            parts = tme_response.text.split('<div class="tgme_widget_message_text js-message_text" dir="auto">')
            texts = []
            permalinks = []
            for part in parts[1:]:
                entry = part.split("</div>")[0]
                if "remove" in medium:
                    to_remove = medium["remove"]
                else:
                    to_remove = []
                filtered_text = filter_text(entry, to_remove)
                texts.append(filtered_text)
            parts = tme_response.text.split('data-post="')
            for part in parts[1:]:
                permalink = "https://t.me/" + part.split('\"')[0]
                permalinks.append(permalink)
            for text, link in zip(texts, permalinks):
                response.append((text, link))
    else:
        raise Exception
    return response

@app.get("/parseall")
async def parse_all():
    """
        Parse every source via every channel and return as JSON
    """
    response = {}
    for group_id, group_media in config.items():
        response[group_id] = {}
        for medium_id, medium in group_media.items():
            response[group_id][medium_id] = set()
            for setting in medium.keys():
                if setting in SOURCE_CHANNELS:
                    texts = await parse(group_id, medium_id, setting)
                    response[group_id][medium_id].update(texts)
    return response

async def update():
    """
        Update the feed
    """
    global config
    config = read_config()
    state = await parse_all()
    now = datetime.utcnow()
    for group_id, group_media in state.items():
        if group_id not in buffer: buffer[group_id] = {}
        for medium_id, new_entries in group_media.items():
            if medium_id not in buffer[group_id]: buffer[group_id][medium_id] = []
            buffer[group_id][medium_id] = [
                k for k in buffer[group_id][medium_id]
                if (now - datetime.strptime(k[2], DATETIME_FORMAT)).seconds / 3600
                <= HOURS_TO_KEEP_HISTORY
            ]
            for new_entry in new_entries:
                for old_entry in buffer[group_id][medium_id]:
                    if new_entry[0] == old_entry[0]: break
                else:
                    buffer[group_id][medium_id].append(
                        [
                            new_entry[0],
                            new_entry[1],
                            datetime.utcnow().strftime(DATETIME_FORMAT),
                            detect(new_entry[0])
                        ]
                    )
    await wordstat()
    await similarity()

@app.get("/state")
async def state():
    """
        Get the cached feed
    """
    return buffer

@app.get("/words")
async def words():
    """
        Get the cached word statistics
    """
    return global_wordstat

async def wordstat():
    """
        Get top words for each group
    """
    def is_word_ok(w):
        return True
        w = re.sub(NON_WORD, "", w)
        if len(w) < 2: return False
        if w.lower() in STOP_WORDS: return False
        if "http" in w.lower(): return False
        return True
    global global_wordstat
    wordstat = {}
    for group_id, group_media in buffer.items():
        wordstat[group_id] = {}
        for medium_id, entries in group_media.items():
            wordstat[group_id][medium_id] = {}
            counter = Counter()
            for index, entry in enumerate(entries):
                parsed_material = []
                for word in letters_only(entry[0]).split(" "):
                    word_tags = "_"
                    word_nf = "_"
                    if is_word_ok(word):
                        if entry[3] == "ru":
                            parsed = morph_ru.parse(word)[0]
                        elif entry[3] == "uk":
                            parsed = morph_ua.parse(word)[0]
                        else:
                            continue
                        word_tags = str(parsed.tag)
                        word_nf = str(parsed.normal_form)
                        counter[word_nf + " " + word_tags] += 1
                    parsed_material.append([word, word_nf, word_tags])
                if len(buffer[group_id][medium_id][index]) < 5:
                    buffer[group_id][medium_id][index].append(parsed_material)
            wordstat[group_id][medium_id] = dict(counter)
    global_wordstat = wordstat

async def similarity():
    def linkify(word, nf, tgs):
        r = word.replace("\n","")
        nf = nf.replace("\n","")
        if "Geox" in tgs:
            return "<button onclick='srch(\"" + nf + " \")' class='geox " + nf + "'>" + r + "</button>"
        if "Surn" in tgs:
            return "<button onclick='srch(\"" + nf + " \")' class='surn " + nf + "'>" + r + "</button>"
        if "Orgn" in tgs:
            return "<button onclick='srch(\"" + nf + " \")' class='orgn " + nf + "'>" + r + "</button>"
        return word

    global global_similarity
    entrs = []
    embs = []
    response = {}
    for group_id, group_media in buffer.items():
        for medium_id, entries in group_media.items():
            for index, entry in enumerate(entries):
                entrs.append((
                    group_id,
                    medium_id,
                    " ".join([linkify(x[0], x[1], x[2]) for x in entry[4]]),
                    entry[1],
                    entry[2]
                ))
                embs.append(" ".join([x[1] for x in entry[4]]))
    emb_matrix = [
        [int(fuzz.ratio(embs[i], embs[j])>SIMILARITY_THRESHOLD) if i>j else 0 for i in range(len(embs))]
        for j in range(len(embs))
    ]
    for j in range(len(embs)):
        emb_matrix[j][j] = 1
        for i in range(len(embs)):
            emb_matrix[i][j] = emb_matrix[j][i]
    csrgraph = csr_matrix(emb_matrix)
    n_components, labels = connected_components(csgraph=csrgraph, directed=False, return_labels=True)
    for index, label in enumerate(labels):
        if label not in response:
            response[int(label)] = []
        response[label].append(entrs[int(index)])
    global_similarity = response

@app.get("/similarity")
async def get_similarity():
    return global_similarity

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("index.html", "r") as f:
        return f.read()

@app.get("/bg", response_class=FileResponse)
async def index():
    return FileResponse("background.webp")


@app.on_event('startup')
async def init_data():
    await update()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(update, 'cron', minute=f'*/{UPDATE_INTERVAL}')
    scheduler.start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

