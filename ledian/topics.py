"""Conservative, inspectable rules for classifying and matching news."""

import re
import unicodedata
from difflib import SequenceMatcher


DEFAULT_WATCH = {
    "ChatGPT": ["ChatGPT", "OpenAI", "GPT"],
    "Claude": ["Claude", "Anthropic"],
    "Grok": ["Grok", "xAI"],
    "Gemini": ["Gemini", "DeepMind"],
    "Kimi": ["Kimi", "月之暗面", "Moonshot AI"],
    "DeepSeek": ["DeepSeek", "深度求索"],
    "即梦": ["即梦", "Dreamina", "Seedance"],
}
AI = r"人工智能|大模型|生成式|多模态|智能体|英伟达|NVIDIA|\b(?:AI|LLM|AGI)\b"
INDUSTRY = {
    "开源与模型": r"开源|发布.{0,8}模型|新模型|open.source|new model|model release",
    "芯片与算力": r"算力|芯片|显卡|数据中心|\b(?:GPU|chips?|compute)\b",
    "投融资与并购": r"融资|并购|收购|上市|funding|acquisition|\bIPO\b",
    "政策与版权": r"政策|监管|版权|著作权|copyright|regulat|policy",
    "研究与应用": r"论文|研究|应用|案例|research|case study|benchmark",
}
ACTIONS = {
    "价格与额度": r"降价|涨价|调价|价格调整|额度调整|取消免费|price (?:cut|increase|change)|pricing (?:change|update)|(?:quota|rate limit) (?:change|increase|decrease)",
    "服务中断": r"服务中断|宕机|故障|outage|service disruption|degraded performance|elevated (?:error|failure)|service unavailable",
    "开源": r"开源|open[- ]sourc",
    "发布与上线": r"发布|推出|上线|正式开放|正式可用|introduc(?:e|es|ing)|launch(?:es|ed|ing)?|releas(?:e|es|ed|ing)|now available|general availability|roll(?:ing)? out",
}
RUMOR = re.compile(r"传闻|爆料|网传|疑似|据传|尚未证实|rumou?r|alleged|unconfirmed|leak(?:ed|s)?", re.I)
VERSION = re.compile(
    r"(?<![a-z0-9])(?:gpt|claude|grok|gemini|kimi|deepseek|seedance)[\s-]*(?:opus[\s-]*|sonnet[\s-]*|haiku[\s-]*|v)?"
    r"\d+(?:\.\d+)*(?:[\s-]+(?:pro|flash|mini|nano|lite|preview|thinking|fast|ultra|instant|turbo|max))?"
    r"|(?<![a-z0-9])deepseek[\s-]*[rv]\d+(?:\.\d+)*", re.I,
)


def normalized(value):
    return unicodedata.normalize("NFKC", value).casefold()


def title_key(value):
    return "".join(c for c in normalized(value) if c.isalnum())


def matches(value, term):
    if term.isascii() and term[0].isalnum() and term[-1].isalnum():
        return bool(re.search(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])", value))
    return term in value


def classify(title, description, watch):
    text = normalized(f"{title} {description}")
    names = [
        name for name, aliases in watch.items()
        if any(matches(text, normalized(alias)) for alias in [name, *aliases])
    ]
    industry = [
        name for name, pattern in INDUSTRY.items() if re.search(pattern, text, re.I)
    ] if names or re.search(AI, text, re.I) else []
    return names, industry


def major_change(title, names, industry):
    if not (names or industry) or RUMOR.search(title):
        return None
    return next((action for action, rule in ACTIONS.items() if re.search(rule, title, re.I)), None)


def event_key(title):
    # Exact titles may consolidate items, but alone cannot corroborate copied reporting.
    return title_key(title)


def same_event(left, right):
    left_key, right_key = event_key(left), event_key(right)
    if left_key == right_key and left_key:
        return True
    left_versions = {title_key(v) for v in VERSION.findall(left)}
    right_versions = {title_key(v) for v in VERSION.findall(right)}
    if not left_versions or left_versions != right_versions:
        return False
    left_actions = {name for name, pattern in ACTIONS.items() if re.search(pattern, left, re.I)}
    right_actions = {name for name, pattern in ACTIONS.items() if re.search(pattern, right, re.I)}
    return (
        bool(left_actions)
        and left_actions == right_actions
        and SequenceMatcher(None, left_key, right_key).ratio() >= 0.65
    )
