"""
Knowledge Builder v2.0 (2026-03-23) — Daily Concept Extraction Pipeline
Reads Pending entries from Conversations Log → extracts concepts via Claude Sonnet →
writes rich textbook-style entries to Knowledge Builder Concepts DB → flips status to Extracted.

Deploy on Railway with cron schedule.
"""

import os
import json
import re
import requests
import anthropic
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

# Conversations Log — source DB (read Pending entries)
CONVERSATIONS_DB_ID = os.environ.get(
    "CONVERSATIONS_DB_ID", "6a7eea2f15cb443280a1f3569d1d494b"
)

# Knowledge Builder Concepts — target DB (write concept entries)
CONCEPTS_DB_ID = os.environ.get(
    "CONCEPTS_DB_ID", "b183d43d12ee4ed692f5b682bf4539f7"
)

NOTION_API = "https://api.notion.com/v1"
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

VERSION = "2.0"

# ─── NOTION HELPERS ───────────────────────────────────────────────────────────


def query_pending_conversations():
    """Fetch all Conversations Log entries with Knowledge Builder Status = Pending."""
    url = f"{NOTION_API}/databases/{CONVERSATIONS_DB_ID}/query"
    payload = {
        "filter": {
            "property": "Knowledge Builder Status",
            "select": {"equals": "Pending"},
        }
    }
    results = []
    has_more = True
    start_cursor = None

    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor
        resp = requests.post(url, headers=NOTION_HEADERS, json=payload)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return results


def extract_text_property(page, prop_name):
    """Extract plain text from a Notion rich_text or title property."""
    prop = page.get("properties", {}).get(prop_name, {})
    prop_type = prop.get("type", "")

    if prop_type == "title":
        parts = prop.get("title", [])
    elif prop_type == "rich_text":
        parts = prop.get("rich_text", [])
    else:
        return ""

    return "".join(p.get("plain_text", "") for p in parts)


def extract_select_property(page, prop_name):
    """Extract the name from a select property."""
    prop = page.get("properties", {}).get(prop_name, {})
    sel = prop.get("select")
    if sel:
        return sel.get("name", "")
    return ""


def get_page_content(page_id):
    """Fetch the full block children (body) of a page as plain text."""
    url = f"{NOTION_API}/blocks/{page_id}/children?page_size=100"
    resp = requests.get(url, headers=NOTION_HEADERS)
    resp.raise_for_status()
    blocks = resp.json().get("results", [])

    text_parts = []
    for block in blocks:
        block_type = block.get("type", "")
        block_data = block.get(block_type, {})
        rich_text = block_data.get("rich_text", [])
        line = "".join(rt.get("plain_text", "") for rt in rich_text)
        if line.strip():
            text_parts.append(line.strip())

    return "\n".join(text_parts)


def build_conversation_context(page):
    """Assemble all available text from a Conversations Log entry."""
    page_id = page["id"]
    title = extract_text_property(page, "Title")
    overview = extract_text_property(page, "Overview")
    key_concepts = extract_text_property(page, "Key Concepts")
    what_built = extract_text_property(page, "What Was Built or Decided")
    open_threads = extract_text_property(page, "Open Threads")
    project = extract_select_property(page, "Project")

    # Also fetch the page body for richer context
    body = get_page_content(page_id)

    parts = [f"# {title}"]
    if project:
        parts.append(f"Project: {project}")
    if overview:
        parts.append(f"\n## Overview\n{overview}")
    if key_concepts:
        parts.append(f"\n## Key Concepts Mentioned\n{key_concepts}")
    if what_built:
        parts.append(f"\n## What Was Built or Decided\n{what_built}")
    if open_threads:
        parts.append(f"\n## Open Threads\n{open_threads}")
    if body:
        parts.append(f"\n## Full Page Content\n{body}")

    return "\n".join(parts), title, project


def update_status_to_extracted(page_id):
    """Set Knowledge Builder Status to Extracted."""
    url = f"{NOTION_API}/pages/{page_id}"
    payload = {
        "properties": {
            "Knowledge Builder Status": {"select": {"name": "Extracted"}}
        }
    }
    resp = requests.patch(url, headers=NOTION_HEADERS, json=payload)
    resp.raise_for_status()


def create_concept_page(concept_data, project, source_title):
    """Write a single concept entry to the Knowledge Builder Concepts DB."""
    url = f"{NOTION_API}/pages"

    # Build properties — keep them short per Justin's rule
    properties = {
        "Concept": {
            "title": [
                {"text": {"content": concept_data.get("concept_name", "Untitled")[:80]}}
            ]
        },
        "Date": {"date": {"start": datetime.now().strftime("%Y-%m-%d")}},
        "Status": {"select": {"name": "New"}},
        "One-Line Definition": {
            "rich_text": [
                {"text": {"content": concept_data.get("one_line", "")[:200]}}
            ]
        },
        "Source Conversation": {
            "rich_text": [{"text": {"content": source_title[:200]}}]
        },
    }

    # Map project name if it matches a valid option
    valid_projects = ["IP Monitor", "C2N", "Accounting", "General Learning", "Other"]
    if project in valid_projects:
        properties["Project"] = {"select": {"name": project}}

    # Build the rich page body — this is where the textbook content lives
    body_md = concept_data.get("page_body", "")

    payload = {
        "parent": {"database_id": CONCEPTS_DB_ID},
        "properties": properties,
        "children": markdown_to_notion_blocks(body_md),
    }

    resp = requests.post(url, headers=NOTION_HEADERS, json=payload)
    resp.raise_for_status()
    return resp.json().get("id", "")


# ─── MARKDOWN → NOTION BLOCKS ────────────────────────────────────────────────


def markdown_to_notion_blocks(md_text):
    """Convert simple markdown to Notion block children.
    
    Handles: ## headings, **bold**, plain paragraphs, toggle blocks,
    horizontal rules, and bulleted list items.
    """
    blocks = []
    lines = md_text.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Skip empty lines
        if not line:
            i += 1
            continue

        # Horizontal rule
        if line == "---":
            blocks.append({"object": "block", "type": "divider", "divider": {}})
            i += 1
            continue

        # Heading 2
        if line.startswith("## "):
            text = line[3:].strip()
            blocks.append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": parse_rich_text(text)},
            })
            i += 1
            continue

        # Heading 3
        if line.startswith("### "):
            text = line[4:].strip()
            blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {"rich_text": parse_rich_text(text)},
            })
            i += 1
            continue

        # Toggle block: <toggle>Summary text</toggle> followed by indented content
        if line.startswith("<toggle>") and line.endswith("</toggle>"):
            summary = line[8:-9].strip()
            # Collect indented lines that follow as toggle children
            toggle_children = []
            i += 1
            while i < len(lines) and (lines[i].startswith("  ") or lines[i].startswith("\t")):
                child_line = lines[i].strip()
                if child_line:
                    toggle_children.append({
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {"rich_text": parse_rich_text(child_line)},
                    })
                i += 1

            blocks.append({
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": parse_rich_text(summary),
                    "children": toggle_children if toggle_children else [
                        {
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {"rich_text": [{"type": "text", "text": {"content": " "}}]},
                        }
                    ],
                },
            })
            continue

        # Bulleted list item
        if line.startswith("- ") or line.startswith("* "):
            text = line[2:].strip()
            blocks.append({
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": parse_rich_text(text)},
            })
            i += 1
            continue

        # Callout block: > callout text
        if line.startswith("> "):
            text = line[2:].strip()
            blocks.append({
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": parse_rich_text(text),
                    "icon": {"type": "emoji", "emoji": "💡"},
                },
            })
            i += 1
            continue

        # Default: paragraph
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": parse_rich_text(line)},
        })
        i += 1

    return blocks


def parse_rich_text(text):
    """Parse markdown-style bold (**text**) into Notion rich_text annotations."""
    parts = []
    pattern = r"\*\*(.+?)\*\*"
    last_end = 0

    for match in re.finditer(pattern, text):
        # Text before the bold
        before = text[last_end : match.start()]
        if before:
            parts.append({
                "type": "text",
                "text": {"content": before},
            })
        # Bold text
        parts.append({
            "type": "text",
            "text": {"content": match.group(1)},
            "annotations": {"bold": True},
        })
        last_end = match.end()

    # Remaining text after last bold
    remaining = text[last_end:]
    if remaining:
        parts.append({
            "type": "text",
            "text": {"content": remaining},
        })

    # If no parts were created, return the full text as-is
    if not parts:
        parts.append({"type": "text", "text": {"content": text}})

    return parts


# ─── CLAUDE EXTRACTION ────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """You are a technical educator writing for a smart, non-engineer founder who learns by building real systems. He understands Make.com flows, webhooks, JSON, and Notion practically — but doesn't write code from scratch. He learns best when concepts are grounded in things he actually built.

Your job: extract 2-5 distinct technical concepts from the conversation summary below. For each concept, produce a rich, textbook-quality entry that takes about 5-6 minutes to read.

IMPORTANT FORMATTING RULES:
- Use ## for section headings
- Use **bold** for emphasis
- Use - for bullet points
- Use <toggle>Question text here</toggle> followed by indented answer lines for Socratic Q&A toggles
- Use > for callout/highlight blocks
- Use --- for horizontal dividers between sections

For EACH concept, structure the page body EXACTLY like this:

## What It Is
A clear 2-3 sentence explanation. No jargon without immediate plain-language follow-up. Write like you're explaining to a smart friend over coffee.

## How It Showed Up in Your Work
Ground this in the SPECIFIC project and situation from the conversation. Reference actual tools, databases, and decisions by name. This section should make the reader say "oh, THAT's what that was."

## How It Actually Works
The mechanical explanation. Walk through what happens step by step when this concept is in play. Use a concrete example — ideally from the conversation context. If there are moving parts, explain each one and how they connect. Think of this like a "How It's Made" segment. 3-5 paragraphs.

## What Happens If You Ignore It
Real consequences. Not hypothetical doom — actual problems that would show up. Reference situations from the conversation if possible. Be specific about what breaks, costs money, or wastes time.

## Test Yourself
<toggle>Question 1: A Socratic question that tests understanding — not trivia, but "could you explain this to Jordan?"</toggle>
  Answer paragraph that reveals the answer and adds one extra insight.

<toggle>Question 2: A harder question that tests whether you could apply this concept in a new situation</toggle>
  Answer paragraph with the answer and a connection to something else in the system.

## Go Deeper
- **Search term 1** — one-line note on what you'll find and why it matters for your stack
- **Search term 2** — one-line note
- **Search term 3** — one-line note (optional)

## Related Concepts
- **Concept name** — one sentence on how it connects to this one

---

Return your response as a JSON array. Each element must have:
- "concept_name": string (2-5 words, the concept title)
- "one_line": string (one sentence definition, under 200 chars)
- "page_body": string (the full markdown content above — all sections, toggles, everything)

Return ONLY the JSON array. No preamble, no markdown fences, no explanation outside the array."""


def extract_concepts(conversation_text, title, project):
    """Call Claude Sonnet to extract concepts from a conversation summary."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    user_message = f"""Here is the conversation summary to extract concepts from:

---
{conversation_text}
---

Extract 2-5 technical concepts that would be most valuable for a founder building automation systems to understand deeply. Focus on concepts that are:
1. Transferable — useful beyond just this one project
2. Practical — something he'll encounter again
3. Non-obvious — not just "what is an API" level basics

Project context: {project or 'General'}
Conversation title: {title}"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=8000,
        system=[{
            "type": "text",
            "text": EXTRACTION_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[
            {"role": "user", "content": user_message},
            {"role": "assistant", "content": "["},
        ],
    )

    raw = "[" + response.content[0].text
    return parse_json_response(raw)


def parse_json_response(raw):
    """Parse JSON with layered fallback — same pattern as C2N."""
    # Attempt 1: direct parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Attempt 2: strip markdown code fences
    cleaned = re.sub(r"```json\s*", "", raw)
    cleaned = re.sub(r"```\s*$", "", cleaned).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Attempt 3: find the outermost array brackets
    start = raw.find("[")
    end = raw.rfind("]")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(raw[start : end + 1])
        except json.JSONDecodeError:
            pass

    print(f"  [ERROR] Could not parse JSON from Claude response")
    print(f"  Raw response (first 500 chars): {raw[:500]}")
    return []


# ─── MAIN ─────────────────────────────────────────────────────────────────────


def main():
    print(f"Knowledge Builder v{VERSION} — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # 1. Fetch pending conversations
    print("\n1. Querying Conversations Log for Pending entries...")
    pending = query_pending_conversations()
    print(f"   Found {len(pending)} pending entries")

    if not pending:
        print("\n   Nothing to process. Done.")
        return

    total_concepts = 0

    # 2. Process each conversation
    for idx, page in enumerate(pending):
        page_id = page["id"]
        conversation_text, title, project = build_conversation_context(page)
        print(f"\n2.{idx+1} Processing: {title}")
        print(f"     Project: {project or 'None'}")
        print(f"     Context length: {len(conversation_text)} chars")

        # 3. Extract concepts via Claude Sonnet
        print(f"     Extracting concepts via Claude Sonnet...")
        try:
            concepts = extract_concepts(conversation_text, title, project)
        except Exception as e:
            print(f"     [ERROR] Claude API call failed: {e}")
            continue

        if not concepts:
            print(f"     [WARN] No concepts extracted — skipping")
            continue

        print(f"     Extracted {len(concepts)} concepts:")

        # 4. Write each concept to the Concepts DB
        for c_idx, concept in enumerate(concepts):
            name = concept.get("concept_name", "Untitled")
            one_line = concept.get("one_line", "")
            print(f"       [{c_idx+1}] {name}")

            try:
                new_page_id = create_concept_page(concept, project, title)
                print(f"           Written to Notion: {new_page_id}")
                total_concepts += 1
            except Exception as e:
                print(f"           [ERROR] Failed to write: {e}")

        # 5. Mark conversation as Extracted
        try:
            update_status_to_extracted(page_id)
            print(f"     Status flipped to Extracted")
        except Exception as e:
            print(f"     [ERROR] Failed to update status: {e}")

    # Summary
    print("\n" + "=" * 60)
    print(f"Done. Processed {len(pending)} conversations, wrote {total_concepts} concepts.")


if __name__ == "__main__":
    main()
