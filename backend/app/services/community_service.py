import json
import os
import time
import uuid

from .storage_service import atomic_write_json, get_data_path


POSTS_FILE = get_data_path("community_posts.json")


def get_news(force=False):
    return {
        "status": "success",
        "data": [
            {"id": "local-1", "title": "IKUNANCE community service is available", "source": "local", "time": int(time.time())}
        ],
    }


def _load_posts():
    if not os.path.exists(POSTS_FILE):
        return []
    try:
        with open(POSTS_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, list):
            return []
        return [post for post in data if isinstance(post, dict)]
    except Exception:
        return []


def _save_posts(posts):
    atomic_write_json(POSTS_FILE, posts, ensure_ascii=False, indent=2)


def get_posts(page=1, page_size=20, tag=""):
    tag = tag.strip() if isinstance(tag, str) else ""
    posts = _load_posts()
    if tag:
        posts = [post for post in posts if post.get("tag") == tag]
    start = max(page - 1, 0) * page_size
    return {"status": "success", "data": posts[start:start + page_size], "total": len(posts)}


def create_post(author, avatar, avatar_color, content, tag="", owner_id=""):
    content = content.strip() if isinstance(content, str) else ""
    if not content:
        return {"status": "error", "msg": "content is required"}
    if len(content) > 2000:
        return {"status": "error", "msg": "content is too long"}
    tag = tag.strip()[:40] if isinstance(tag, str) else ""
    posts = _load_posts()
    post = {
        "id": str(uuid.uuid4()),
        "author": author,
        "avatar": avatar,
        "avatar_color": avatar_color,
        "content": content,
        "tag": tag,
        "owner_id": owner_id or author,
        "likes": 0,
        "liked_by": [],
        "created_at": int(time.time()),
    }
    posts.insert(0, post)
    _save_posts(posts)
    return {"status": "success", "post": post}


def toggle_like(post_id, user_id):
    posts = _load_posts()
    for post in posts:
        if post.get("id") == post_id:
            liked_by_raw = post.get("liked_by", [])
            liked_by = set(liked_by_raw if isinstance(liked_by_raw, list) else [])
            if user_id in liked_by:
                liked_by.remove(user_id)
            else:
                liked_by.add(user_id)
            post["liked_by"] = sorted(liked_by)
            post["likes"] = len(liked_by)
            _save_posts(posts)
            return {"status": "success", "post": post}
    return {"status": "error", "msg": "post not found"}


def delete_post(post_id, user_id):
    posts = _load_posts()
    target = next((post for post in posts if post.get("id") == post_id), None)
    if not target:
        return {"status": "error", "msg": "post not found"}
    if target.get("owner_id") and target.get("owner_id") != user_id:
        return {"status": "error", "msg": "permission denied"}
    next_posts = [post for post in posts if post.get("id") != post_id]
    _save_posts(next_posts)
    return {"status": "success", "deleted": len(posts) - len(next_posts)}
