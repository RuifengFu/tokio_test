#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

DEFAULT_OWNER = "tokio-rs"
DEFAULT_REPO = "tokio"
DEFAULT_PER_PAGE = 100
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "issues.json"
API_ROOT = "https://api.github.com"


def load_issue_store(path: Path) -> Tuple[Dict[str, Any], Dict[int, Dict[str, Any]]]:
    if not path.exists():
        return {"issues": [], "owner": DEFAULT_OWNER, "repo": DEFAULT_REPO}, {}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    raw_issues = data.get("issues", [])
    issues_map: Dict[int, Dict[str, Any]] = {}
    if isinstance(raw_issues, dict):
        for key, value in raw_issues.items():
            try:
                number = int(key)
            except ValueError:
                continue
            if isinstance(value, dict):
                issues_map[number] = value
    else:
        for issue in raw_issues:
            if isinstance(issue, dict) and "number" in issue:
                issues_map[int(issue["number"])] = issue
    return data, issues_map


def save_issue_store(path: Path, data: Dict[str, Any], issues: Dict[int, Dict[str, Any]]) -> None:
    data = dict(data)
    data["issues"] = sorted(issues.values(), key=lambda item: item.get("number", 0))
    data["fetched_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    data["issue_count"] = len(data["issues"])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)


def get_token() -> Optional[str]:
    return os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")


def parse_github_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None


def to_github_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def latest_updated_at(issues: Dict[int, Dict[str, Any]]) -> Optional[str]:
    timestamps = [
        parse_github_datetime(issue.get("updated_at", ""))
        for issue in issues.values()
        if issue.get("updated_at")
    ]
    timestamps = [value for value in timestamps if value]
    if not timestamps:
        return None
    return to_github_iso(max(timestamps))


def request_json(url: str, token: Optional[str]) -> Tuple[Any, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "tokio-issue-scraper",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
            link = response.headers.get("Link", "")
            return payload, link
    except HTTPError as exc:
        details = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"GitHub API request failed ({exc.code}): {details}") from exc
    except URLError as exc:
        raise RuntimeError(f"GitHub API request failed: {exc.reason}") from exc


def parse_link_header(link_header: str) -> Dict[str, str]:
    links: Dict[str, str] = {}
    for part in link_header.split(","):
        section = [item.strip() for item in part.split(";") if item.strip()]
        if len(section) < 2:
            continue
        url = section[0]
        if url.startswith("<") and url.endswith(">"):
            url = url[1:-1]
        rel = None
        for item in section[1:]:
            if item.startswith("rel="):
                rel = item.split("=", 1)[1].strip('"')
                break
        if rel:
            links[rel] = url
    return links


def build_url(base: str, params: Dict[str, Any]) -> str:
    parsed = urlparse(base)
    query = parse_qs(parsed.query)
    for key, value in params.items():
        if value is None:
            continue
        query[key] = [str(value)]
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def fetch_paginated(url: str, token: Optional[str]) -> List[Any]:
    results: List[Any] = []
    next_url = url
    while next_url:
        payload, link = request_json(next_url, token)
        if isinstance(payload, dict):
            results.extend(payload.get("items", []))
        else:
            results.extend(payload)
        next_url = parse_link_header(link).get("next")
    return results


def build_user(user: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not user:
        return None
    return {
        "login": user.get("login"),
        "id": user.get("id"),
        "type": user.get("type"),
        "url": user.get("html_url"),
    }


def build_issue_record(issue: Dict[str, Any], comments: List[Dict[str, Any]]) -> Dict[str, Any]:
    labels = [
        {
            "name": label.get("name"),
            "description": label.get("description"),
            "color": label.get("color"),
        }
        for label in issue.get("labels", [])
        if isinstance(label, dict)
    ]
    assignees = [
        build_user(user)
        for user in issue.get("assignees", [])
        if isinstance(user, dict)
    ]
    return {
        "number": issue.get("number"),
        "title": issue.get("title"),
        "state": issue.get("state"),
        "locked": issue.get("locked"),
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "closed_at": issue.get("closed_at"),
        "url": issue.get("html_url"),
        "user": build_user(issue.get("user")),
        "labels": labels,
        "assignees": [assignee for assignee in assignees if assignee],
        "comments_count": issue.get("comments"),
        "body": issue.get("body"),
        "comments": comments,
    }


def build_comment_record(comment: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": comment.get("id"),
        "user": build_user(comment.get("user")),
        "created_at": comment.get("created_at"),
        "updated_at": comment.get("updated_at"),
        "body": comment.get("body"),
    }


def fetch_issue_comments(comments_url: str, token: Optional[str]) -> List[Dict[str, Any]]:
    url = build_url(comments_url, {"per_page": DEFAULT_PER_PAGE})
    comments = fetch_paginated(url, token)
    return [build_comment_record(comment) for comment in comments if isinstance(comment, dict)]


def fetch_issues(owner: str, repo: str, token: Optional[str], since: Optional[str]) -> List[Dict[str, Any]]:
    base_url = f"{API_ROOT}/repos/{owner}/{repo}/issues"
    params = {
        "state": "all",
        "per_page": DEFAULT_PER_PAGE,
        "sort": "updated",
        "direction": "desc",
        "since": since,
    }
    url = build_url(base_url, params)
    issues = fetch_paginated(url, token)
    return [issue for issue in issues if isinstance(issue, dict) and "pull_request" not in issue]


def format_bar(value: int, maximum: int, width: int = 40) -> str:
    if maximum <= 0:
        return ""
    length = max(1, int(round((value / maximum) * width))) if value else 0
    return "█" * length


def summarize_labels(issues: Iterable[Dict[str, Any]], top: Optional[int]) -> None:
    counter = Counter(label.get("name") for issue in issues for label in issue.get("labels", []))
    counter.pop(None, None)
    if not counter:
        print("No labels found.")
        return
    most_common = counter.most_common(top)
    maximum = most_common[0][1] if most_common else 0
    print("Label summary:")
    for name, count in most_common:
        bar = format_bar(count, maximum)
        print(f"- {name}: {count} {bar}")


def summarize_states(issues: Iterable[Dict[str, Any]]) -> None:
    counter = Counter(issue.get("state") for issue in issues)
    counter.pop(None, None)
    if not counter:
        return
    print("State summary:")
    for state, count in counter.items():
        print(f"- {state}: {count}")


def filter_by_label(issues: Iterable[Dict[str, Any]], label: str) -> List[Dict[str, Any]]:
    return [
        issue
        for issue in issues
        if any(label == entry.get("name") for entry in issue.get("labels", []))
    ]


def analyze_store(path: Path, label: Optional[str], top: Optional[int]) -> None:
    data, issues_map = load_issue_store(path)
    issues = list(issues_map.values())
    if not issues:
        print(f"No issues found in {path}. Run the fetch command first.")
        return
    print(f"Analyzing {len(issues)} issues from {data.get('owner', '')}/{data.get('repo', '')}.")
    summarize_states(issues)
    summarize_labels(issues, top)
    if label:
        filtered = filter_by_label(issues, label)
        print(f"\nIssues with label '{label}': {len(filtered)}")
        for issue in sorted(filtered, key=lambda item: item.get("number", 0)):
            print(f"- #{issue.get('number')}: {issue.get('title')}")


def run_fetch(args: argparse.Namespace) -> None:
    output = Path(args.output)
    data, issues_map = load_issue_store(output)
    if args.owner:
        data["owner"] = args.owner
    if args.repo:
        data["repo"] = args.repo
    token = get_token()
    since = None if args.full_refresh else (args.since or latest_updated_at(issues_map))
    if since:
        print(f"Fetching issues updated since {since}...")
    else:
        print("Fetching all issues... (this may take a while)")
    issues = fetch_issues(data["owner"], data["repo"], token, since)
    for issue in issues:
        number = issue.get("number")
        if not isinstance(number, int):
            continue
        comments: List[Dict[str, Any]] = []
        if args.include_comments and issue.get("comments", 0):
            comments = fetch_issue_comments(issue.get("comments_url"), token)
        record = build_issue_record(issue, comments)
        issues_map[number] = record
    save_issue_store(output, data, issues_map)
    print(f"Saved {len(issues_map)} issues to {output}.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch and analyze GitHub issues for the tokio repository.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch", help="Fetch issues and store them locally")
    fetch_parser.add_argument("--owner", default=DEFAULT_OWNER, help="GitHub owner (default: tokio-rs)")
    fetch_parser.add_argument("--repo", default=DEFAULT_REPO, help="GitHub repository (default: tokio)")
    fetch_parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path to the output JSON file")
    fetch_parser.add_argument(
        "--since",
        help="ISO 8601 timestamp to fetch updates since (overrides incremental mode)",
    )
    fetch_parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore local cache and fetch all issues",
    )
    fetch_parser.add_argument(
        "--no-comments",
        dest="include_comments",
        action="store_false",
        help="Skip fetching issue comments",
    )
    fetch_parser.set_defaults(include_comments=True)

    analyze_parser = subparsers.add_parser("analyze", help="Analyze stored issues")
    analyze_parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path to the output JSON file")
    analyze_parser.add_argument("--label", help="Filter issues by label")
    analyze_parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Limit label summary to the top N labels",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "fetch":
            run_fetch(args)
        elif args.command == "analyze":
            analyze_store(Path(args.output), args.label, args.top)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
