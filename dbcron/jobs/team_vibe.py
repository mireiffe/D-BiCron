"""Team Vibe Check — randomly generate team members' current status."""

from __future__ import annotations

import os
import random
from datetime import datetime

from .base import Job, JobResult

MOODS = [
    ("😎 컨디션 최상", "오늘 뭐든 할 수 있을 것 같음"),
    ("🔥 열정 만수르", "퇴근이 뭔지 모르는 상태"),
    ("😴 졸림 주의보", "커피 3잔째인데 아직 안 깸"),
    ("🤔 깊은 생각 중", "모니터 응시한 지 30분째"),
    ("😤 빡침 게이지 80%", "빌드가 또 터졌다"),
    ("🥳 신남", "점심 맛있게 먹음"),
    ("😑 무표정", "하..."),
    ("🤯 머리 과부하", "PR 리뷰 12개 밀림"),
    ("😊 평온", "오늘은 조용히 코딩하는 날"),
    ("💀 체력 방전", "어제 새벽 3시에 잠"),
    ("🧊 쿨한 상태", "다 괜찮음"),
    ("🫠 녹는 중", "에어컨 좀 틀어줘..."),
]

FOODS = [
    "🍜 짬뽕", "🍕 피자", "🍣 초밥", "🍔 버거킹 와퍼",
    "🥘 김치찌개", "🍗 BBQ 황금올리브", "🍝 까르보나라",
    "🌮 타코", "🍱 편의점 도시락", "☕ 일단 커피부터",
    "🍰 딸기 케이크", "🥤 버블티", "🍖 삼겹살",
    "🍛 카레", "🥟 만두", "🫕 부대찌개",
    "🍙 참치 삼각김밥", "🧇 와플", "🍲 순두부찌개",
    "🍦 메로나", "🥩 소고기 국밥", "🌯 브리또",
]

ACTIVITIES = [
    "코드 리뷰 중", "버그 잡는 중", "커피 타러 가는 중",
    "슬랙 확인 중", "미팅 대기 중", "테스트 작성 중",
    "문서 업데이트 중", "점심 메뉴 고르는 중", "빌드 기다리는 중",
    "디버깅 3시간째", "리팩토링 중", "Stack Overflow 서핑 중",
    "PR 올리는 중", "야근 각 재는 중", "산책 갔다 옴",
    "GPT한테 질문 중", "배포 파이프라인 만지는 중",
    "자리에서 스트레칭 중", "커밋 메시지 고민 중",
]

QUOTES = [
    "오늘은 될 것 같은 느낌!",
    "퇴근하고 싶다...",
    "이 버그 누가 만든 거야... 아 나구나",
    "커밋 메시지 뭐라고 쓰지",
    "이것만 하고 퇴근한다 (3시간 전에도 같은 말 함)",
    "내일의 나에게 맡기자",
    "왜 로컬에서는 되는데...",
    "오늘 저녁 뭐 먹지",
    "금요일까지만 버티자",
    "이번 스프린트는 진짜 빡세다",
    "테스트 통과! 기분 좋다",
    "이거 한 줄이면 될 줄 알았는데...",
    "회의 끝나고 진짜 일 시작한다",
    "어제 짠 코드가 천재의 코드임",
    "아 점심 너무 많이 먹었다",
    "오류 메시지 읽기 싫어...",
    "다음 주에는 진짜 정리한다",
]

DEFAULT_MEMBERS = ["Alice", "Bob", "Charlie"]


def _progress_bar(pct: int, width: int = 20) -> str:
    filled = round(width * pct / 100)
    return "█" * filled + "░" * (width - filled)


def _generate_member_block(name: str) -> str:
    mood_label, mood_desc = random.choice(MOODS)
    productivity = random.randint(10, 100)
    food = random.choice(FOODS)
    activity = random.choice(ACTIVITIES)
    quote = random.choice(QUOTES)

    bar = _progress_bar(productivity)

    lines = [
        f"  ▸ {name}",
        f"    기분       {mood_label}",
        f"               \"{mood_desc}\"",
        f"    업무능률   {bar}  {productivity}%",
        f"    하는 일    {activity}",
        f"    먹고싶은것 {food}",
        f"    💬 \"{quote}\"",
    ]
    return "\n".join(lines)


class TeamVibeJob(Job):
    name = "team_vibe"
    label = "Team Vibe Check"
    description = "팀원들의 현재 상태를 랜덤으로 생성합니다"
    default_args: dict = {}

    def run(self, **kwargs) -> JobResult:
        raw = os.getenv("TEAM_MEMBERS", "")
        members = [m.strip() for m in raw.split(",") if m.strip()]
        if not members:
            members = DEFAULT_MEMBERS

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        header = (
            "\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  TEAM VIBE CHECK  //  {now}\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        )

        blocks = [_generate_member_block(m) for m in members]
        body = "\n\n".join(blocks)

        footer = (
            "\n\n"
            "─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─\n"
            f"  {len(members)} members checked  ·  powered by D-BiCron\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        )

        report = header + "\n" + body + footer
        print(report)

        return JobResult(
            success=True,
            message=f"Vibe checked for {len(members)} members",
            rows_affected=len(members),
        )
