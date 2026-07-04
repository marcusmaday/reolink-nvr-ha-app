"""
ai_enrichment.py

Helpers for optional LLM-powered notification enrichment.
"""

from __future__ import annotations

import base64
import json
import logging
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import BaseModel

from settings_store import AIEnrichmentSettings, KnownSubjectSettings

logger = logging.getLogger(__name__)


class AIEnrichmentError(RuntimeError):
    """Raised when the enrichment provider returns an unusable response."""


class NotificationEnrichmentResult(BaseModel):
    safe_summary: str | None = None
    fun_summary: str | None = None
    known_subject_name: str | None = None
    known_subject_confidence: float | None = None
    primary_subject: str | None = None
    activity: str | None = None
    confidence: float | None = None


class OpenAIEnrichmentClient:
    def __init__(self, *, api_key: str, timeout_seconds: int = 8) -> None:
        self._api_key = api_key.strip()
        self._timeout_seconds = max(int(timeout_seconds), 3)

    async def analyze_snapshot(
        self,
        *,
        image_path: Path,
        event_type: str,
        camera_name: str,
        settings: AIEnrichmentSettings,
        known_subjects: list[KnownSubjectSettings],
    ) -> NotificationEnrichmentResult:
        if not self._api_key:
            raise AIEnrichmentError("Missing OpenAI API key")
        if not image_path.exists():
            raise AIEnrichmentError(f"Snapshot file does not exist: {image_path}")

        image_bytes = image_path.read_bytes()
        if not image_bytes:
            raise AIEnrichmentError(f"Snapshot file is empty: {image_path}")

        mime_type = "image/png" if image_path.suffix.lower() == ".png" else "image/jpeg"
        base64_image = base64.b64encode(image_bytes).decode("ascii")
        prompt = self._build_prompt(
            event_type=event_type,
            camera_name=camera_name,
            fun_style=settings.fun_style,
            include_fun_summary=settings.include_fun_summary and settings.fun_style != "off",
            known_subjects=known_subjects,
        )
        payload = {
            "model": settings.model.strip() or "gpt-4.1-mini",
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You help write concise home security notification descriptions. "
                        "Use only details visible in the image and supplied context. "
                        "If you are unsure about an identity, return unknown. "
                        "Do not infer sensitive traits or intent. "
                        "Respond with JSON only."
                    ),
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{mime_type};base64,{base64_image}",
                                "detail": settings.detail if settings.detail in {"low", "high"} else "low",
                            },
                        },
                    ],
                },
            ],
            "max_completion_tokens": 240,
        }

        response_json = await self._request(payload)
        content = self._extract_message_content(response_json)
        parsed = self._parse_json_payload(content)
        return NotificationEnrichmentResult.model_validate(parsed)

    async def _request(self, payload: dict[str, Any]) -> dict[str, Any]:
        timeout = aiohttp.ClientTimeout(
            total=self._timeout_seconds,
            connect=min(self._timeout_seconds, 5),
            sock_connect=min(self._timeout_seconds, 5),
            sock_read=self._timeout_seconds,
        )
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.post("https://api.openai.com/v1/chat/completions", json=payload) as response:
                    if response.status >= 400:
                        body = await response.text()
                        raise AIEnrichmentError(
                            f"OpenAI request failed with {response.status}: {body[:400]}"
                        )
                    return await response.json()
        except aiohttp.ClientError as exc:
            raise AIEnrichmentError(str(exc)) from exc

    @staticmethod
    def _extract_message_content(response_json: dict[str, Any]) -> str:
        choices = response_json.get("choices")
        if not isinstance(choices, list) or not choices:
            raise AIEnrichmentError("OpenAI response did not include any choices")

        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            text_chunks = [
                part.get("text", "")
                for part in content
                if isinstance(part, dict) and part.get("type") in {"text", "output_text"}
            ]
            joined = "".join(text_chunks).strip()
            if joined:
                return joined
        raise AIEnrichmentError("OpenAI response did not include text content")

    @staticmethod
    def _parse_json_payload(raw_content: str) -> dict[str, Any]:
        cleaned = raw_content.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`")
            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:].strip()
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise AIEnrichmentError(f"OpenAI response was not valid JSON: {raw_content[:200]}") from exc
        if not isinstance(parsed, dict):
            raise AIEnrichmentError("OpenAI response JSON was not an object")
        return parsed

    @staticmethod
    def _build_prompt(
        *,
        event_type: str,
        camera_name: str,
        fun_style: str,
        include_fun_summary: bool,
        known_subjects: list[KnownSubjectSettings],
    ) -> str:
        subject_lines = []
        for subject in known_subjects:
            description = subject.description.strip()
            if not description:
                continue
            scope_parts = []
            if subject.channels:
                scope_parts.append(f"channels={','.join(str(channel) for channel in subject.channels)}")
            if subject.event_types:
                scope_parts.append(f"events={','.join(subject.event_types)}")
            scope = f" ({'; '.join(scope_parts)})" if scope_parts else ""
            subject_lines.append(
                f"- {subject.name.strip()} [{subject.subject_type.strip() or 'other'}]{scope}: {description}"
            )

        subject_block = "\n".join(subject_lines) if subject_lines else "- none configured"
        fun_instruction = (
            f'Include a short "fun_summary" with a {fun_style} tone if the image is clear enough.'
            if include_fun_summary
            else 'Set "fun_summary" to an empty string.'
        )

        return (
            f"You are analyzing a single home security snapshot for camera '{camera_name}' with event type '{event_type}'.\n"
            "Return a compact JSON object with exactly these keys:\n"
            '{'
            '"safe_summary": string, '
            '"fun_summary": string, '
            '"known_subject_name": string, '
            '"known_subject_confidence": number, '
            '"primary_subject": string, '
            '"activity": string, '
            '"confidence": number'
            '}\n'
            "Rules:\n"
            "- safe_summary must be one short sentence and grounded only in the image.\n"
            "- known_subject_name must be one configured subject name or unknown.\n"
            "- known_subject_confidence and confidence must be numbers from 0 to 1.\n"
            "- If identity is unclear, use known_subject_name='unknown' and keep confidence modest.\n"
            "- Never mention race, age, attractiveness, emotion, or criminal intent.\n"
            f"- {fun_instruction}\n"
            "Configured known subjects:\n"
            f"{subject_block}"
        )
