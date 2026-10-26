"""AI Provider abstraction for Parquet Guard.

Provides:
1. BaseAIProvider interface.
2. MockAIProvider for deterministic, offline, zero-network unit and CI testing.
3. OpenAICompatibleProvider for live model invocation via standard library urllib (no mandatory external SDKs).
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class ProviderError(Exception):
    """Base exception for AI provider invocation errors."""

    pass


class ProviderTimeoutError(ProviderError):
    """Raised when an AI request times out."""

    pass


class ProviderRateLimitError(ProviderError):
    """Raised when an AI request hits HTTP 429 rate limit."""

    pass


class ProviderAuthError(ProviderError):
    """Raised when credentials are missing or invalid (HTTP 401/403)."""

    pass


class ProviderResponse:
    """Standardized response from an AI provider."""

    def __init__(
        self,
        content: str,
        provider_name: str,
        model_id: str,
        latency_ms: float,
        tokens_prompt: Optional[int] = None,
        tokens_completion: Optional[int] = None,
    ) -> None:
        self.content = content
        self.provider_name = provider_name
        self.model_id = model_id
        self.latency_ms = latency_ms
        self.tokens_prompt = tokens_prompt
        self.tokens_completion = tokens_completion


class BaseAIProvider(ABC):
    """Abstract interface for AI model providers."""

    @abstractmethod
    def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        response_format_json: bool = True,
    ) -> ProviderResponse:
        """Submit prompt to model and return standardized ProviderResponse."""
        pass


class MockAIProvider(BaseAIProvider):
    """Deterministic, offline mock provider for testing and evaluation."""

    def __init__(
        self,
        model_id: str = "mock-v1",
        default_latency_ms: float = 12.0,
        custom_responses: Optional[Dict[str, str]] = None,
        simulate_error: Optional[Exception] = None,
    ) -> None:
        self.model_id = model_id
        self.default_latency_ms = default_latency_ms
        self.custom_responses = custom_responses or {}
        self.simulate_error = simulate_error
        self.invocation_count = 0

    def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        response_format_json: bool = True,
    ) -> ProviderResponse:
        self.invocation_count += 1

        if self.simulate_error:
            raise self.simulate_error

        # Check for custom matched response by key substrings
        for key, resp in self.custom_responses.items():
            if key in prompt:
                return ProviderResponse(
                    content=resp,
                    provider_name="mock",
                    model_id=self.model_id,
                    latency_ms=self.default_latency_ms,
                    tokens_prompt=len(prompt) // 4,
                    tokens_completion=len(resp) // 4,
                )

        # Extract findings from prompt XML for grounded mock response
        import html
        import re

        finding_matches = re.findall(
            r'<finding id="([^"]+)">\s*<field_path>([^<]+)</field_path>\s*<change_type>([^<]+)</change_type>\s*<severity>([^<]+)</severity>',
            prompt,
        )
        confirmed_changes = []
        consequences = []
        cited_ids = []

        unescaped_prompt_low = html.unescape(prompt).lower()
        is_breaking = "is_breaking>true" in prompt.lower()
        has_suspicious = any(
            tok in unescaped_prompt_low
            for tok in [
                "ignore",
                "override",
                "bypass",
                "drop table",
                "system:",
                "<system>",
                "grant_access",
            ]
        )

        for fid, raw_fpath, ctype, sev in finding_matches:
            fpath = html.unescape(raw_fpath)
            cited_ids.append(fid)
            confirmed_changes.append(
                {
                    "finding_id": fid,
                    "field_path": fpath,
                    "nature_of_change": f"Schema modified field '{fpath}' ({ctype}).",
                    "severity": sev,
                    "evidence_quote": f"Finding {fid} on {fpath}",
                }
            )
            cat = (
                "QUERY_FAILURE"
                if sev == "BREAKING"
                else ("TEMPORAL_OVERFLOW" if "overflow" in prompt.lower() else "SAFE_ADDITION")
            )
            consequences.append(
                {
                    "category": cat,
                    "affected_engines": ["DuckDB", "PyArrow"],
                    "description": f"Downstream query engines processing '{fpath}' may encounter unexpected incompatibility.",
                    "linked_finding_ids": [fid],
                }
            )

        if not confirmed_changes:
            confirmed_changes = [
                {
                    "finding_id": "F001",
                    "field_path": "dataset.field",
                    "nature_of_change": "Detected field modification in schema.",
                    "severity": "BREAKING" if is_breaking else "SAFE",
                    "evidence_quote": "Finding F001 structural discrepancy",
                }
            ]
            consequences = [
                {
                    "category": "QUERY_FAILURE" if is_breaking else "SAFE_ADDITION",
                    "affected_engines": ["DuckDB", "PyArrow"],
                    "description": "Downstream query engines may encounter unexpected type incompatibility.",
                    "linked_finding_ids": ["F001"],
                }
            ]
            cited_ids = ["F001"]

        verdict_m = re.search(r"<deterministic_verdict>([^<]+)</deterministic_verdict>", prompt)
        verdict_str = verdict_m.group(1) if verdict_m else ("INCOMPATIBLE" if is_breaking else "VALID")

        # Default synthetic diagnosis response
        default_resp = json.dumps(
            {
                "summary": f"Mock analysis: Schema contains {len(confirmed_changes)} confirmed structural discrepancies.",
                "deterministic_verdict": verdict_str,
                "is_blocking_ci": is_breaking,
                "findings_count": len(confirmed_changes),
                "confirmed_changes": confirmed_changes,
                "potential_consequences": consequences,
                "all_cited_finding_ids": cited_ids,
                "confidence_assessment": "HIGH",
                "assumptions": ["Assumed baseline contract represents true production state."],
                "hypotheses": ["Column modification may originate from an uncoordinated upstream update."],
                "unknowns_and_limitations": [
                    "Zero row data was scanned; physical value distribution cannot be verified."
                ],
                "recommended_verification_steps": [
                    "Run PyArrow schema comparison assertions against historical partitions."
                ],
                "candidate_remediations": [
                    {
                        "strategy_name": "Update Golden Contract",
                        "action_type": "UPDATE_CONTRACT",
                        "description": "If evolution is intentional, regenerate and commit new SchemaContract.",
                        "feasibility": "HIGH",
                        "tradeoffs": "Requires coordinating downstream readers to verify compatibility.",
                    }
                ],
                "suspicious_input_detected": has_suspicious,
            },
            indent=2,
        )

        return ProviderResponse(
            content=default_resp,
            provider_name="mock",
            model_id=self.model_id,
            latency_ms=self.default_latency_ms,
            tokens_prompt=len(prompt) // 4,
            tokens_completion=len(default_resp) // 4,
        )


class OpenAICompatibleProvider(BaseAIProvider):
    """OpenAI API compatible provider implemented with standard Python urllib.

    Zero external SDK requirements. Reads API key from OPENAI_API_KEY environment variable.
    """

    def __init__(
        self,
        model_id: str = "gpt-4o-mini",
        api_key: Optional[str] = None,
        base_url: str = "https://api.openai.com/v1",
        timeout_seconds: float = 30.0,
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> None:
        self.model_id = model_id
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        cleaned_url = base_url.strip().rstrip("/")
        if not (
            cleaned_url.startswith("https://")
            or cleaned_url.startswith("http://localhost")
            or cleaned_url.startswith("http://127.0.0.1")
        ):
            raise ValueError(
                f"Insecure or unsupported provider base_url '{base_url}'. Only 'https://' (or localhost HTTP) is permitted."
            )
        self.base_url = cleaned_url
        self.timeout_seconds = timeout_seconds
        self.temperature = temperature
        self.max_tokens = max_tokens

    def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        response_format_json: bool = True,
    ) -> ProviderResponse:
        if not self.api_key:
            raise ProviderAuthError(
                "OPENAI_API_KEY environment variable is not set and no API key was provided."
            )

        endpoint = f"{self.base_url}/chat/completions"
        messages: list[dict[str, str]] = []

        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload: dict[str, Any] = {
            "model": self.model_id,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }

        if response_format_json:
            payload["response_format"] = {"type": "json_object"}

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            endpoint,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": "parquet-guard/0.4.0a1-ai",
            },
            method="POST",
        )

        start_time = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as response:
                latency_ms = (time.perf_counter() - start_time) * 1000.0
                body = response.read().decode("utf-8")
                res_json = json.loads(body)

                content = res_json["choices"][0]["message"]["content"]
                usage = res_json.get("usage", {})
                tokens_prompt = usage.get("prompt_tokens")
                tokens_completion = usage.get("completion_tokens")

                return ProviderResponse(
                    content=content,
                    provider_name="openai",
                    model_id=self.model_id,
                    latency_ms=latency_ms,
                    tokens_prompt=tokens_prompt,
                    tokens_completion=tokens_completion,
                )

        except urllib.error.HTTPError as err:
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            error_body = err.read().decode("utf-8", errors="replace")
            if err.code in (401, 403):
                raise ProviderAuthError(f"Authentication failed (HTTP {err.code}): {error_body}") from err
            elif err.code == 429:
                raise ProviderRateLimitError(f"Rate limit exceeded (HTTP 429): {error_body}") from err
            else:
                raise ProviderError(f"OpenAI API error (HTTP {err.code}): {error_body}") from err

        except urllib.error.URLError as err:
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            if "timed out" in str(err.reason).lower():
                raise ProviderTimeoutError(
                    f"Request timed out after {self.timeout_seconds}s: {err.reason}"
                ) from err
            raise ProviderError(f"Network error connecting to AI provider: {err.reason}") from err

        except Exception as err:
            raise ProviderError(f"Unexpected error communicating with AI provider: {err}") from err


def get_provider(
    provider_type: str = "auto",
    model_id: Optional[str] = None,
    api_key: Optional[str] = None,
    **kwargs: Any,
) -> BaseAIProvider:
    """Factory to initialize an AI provider."""
    p_type = provider_type.lower()
    if p_type == "mock":
        return MockAIProvider(model_id=model_id or "mock-v1", **kwargs)
    elif p_type == "openai":
        return OpenAICompatibleProvider(model_id=model_id or "gpt-4o-mini", api_key=api_key, **kwargs)
    elif p_type == "auto":
        # If OPENAI_API_KEY is present in env, use OpenAI; otherwise fall back to Mock
        if os.environ.get("OPENAI_API_KEY") or api_key:
            return OpenAICompatibleProvider(model_id=model_id or "gpt-4o-mini", api_key=api_key, **kwargs)
        return MockAIProvider(model_id=model_id or "mock-auto", **kwargs)
    else:
        raise ValueError(
            f"Unsupported provider type '{provider_type}'. Allowed values: 'auto', 'openai', 'mock'."
        )
