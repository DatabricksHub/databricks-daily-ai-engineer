"""
utils/llm_client.py — Unified LLM client supporting multiple free providers.

Supported providers (set LLM_PROVIDER env var):
  groq      — FREE at console.groq.com (recommended for students)
  gemini    — FREE tier at aistudio.google.com
  ollama    — 100% local and free, install at ollama.com
  anthropic — Paid, best quality
"""

import time
from typing import Iterator

from config import cfg
from utils.logger import get_logger

logger = get_logger("llm_client", "LLMClient")


def chat(
    prompt: str,
    system: str = "You are a helpful AI assistant specialising in Databricks.",
    max_tokens: int = 4096,
    temperature: float = 0.3,
    retries: int = 3,
) -> str:
    """
    Unified chat function — routes to the correct provider based on cfg.llm_provider.
    Drop-in replacement for the old anthropic-only chat().
    """
    provider = cfg.llm_provider.lower()
    logger.debug(f"Using provider: {provider}")

    if provider == "groq":
        return _chat_groq(prompt, system, max_tokens, temperature, retries)
    elif provider == "gemini":
        return _chat_gemini(prompt, system, max_tokens, temperature, retries)
    elif provider == "ollama":
        return _chat_ollama(prompt, system, max_tokens, temperature)
    elif provider == "anthropic":
        return _chat_anthropic(prompt, system, max_tokens, temperature, retries)
    else:
        raise ValueError(
            f"Unknown LLM_PROVIDER='{provider}'. "
            "Choose: groq | gemini | ollama | anthropic"
        )


# ── Groq (FREE) ────────────────────────────────────────────────────────────────

def _chat_groq(
    prompt: str,
    system: str,
    max_tokens: int,
    temperature: float,
    retries: int,
) -> str:
    try:
        from groq import Groq
    except ImportError:
        raise ImportError("Run: pip install groq")

    if not cfg.groq_api_key:
        raise EnvironmentError(
            "GROQ_API_KEY not set.\n"
            "1. Sign up FREE at console.groq.com\n"
            "2. export GROQ_API_KEY=gsk_..."
        )

    client = Groq(api_key=cfg.groq_api_key)

    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=cfg.groq_model,
                max_tokens=max_tokens,
                temperature=temperature,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user",   "content": prompt},
                ],
            )
            text = response.choices[0].message.content
            logger.debug(f"Groq response: ~{len(text.split())} words")
            return text
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt * 5
                logger.warning(f"Groq error: {e}. Retrying in {wait}s…")
                time.sleep(wait)
            else:
                raise


# ── Google Gemini (FREE tier) ──────────────────────────────────────────────────

def _chat_gemini(
    prompt: str,
    system: str,
    max_tokens: int,
    temperature: float,
    retries: int,
) -> str:
    try:
        import google.generativeai as genai
    except ImportError:
        raise ImportError("Run: pip install google-generativeai")

    if not cfg.gemini_api_key:
        raise EnvironmentError(
            "GEMINI_API_KEY not set.\n"
            "1. Get free key at aistudio.google.com\n"
            "2. export GEMINI_API_KEY=AIza..."
        )

    genai.configure(api_key=cfg.gemini_api_key)
    model = genai.GenerativeModel(
        model_name=cfg.gemini_model,
        system_instruction=system,
        generation_config=genai.types.GenerationConfig(
            max_output_tokens=max_tokens,
            temperature=temperature,
        ),
    )

    for attempt in range(retries):
        try:
            response = model.generate_content(prompt)
            text = response.text
            logger.debug(f"Gemini response: ~{len(text.split())} words")
            return text
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt * 5
                logger.warning(f"Gemini error: {e}. Retrying in {wait}s…")
                time.sleep(wait)
            else:
                raise


# ── Ollama (LOCAL — 100% free) ─────────────────────────────────────────────────

def _chat_ollama(
    prompt: str,
    system: str,
    max_tokens: int,
    temperature: float,
) -> str:
    try:
        import requests as req
    except ImportError:
        raise ImportError("Run: pip install requests")

    url = f"{cfg.ollama_base_url}/api/chat"
    payload = {
        "model": cfg.ollama_model,
        "stream": False,
        "options": {"temperature": temperature, "num_predict": max_tokens},
        "messages": [
            {"role": "system",  "content": system},
            {"role": "user",    "content": prompt},
        ],
    }

    try:
        resp = req.post(url, json=payload, timeout=120)
        resp.raise_for_status()
        text = resp.json()["message"]["content"]
        logger.debug(f"Ollama response: ~{len(text.split())} words")
        return text
    except req.exceptions.ConnectionError:
        raise ConnectionError(
            f"Cannot reach Ollama at {cfg.ollama_base_url}.\n"
            "1. Install Ollama: ollama.com\n"
            f"2. Pull the model: ollama pull {cfg.ollama_model}\n"
            "3. Start Ollama: ollama serve"
        )


# ── Anthropic (Paid) ───────────────────────────────────────────────────────────

def _chat_anthropic(
    prompt: str,
    system: str,
    max_tokens: int,
    temperature: float,
    retries: int,
) -> str:
    try:
        import anthropic
    except ImportError:
        raise ImportError("Run: pip install anthropic")

    if not cfg.anthropic_api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY not set.\n"
            "Get a key at console.anthropic.com (requires billing)"
        )

    client = anthropic.Anthropic(api_key=cfg.anthropic_api_key)

    for attempt in range(retries):
        try:
            response = client.messages.create(
                model=cfg.anthropic_model,
                max_tokens=max_tokens,
                temperature=temperature,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text
            logger.debug(f"Anthropic response: ~{len(text.split())} words")
            return text
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt * 10
                logger.warning(f"Anthropic error: {e}. Retrying in {wait}s…")
                time.sleep(wait)
            else:
                raise


# ── Streaming (Groq + Anthropic only) ─────────────────────────────────────────

def stream(
    prompt: str,
    system: str = "You are a helpful AI assistant specialising in Databricks.",
    max_tokens: int = 8192,
) -> Iterator[str]:
    """Streaming fallback — yields full response as single chunk for unsupported providers."""
    provider = cfg.llm_provider.lower()
    if provider in ("ollama", "gemini"):
        # These providers don't support streaming in this wrapper — return full response
        yield chat(prompt, system, max_tokens)
        return

    if provider == "groq":
        try:
            from groq import Groq
            client = Groq(api_key=cfg.groq_api_key)
            with client.chat.completions.create(
                model=cfg.groq_model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user",   "content": prompt},
                ],
                max_tokens=max_tokens,
                stream=True,
            ) as s:
                for chunk in s:
                    delta = chunk.choices[0].delta.content
                    if delta:
                        yield delta
        except ImportError:
            yield chat(prompt, system, max_tokens)

    elif provider == "anthropic":
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=cfg.anthropic_api_key)
            with client.messages.stream(
                model=cfg.anthropic_model,
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            ) as s:
                for chunk in s.text_stream:
                    yield chunk
        except ImportError:
            yield chat(prompt, system, max_tokens)