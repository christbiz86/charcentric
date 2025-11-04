import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import os

try:
    import google.generativeai as genai
    from tenacity import retry, stop_after_attempt, wait_exponential
except Exception:  # optional deps
    genai = None


class LLMProvider(ABC):
    @abstractmethod
    def analyze_sentiment(self, texts: List[str]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def detect_toxicity(self, texts: List[str]) -> List[Dict[str, Any]]:
        pass


class GeminiLLMProvider(LLMProvider):
    def __init__(self, api_key: str):
        if genai is None:
            raise RuntimeError("google-generativeai not installed in this environment")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-pro')

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))  # type: ignore
    def analyze_sentiment(self, texts: List[str]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for text in texts:
            prompt = (
                """
                You are an AI assistant that classifies the sentiment of short text messages.
                For the input message, respond with exactly one of: POSITIVE, NEGATIVE, or NEUTRAL.
                Respond only with the label, no explanations.
                
                Input: {text}
                """
            ).format(text=text)
            try:
                response = self.model.generate_content(prompt)
                sentiment = response.text.strip().upper()
                score_map = {"POSITIVE": 5, "NEUTRAL": 3, "NEGATIVE": 0}
                results.append({
                    "text": text,
                    "sentiment": sentiment,
                    "score": score_map.get(sentiment, 3)
                })
                time.sleep(0.1)
            except Exception as e:  # noqa: BLE001
                results.append({"text": text, "sentiment": "ERROR", "score": -1, "error": str(e)})
        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))  # type: ignore
    def detect_toxicity(self, texts: List[str]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for text in texts:
            prompt = (
                """
                You are an AI assistant that detects toxic language.
                For the input message, respond with exactly one of: TOXIC or NON_TOXIC.
                Respond only with the label, no explanations.
                
                Input: {text}
                """
            ).format(text=text)
            try:
                response = self.model.generate_content(prompt)
                toxicity = response.text.strip().upper()
                score_map = {"TOXIC": 1, "NON_TOXIC": 0}
                results.append({
                    "text": text,
                    "toxicity": toxicity,
                    "score": score_map.get(toxicity, 0)
                })
                time.sleep(0.1)
            except Exception as e:  # noqa: BLE001
                results.append({"text": text, "toxicity": "ERROR", "score": -1, "error": str(e)})
        return results


class MockLLMProvider(LLMProvider):
    def analyze_sentiment(self, texts: List[str]) -> List[Dict[str, Any]]:
        return [{"text": text, "sentiment": "NEUTRAL", "score": 3} for text in texts]

    def detect_toxicity(self, texts: List[str]) -> List[Dict[str, Any]]:
        return [{"text": text, "toxicity": "NON_TOXIC", "score": 0} for text in texts]