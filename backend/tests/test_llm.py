import pytest
from charcentric.llm import MockLLMProvider


class TestMockLLMProvider:
    def test_analyze_sentiment(self):
        """Test sentiment analysis with mock provider"""
        provider = MockLLMProvider()
        texts = ["I love this", "I hate this", "Neutral text"]
        
        results = provider.analyze_sentiment(texts)
        
        assert len(results) == 3
        assert all("text" in r for r in results)
        assert all("sentiment" in r for r in results)
        assert all("score" in r for r in results)
        assert all(r["sentiment"] == "NEUTRAL" for r in results)
        assert all(r["score"] == 3 for r in results)
    
    def test_detect_toxicity(self):
        """Test toxicity detection with mock provider"""
        provider = MockLLMProvider()
        texts = ["Friendly message", "Potentially toxic"]
        
        results = provider.detect_toxicity(texts)
        
        assert len(results) == 2
        assert all("text" in r for r in results)
        assert all("toxicity" in r for r in results)
        assert all("score" in r for r in results)
        assert all(r["toxicity"] == "NON_TOXIC" for r in results)
        assert all(r["score"] == 0 for r in results)