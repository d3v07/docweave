"""Embedding generation using sentence-transformers."""
from dataclasses import dataclass
from typing import List, Optional, Union
import numpy as np


@dataclass
class EmbeddingResult:
    """Result of embedding generation."""
    text: str
    embedding: List[float]
    model: str
    dimension: int


class Embedder:
    """Generates embeddings for text using sentence-transformers."""

    DEFAULT_MODEL = "all-MiniLM-L6-v2"

    def __init__(self, model_name: Optional[str] = None):
        """Initialize with sentence-transformer model."""
        self.model_name = model_name or self.DEFAULT_MODEL
        self._model = None
        self._dimension = None

    def _load_model(self):
        """Lazy load the model."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(self.model_name)
                # Get dimension from a test embedding
                test_emb = self._model.encode("test")
                self._dimension = len(test_emb)
            except ImportError:
                raise ImportError(
                    "sentence-transformers is required. "
                    "Install with: pip install sentence-transformers"
                )

    @property
    def dimension(self) -> int:
        """Get embedding dimension."""
        self._load_model()
        return self._dimension

    def embed(self, text: str) -> EmbeddingResult:
        """Generate embedding for a single text."""
        self._load_model()

        embedding = self._model.encode(text, convert_to_numpy=True)

        return EmbeddingResult(
            text=text,
            embedding=embedding.tolist(),
            model=self.model_name,
            dimension=len(embedding)
        )

    def embed_batch(self, texts: List[str], batch_size: int = 32) -> List[EmbeddingResult]:
        """Generate embeddings for multiple texts efficiently."""
        self._load_model()

        if not texts:
            return []

        # Encode in batches
        embeddings = self._model.encode(
            texts,
            batch_size=batch_size,
            convert_to_numpy=True,
            show_progress_bar=False
        )

        results = []
        for text, emb in zip(texts, embeddings):
            results.append(EmbeddingResult(
                text=text,
                embedding=emb.tolist(),
                model=self.model_name,
                dimension=len(emb)
            ))

        return results

    def embed_claim(self, subject: str, predicate: str, object_value: str) -> EmbeddingResult:
        """Generate embedding for a claim triple."""
        # Create a natural language representation of the claim
        claim_text = f"{subject} {predicate.replace('_', ' ')} {object_value}"
        return self.embed(claim_text)

    def embed_entity(self, name: str, entity_type: str, context: str = "") -> EmbeddingResult:
        """Generate embedding for an entity."""
        # Create a rich representation
        if context:
            entity_text = f"{name} ({entity_type}): {context[:200]}"
        else:
            entity_text = f"{name} ({entity_type})"
        return self.embed(entity_text)

    def similarity(self, emb1: List[float], emb2: List[float]) -> float:
        """Calculate cosine similarity between two embeddings."""
        a = np.array(emb1)
        b = np.array(emb2)
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

    def find_similar(
        self,
        query_embedding: List[float],
        embeddings: List[List[float]],
        top_k: int = 5,
        threshold: float = 0.5
    ) -> List[tuple[int, float]]:
        """Find most similar embeddings to a query."""
        query = np.array(query_embedding)
        similarities = []

        for idx, emb in enumerate(embeddings):
            sim = self.similarity(query_embedding, emb)
            if sim >= threshold:
                similarities.append((idx, sim))

        # Sort by similarity descending
        similarities.sort(key=lambda x: x[1], reverse=True)

        return similarities[:top_k]
