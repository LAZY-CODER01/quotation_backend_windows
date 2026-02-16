import os
from pymongo import MongoClient
import certifi
from sentence_transformers import SentenceTransformer
from flask import current_app

class SemanticSearchService:
    _model = None

    def __init__(self):
        self.mongo_uri = "mongodb+srv://dbestsnapquote_db_user:Dbest2000@cluster0.djcreza.mongodb.net"
        self.db_name = "quotation_rag"
        self.collection_name = "offers"
        self.embedding_model = "BAAI/bge-base-en-v1.5"
        self.top_k = 20
        self.min_vector_score = 0.65
        self.keyword_boost = 0.15

        # Initialize model if not already done (singleton-like behavior at class level)
        if SemanticSearchService._model is None:
            SemanticSearchService._model = SentenceTransformer(self.embedding_model)
            SemanticSearchService._model.max_seq_length = 512

        self.client = MongoClient(
            self.mongo_uri,
            tls=True,
            tlsCAFile=certifi.where()
        )
        self.collection = self.client[self.db_name][self.collection_name]

    def _generate_embedding(self, text: str):
        """
        BGE performs better when query is prefixed.
        """
        formatted_query = (
            "Represent this sentence for searching relevant products: "
            + text.lower()
        )
        return SemanticSearchService._model.encode(
            formatted_query,
            normalize_embeddings=True
        ).tolist()

    def search(self, query: str):
        try:
            query_embedding = self._generate_embedding(query)

            pipeline = [
                {
                    "$vectorSearch": {
                        "index": "offers_vector",
                        "path": "embedding",
                        "queryVector": query_embedding,
                        "numCandidates": 100,
                        "limit": 50
                    }
                },
                {
                    "$addFields": {
                        "vector_score": {"$meta": "vectorSearchScore"}
                    }
                },
                {
                    "$match": {
                        "vector_score": {"$gte": self.min_vector_score}
                    }
                },
                {
                    "$addFields": {
                        "keyword_match": {
                            "$cond": [
                                {
                                    "$or": [
                                        {"$regexMatch": {"input": "$requirement", "regex": query, "options": "i"}},
                                        {"$regexMatch": {"input": "$offer", "regex": query, "options": "i"}}
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    }
                },
                {
                    "$addFields": {
                        "final_score": {
                            "$add": [
                                "$vector_score",
                                {"$multiply": ["$keyword_match", self.keyword_boost]}
                            ]
                        }
                    }
                },
                {
                    "$sort": {"final_score": -1}
                },
                {
                    "$limit": self.top_k
                },
                {
                    "$project": {
                        "_id": 0,
                        "requirement": 1,
                        "offer": 1,
                        "brand": 1,
                        "price": 1,
                        "currency": 1,
                        "image_url": 1,
                        "source_file": 1,
                        "row_number": 1,
                        "unit": 1, 
                        "score": "$final_score"
                    }
                }
            ]

            results = list(self.collection.aggregate(pipeline))
            return results

        except Exception as e:
            print(f"Error in semantic search: {e}")
            return []
