import csv
import os
from typing import List, Dict, Any

from .models import BlockRun, Artifact
from .llm import LLMProvider


class StepExecutor:
    def __init__(self, llm_provider: LLMProvider, data_dir: str = "/data"):
        self.llm_provider = llm_provider
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def execute_csv_reader(self, block_run: BlockRun, config: Dict[str, Any]) -> List[Artifact]:
        file_path = config.get("file_path", "/data/sample.csv")
        delimiter = config.get("delimiter", ";")  # Default to semicolon for sample.csv
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            rows = list(reader)
        artifact = Artifact(
            block_run_id=block_run.id,
            name="csv_data",
            type="csv_rows",
            data={
                "row_count": len(rows),
                "columns": list(rows[0].keys()) if rows else [],
                "preview": rows[:5]
            }
        )
        return [artifact]

    def execute_llm_sentiment(self, block_run: BlockRun, input_artifacts: List[Artifact]) -> List[Artifact]:
        texts: List[str] = []
        for artifact in input_artifacts:
            if artifact.type == "csv_rows":
                for row in artifact.data.get("preview", []):
                    if "text" in row:
                        texts.append(row["text"])
        results = self.llm_provider.analyze_sentiment(texts)
        artifact = Artifact(
            block_run_id=block_run.id,
            name="sentiment_results",
            type="sentiment_analysis",
            data={"results": results, "total_processed": len(results)}
        )
        return [artifact]

    def execute_llm_toxicity(self, block_run: BlockRun, input_artifacts: List[Artifact]) -> List[Artifact]:
        texts: List[str] = []
        for artifact in input_artifacts:
            if artifact.type == "csv_rows":
                for row in artifact.data.get("preview", []):
                    if "text" in row:
                        texts.append(row["text"])
        results = self.llm_provider.detect_toxicity(texts)
        artifact = Artifact(
            block_run_id=block_run.id,
            name="toxicity_results",
            type="toxicity_detection",
            data={"results": results, "total_processed": len(results)}
        )
        return [artifact]

    def execute_file_writer(self, block_run: BlockRun, input_artifacts: List[Artifact], config: Dict[str, Any]) -> List[Artifact]:
        output_type = config.get("output_type", "sentiment")
        input_artifact = None
        for artifact in input_artifacts:
            if (output_type == "sentiment" and artifact.type == "sentiment_analysis") or \
            (output_type == "toxicity" and artifact.type == "toxicity_detection"):
                input_artifact = artifact
                break
        if input_artifact is None:
            return []
        filename = f"{output_type}_results.csv"
        file_path = os.path.join(self.data_dir, filename)
        results = input_artifact.data.get("results", [])
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            if output_type == "sentiment":
                fieldnames = ['text', 'sentiment', 'score']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for result in results:
                    writer.writerow({
                        'text': result['text'],
                        'sentiment': result['sentiment'],
                        'score': result['score']
                    })
            else:
                fieldnames = ['text', 'toxicity', 'score']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for result in results:
                    writer.writerow({
                        'text': result['text'],
                        'toxicity': result['toxicity'],
                        'score': result['score']
                    })
        artifact = Artifact(
            block_run_id=block_run.id,
            name=f"{output_type}_output",
            type="file_output",
            data={"file_path": file_path, "file_size": os.path.getsize(file_path), "rows_written": len(results)}
        )
        return [artifact]


