import re
from pathlib import Path

import joblib


class KRLClassifier:
    def __init__(self, model_dir=None):
        if model_dir is None:
            model_dir = Path(__file__).resolve().parent
        model_dir = Path(model_dir)
        self.stage1 = joblib.load(model_dir / "stage1.joblib")
        self.stage2 = joblib.load(model_dir / "stage2.joblib")

    @staticmethod
    def preprocess(text):
        if not isinstance(text, str):
            raise TypeError("Tweet must be a string.")
        normalized = re.sub(r"\s+", " ", text).strip()
        if not normalized:
            raise ValueError("Tweet cannot be empty.")
        return normalized

    def predict(self, raw_text):
        text = self.preprocess(raw_text)
        stage1_pred = int(self.stage1.predict([text])[0])
        if stage1_pred == 0:
            label = "not_disruption"
        else:
            label = str(self.stage2.predict([text])[0])
        return {"prediction": label, "is_disruption": stage1_pred == 1}
