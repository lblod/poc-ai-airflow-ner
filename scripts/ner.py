from gcs import read_json, write_json
import fire
import flair
print(flair.cache_root)
flair.cache_root = "/models/"

from flair.data import Sentence
from flair.models import SequenceTagger
from typing import List, Dict
from tqdm import tqdm


class FlairNER:
    def __init__(self, model_name: str):
        self.ner_model = SequenceTagger.load(model_name)

    def get_entities(self, text: str, confidence: float) -> List[Dict]:
        ner_entities = []

        decap_text = " ".join([word.lower().capitalize() if not word.islower() else word for word in text.split(" ")])
        tokenized_text = Sentence(decap_text)
        self.ner_model.predict(tokenized_text)
        # Entity text, label, position & score extraction
        for entity in tokenized_text.get_spans('ner'):
            if entity.tag not in ['MISC'] and entity.score >= confidence:
                ner_entities.append({"token": entity.text, "tag": entity.tag, "start_pos": entity.start_position,
                                     "end_pos": entity.end_position, "confidence_score": entity.score})
        return ner_entities


def ner(model_name="/models/NER-model", confidence=0.6):
    try:
        records = read_json(file_name="export.json")

        model = FlairNER(model_name)

        if records is None:
            return None
        texts = [text["text"][:10_000] for text in records]

        # I am aware that this is not actually fully batched, but that's not really relevant here.
        processed_ners = []
        for i, text in tqdm(enumerate(texts)):
            processed_ners.append({**records[i], "ner": model.get_entities(text, confidence)})

        write_json(file_name="ner.json", content=processed_ners)

    except Exception as e:
        raise e


if __name__ == '__main__':
  fire.Fire(ner)
