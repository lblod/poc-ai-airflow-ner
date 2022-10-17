from gcs import read_json, write_json
import fire
import flair

# set flair root cache --> fixes offline loading
print(flair.cache_root)
flair.cache_root = "/models/"

from flair.data import Sentence
from flair.models import SequenceTagger
from typing import List, Dict
from tqdm import tqdm


class FlairNER:
    """
    Helper class that does the heavy NER lifting for you
    """

    def __init__(self, model_name: str):
        """
        Init function that loads model from path
        :param model_name:
        """
        self.ner_model = SequenceTagger.load(model_name)

    def get_entities(self, text: str, confidence: float) -> List[Dict]:
        """
        This function processes the text and returns the entities

        :param text: the input text
        :param confidence: the minimum confidence to be excepted as NE
        :return: a list of dicts containing the entities in the text
        """
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
        # Load content from file
        records = read_json(file_name="export.json")

        # skip if none
        if not records:
            return None

        # Instantiate the helper class
        model = FlairNER(model_name)

        texts = [text["text"][:10_000] for text in records]


        processed_ners = []

        # loop over docs
        for i, text in tqdm(enumerate(texts)):
            processed_ners.append({**records[i], "ner": model.get_entities(text, confidence)})

        write_json(file_name="ner.json", content=processed_ners)

    except Exception as e:
        raise e


if __name__ == '__main__':
    fire.Fire(ner)
