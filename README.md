# poc-ai-airflow-ner

![](https://build.redpencil.io/api/badges/lblod/poc-ai-airflow-ner/status.svg)

This repository contains the airflow code that is used for named entity recognition (NER). The current NER implementation
will return three types of entities:
- Organization
- Location
- Person

In the current context, we return the found named entities as followed
```py
# Keep in mind, this is en example
{"word"ML2Grow , "star":117, "end":124, "confidence":0.97, "entity_type":"ORG"}
```

A more indepth functionality guide can be found at the poc-ai-deployment repo.

NER airflow by ML2Grow
