from gcs import read_json
import fire, uuid, requests
from tqdm import tqdm

TYPE_MAPPING = {"LOC": "Location", "PER": "Person", "ORG": "Organization"}


def save(endpoint):
    """
    Function that creates and executes the DELETE WHERE and INSERT queries

    :param endpoint: the url to the sparql endpoint
    :return:
    """
    records = read_json(file_name="ner.json")
    headers = {
        "Accept": "application/sparql-results+json,*/*;q=0.9"
    }

    for record in tqdm(records):
        try:
            file_name = record["thing"]

            uris, query_extension = [], []
            for ner in record["ner"]:
                ner_uri = f"http://data.lblod.info/ners/{str(uuid.uuid4())}"
                uris.append(f"<{ner_uri}>")
                query_extension.append(
                    f"""<{ner_uri}> a ext:Ner; 
                    ext:start {ner['start_pos']}; 
                    ext:end {ner['end_pos']}; 
                    ext:word \"{ner['token']}\";
                    ext:entity ext:{TYPE_MAPPING[ner['tag']]} .
                    """
                )

            if len(record["ner"]) != 0:
                INSERT = f"""
                <{file_name}> ext:hasNer {' , '.join(uris)} .
                {''.join(query_extension)}
                <{file_name}> ext:ingestedml2GrowSmartRegulationsNer "1"
                """
            else:
                INSERT = f"""
                <{file_name}> ext:ingestedml2GrowSmartRegulationsNer "1".
                """

            q = f"""
            PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
            
            
            DELETE{{
             GRAPH <http://mu.semte.ch/application>{{
             <{file_name}> ext:hasNer ?b ; ext:ingestedml2GrowSmartRegulationsNer ?srn .
            ?b ext:entity ?ner_type; ext:end ?end; ext:start ?start; ext:word ?word .
            }}
            }}
            WHERE{{
             <{file_name}> ext:hasNer ?b ; ext:ingestedml2GrowSmartRegulationsNer ?srn .
            ?b ext:entity ?ner_type; ext:end ?end; ext:start ?start; ext:word ?word .
            
            }}
            """
            #
            # DELETE {{
            #     GRAPH <http://mu.semte.ch/application> {{
            #         <{file_name}> ext:hasNer ?oldNER
            #     }}
            # }}
            # WHERE {{
            #     GRAPH <http://mu.semte.ch/application> {{
            #         <{file_name}> ext:hasNer ?oldNER
            #     }}
            # }}
            
            

            # WHERE
            # {{
            #      GRAPH < http: // mu.semte.ch / application > {{
            #     OPTIONAL
            # {{ < {file_name} > ext: hasNer ?oldNER.}}
            # }}
            # }}

            r = requests.post(endpoint, data={"query": q}, headers=headers)
            if r.status_code != 200:
                print(f"[FAILURE] {50 * '-'} /n {q} /n {50 * '-'}")

            q = f"""
            PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
            
            INSERT {{
                GRAPH <http://mu.semte.ch/application> {{
                    {INSERT}
                }}
            }}
            """

            r = requests.post(endpoint, data={"query": q}, headers=headers)
            if r.status_code != 200:
                print(f"[FAILURE] {50 * '-'} /n {q} /n {50 * '-'}")

        except Exception as ex:
            print(ex)
            raise ex


if __name__ == '__main__':
    fire.Fire(save)
