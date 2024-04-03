# import os
# pip install langchain-openai
# pip install langchain
import pandas as pd
from pandas import json_normalize
import json
from langchain import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain.chains.llm import LLMChain
from math import ceil
import time
from src.toolbox.json_functions import add_df_to_json, clean_json_file
from src.load.postgreSQL import get_comments_non_analysed


def get_feedbacks_from_openAI(demo = False):
    # Define prompt
    prompt_template = """
    As a marketing expert, you will analyse for each of these categories (delimited by <cat> </cat>) the following customer feedback: "{text}".
    Each category analysis will begin by the nature of the comment (Is it positive, negative, not mentioned) and then concisely explain why but only if it's positive or negative.
    The output should be in a JSON format and you will imperatively label it with its comment_id: "{comment_id}" and its company_id: "{company_id}".

    <cat>
        Usability,
        Speed/Performance,
        Pricing,
        Customer Service,
        Product Quality,
        Billing/Invoicing,
        Delivery/Shipping,
        Communication,
        Returns/Refunds,
        Product Features/Functionality
    </cat>

    <json_format>
    "comment_id": "{comment_id}",
    "company_id": "{company_id}",
    output: [
        Category: Category_name,
        Nature: answer,
        Analysis: answer
        ]
    </json_format>
    """
    # # Define prompt 2
    # prompt_template2 = """
    # As a marketing expert, you will analyse the following customer feedback: "{text}".
    # You will identify which categories this feedback relates to and for each category the analysis will begin by the nature of the comment (Is it positive, negative, not mentioned) and then concisely explain why but only if it's positive or negative.
    # The output should be in a JSON format and you will label it with its comment_id: "{comment_id}" and its company_id: "{company_id}"

    # <json_format>
    # [
    #     output: [
    #         comment_id: comment_id
    #         company_id: company_id
    #         Category: Category_name,
    #         Nature: answer,
    #         Analysis: answer
    #         ]
    # ]
    # </json_format>
    # """
    prompt = PromptTemplate.from_template(prompt_template)
    list_llm_chain = []

    # Define LLM chain for each api_key
    llm_julien = ChatOpenAI(
        temperature=0,
        model_name="gpt-3.5-turbo-16k",
        openai_api_key="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",
    )
    llm_chain_clients1 = LLMChain(llm=llm_julien, prompt=prompt)
    list_llm_chain.append(llm_chain_clients1)

    llm_teo = ChatOpenAI(
        temperature=0,
        model_name="gpt-3.5-turbo-16k",
        openai_api_key="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",
    )
    llm_chain_clients2 = LLMChain(llm=llm_teo, prompt=prompt)
    list_llm_chain.append(llm_chain_clients2)

    # llm_haythem = ChatOpenAI(temperature=0, model_name="gpt-3.5-turbo-16k",openai_api_key="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")
    # llm_chain_haythem = LLMChain(llm=llm_haythem, prompt=prompt)
    # list_llm_chain.append(llm_chain_haythem)

    # 1 - On reset feedback.json pour éviter que le fichier devienne trop gros
    clean_json_file("/app/data/feedbacks.json")

    # 2- On récupére le json de comments à analyser
    df_original = get_comments_non_analysed()  # df = pd.read_json('comments_to_analyse.json')

    # filtrer sur PNC Bank
    # df = df_original[df_original['company_name'] == 'PNC Bank']
    df = df_original

    # on créé le json sur lequel on va venir retirer les commentaires qui ont été analysé avant de l'enregistré
    df_without_analyzed_comments = df  # .drop['company_id']

    # Variable servant à éviter l'erreur du rate limit
    rate_limit_per_minute = 3
    rate_limit_per_day = 200
    batch_size = 2  # taille du batch = nb api_key

    # initialisations des compteurs pour envoyer les requetes par batch
    length_df = len(df)  # length of df
    nbr_of_batchs = ceil(
        length_df / batch_size
    )  # On calcul le nombre de batch à l'arrondi supérieur et on le ramene en dessous de 200 si >
    if nbr_of_batchs > rate_limit_per_day:
        nbr_of_batchs = rate_limit_per_day
    batch_counter = 1  # compteur pour debugging
    comment_counter = 0  # compteur pour debugging

    # Calculate the delay based on your rate limit
    delay_in_seconds = 60.0 / rate_limit_per_minute

    print("nbr_of_batchs", nbr_of_batchs)
    for i in range(nbr_of_batchs):
        print(
            f"################################# => batch number: {i} <= #################################"
        )
        print(f"df_without_analyzed_comments size: {len(df_without_analyzed_comments)}")
        print(f"comment_counter: {comment_counter} & batch_size: {batch_size}")

        # On récupére les comments du batch
        df_batch = df.iloc[comment_counter : comment_counter + batch_size,]
        # print(df_batch)
        print_comment_id(df_batch)

        # On analyse les X comments et on récupére l'output de l'analyse
        output_openAI = analyse_comment_with_openIA(df_batch, list_llm_chain)

        # On sauvegarde l'analyse dans le fichier comments_analyzed.json
        df_output = save_openAI_output_into_json(output_openAI)

        # # On enleve les commentaires analysé de df_without_analyzed_comments puis on écrit par dessus le fichier comments.json
        # df_without_analyzed_comments = remove_analyzed_comments(df_without_analyzed_comments, df_output)

        # On incrémente batch_counter et comment_counter
        batch_counter += 1
        comment_counter += batch_size

        if demo:
            break

        # Sleep for the delay
        print(f"Sleep for: {delay_in_seconds} ...")
        time.sleep(delay_in_seconds)

    print("END OF MAIN")


def print_comment_id(df_ids):
    print(df_ids["comment_id"].head(10))


def remove_analyzed_comments(df_without_analyzed_comments, df_output):
    print("-------------------------> remove_analyzed_comments")
    # print(df_without_analyzed_comments)
    # print('')
    # print(df_output)
    # On récupére les comment_id
    comments_id = df_output["comment_id"].unique()
    # print(comments_id)
    # Pour chaque comment_id on va supprimer les comments dans df
    print("len df_without_analyzed_comments: ", len(df_without_analyzed_comments))
    for id in comments_id:
        print(id)
        df_without_analyzed_comments = df_without_analyzed_comments.drop(
            df_without_analyzed_comments[
                df_without_analyzed_comments["comment_id"] == id
            ].index
        )
    print("len df_without_analyzed_comments: ", len(df_without_analyzed_comments))

    # On enregistre de nouveau dans le fichier json
    clean_json_file("comments_to_analyse.json")
    add_df_to_json("comments_to_analyse.json", df_without_analyzed_comments)

    return df_without_analyzed_comments


def save_openAI_output_into_json(output_openAI):
    print("-------------------------> save_openAI_output_into_json")
    # On initialise le dataframe qui récupérera le full output
    columns = ["comment_id", "company_id", "Category", "Nature", "Analysis"]
    df_responses = pd.DataFrame(
        data=None, index=None, columns=columns, dtype=None, copy=None
    )

    # On transforme le str contenu dans ['text'] en dataframe puis on le clean avant de l'ajouter dans df_responses
    for output in output_openAI:
        # test = True
        # On transform le string "output" en dictionnaire
        dict = json.loads(output)

        # On transforme le dictionnaire en dataframe
        if "output" in dict:
            df_comment = json_normalize(dict["output"])
        elif "categories" in dict:
            df_comment = json_normalize(dict["categories"])
        elif "category" in dict:
            df_comment = json_normalize(dict["category"])
        elif "Categories" in dict:
            df_comment = json_normalize(dict["Categories"])
        elif "Category" in dict:
            df_comment = json_normalize(dict["Category"])
        else:
            df_comment = json_normalize(dict)
            print("ERROR ----> dataframe without ouput #################################")
            print(df_comment, "\n")
            # test = False
            raise ValueError(
                "OpenAI invented a key, we need to add the key and reload the script "
            )

        # df_comment = json_normalize(dict['output'])
        # if test:
        # On ajoute la colonne company_id, comment_id et on ordonne les colonnes
        df_comment = df_comment.assign(
            company_id=dict["company_id"], comment_id=dict["comment_id"]
        )
        df_comment = df_comment[columns]

        # # On enleve les lignes ou l'IA a quand même indiqué une catégorie mais avec rien dedans
        # df_comment = df_comment.drop(df_comment[(df_comment['Nature'] == "Not mentioned") | (df_comment['Nature'] == "not mentioned")].index)

        # # On ajoute à df_responses
        df_responses = pd.concat(
            [df_responses, df_comment], ignore_index=True, sort=False
        )

    print("-----output----")
    print_comment_id(df_responses)

    # append data frame to CSV file
    # df_responses.to_csv('comments_analyzed.csv', mode='a', index=False, header=False)
    add_df_to_json("/app/data/feedbacks.json", df_responses)
    # df_responses.to_json('comments_analyzed.json', mode='a', orient='records', lines=True)
    print("-----output saved to json----")

    return df_responses


# @retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
def analyse_comment_with_openIA(df_batch, list_llm_chain):
    print("-------------------------> analyse_comment_with_openIA")
    print(f"len(df_batch): {len(df_batch)}")

    output_final = []
    llm_number = 0  # Compteur pour alterner les llm_chain
    # On parcours df_batch
    for i in df_batch.index:
        # Prepare the input for the chain
        input = {
            "text": df_batch.loc[i, "comment_content"],
            "comment_id": df_batch.loc[i, "comment_id"],
            "company_id": df_batch.loc[i, "company_id"],
        }

        # On détermine quel llm_chain doit être choisi car on alterne entre chacune
        if llm_number == 0:
            print("llm_chain_clients1")
        elif llm_number == 1:
            print("llm_chain_clients2")
        elif llm_number == 2:
            print("llm_chain_haythem")

        output = list_llm_chain[llm_number].run(input)

        # On ajuste la valeur de llm_number avec une remise à 0 au cas ou on atteint la limite du batch size
        # Ne devrait jamais arriver car le batch size ne doit jamais dépasser le nombre d'api_key
        llm_number += 1
        if llm_number == len(df_batch):
            llm_number = 0

        print(output, "\n")
        output_final.append(output)

    return output_final
