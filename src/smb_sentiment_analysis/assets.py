from dagster import asset
import pandas as pd
import requests
import os

from dagster_openai import OpenAIResource


@asset
def fetch_facebook_reviews() -> pd.DataFrame:
    # Replace with real API call/automation
    r = requests.get("https://api.example.com/fb_reviews")  # placeholder
    return pd.DataFrame(r.json())

@asset
def fetch_google_reviews() -> pd.DataFrame:
    r = requests.get("https://api.example.com/google_reviews")  # placeholder
    return pd.DataFrame(r.json())

@asset
def all_reviews(fetch_facebook_reviews, fetch_google_reviews):
    return pd.concat([fetch_facebook_reviews, fetch_google_reviews])


openai_resource = OpenAIResource(api_key=os.getenv("OPENAI_API_KEY"))  # set in .env

@asset
def review_sentiments(all_reviews: pd.DataFrame, openai: OpenAIResource) -> pd.DataFrame:
    results = []
    with openai.get_client() as client:
        for review in all_reviews['text'][:50]:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": f"Sentiment (positive/negative/neutral): {review}"}],
                max_tokens=8
            )
            results.append(response.choices[0].message.content)
    all_reviews["sentiment"] = results
    return all_reviews