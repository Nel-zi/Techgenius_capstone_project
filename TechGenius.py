# Import libraries

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import re
import pandas as pd
from sqlalchemy import create_engine
import lxml
from pprint import pprint
from datetime import datetime

import os
import time

import logging 
import psycopg2


from azure.storage.blob import BlobServiceClient

# Configure logging (optional, but useful for debugging)
logging.basicConfig(level=logging.ERROR)


def extract_data(**kwargs):
    url = 'https://www.trustpilot.com/categories/restaurants_bars?sort=latest_review'
       #https://www.trustpilot.com/categories/restaurants_bars?page=2&sort=latest_review
       # an average of 6 pages covers a 2 weeks scraping interval

    response = requests.get(url)
    #print(response)


    # Request with BeatutifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    # print(soup.prettify())


    # Scrapping all recent review companies using regular expression
    data = soup.find_all('a', href=re.compile(r'^/review/'))

    # Create an empty set to hold unique hrefs
    recent_companies_reviews = set()

    # Use a normal for loop to add each href to the set.
    for link in data:
        recent_companies_reviews.add(link.get('href'))


    # for recent in recent_companies_reviews:
        #print(recent)
    #print("\n")
    #print(len(recent_companies_reviews))


    # Creating an empty dictionary to store the parameters to be extracted
    reviews_dict = {
        'company_name' : [],
        'country' : [],
        'reviewer_name' : [],
        'review_title' : [],
        'review_text' : [],
        'Date_of_experience' : [],
        'rating_text' : []
    }


    # Looping through the list of companies with recent reviews
    for recent in recent_companies_reviews:
        new_url = 'https://www.trustpilot.com' + recent

        Response = requests.get(new_url)

        #print(Response)

        #print("\n")

        # Request with BeatutifulSoup
        Soup = BeautifulSoup(Response.text, 'html.parser')
        # print(Soup.prettify())
        # print("\n")


        # Now we're on each of the companies's page, we'll then extract all the 7 details from the page starting with...
        # 1, The company's name

        company_name_element = Soup.find('h1', class_='typography_default__PeVD_ typography_appearance-default__t8iAq title_title__pKuza')
        if company_name_element:
            company_name = company_name_element.text
        else:
            logging.error("Company name element not found.")
            #company_name = None  # or set a default value if needed


        #company_name = Soup.find('h1', class_ = 'typography_default__PeVD_ typography_appearance-default__t8iAq title_title__dK8Bt').text
        
        company_name = company_name.split()
        company_name1 = company_name[0].strip("'")
        #print(f'The company_name is: {company_name1}')
        #print("\n")

        
        # 2, Find the relevant div that contains all the reviews on the company's review page
        reviews = Soup.find_all('div', class_ = 'styles_cardWrapper__g8amG styles_show__Z8n7u')
        


        for review in reviews:
            
            if review:
                try:
                    # 2, Find the relevant div that contains the reviewer's country
                    country = review.find('div', class_ = 'styles_consumerExtraDetails__NY6RP').text  
                    country = country.split('•')
                    country1 = country[0]
                    # print(f'The country is: {country[0]}')
                    # print("\n")

                except AttributeError as e:
                    print(f"Error parsing Country from {review}: {e}")
                    continue  # Skip this job if data parsing fails

            if review:
                try:
                    # 3, Find the relevant div that contains the reviewer's name
                    reviewer_name = review.find('a', href=re.compile(r'^/users/')).text
                    reviewer_name = reviewer_name.split(country1)
                    reviewer_name1 = reviewer_name[0]
                    # print(f'The reviewer_name is: {reviewer_name[0]}')
                    # print("\n")

                except AttributeError as e:
                    print(f"Error parsing reviewer_name from {review}: {e}")
                    continue  # Skip this job if data parsing fails
            
            
            if review:
                try:
                    # 4, Find the relevant div that contains the review's title
                    review_title = review.find('h2', class_ = 'typography_heading-xs__osRhC typography_appearance-default__t8iAq').text
                    # print(f'The review_title is: {review_title}')
                    # print("\n")


                except AttributeError as e:
                    print(f"Error parsing review_title from {review}: {e}")
                    continue  # Skip this job if data parsing fails
            

            if review:
                try:
                    # 5, Find the relevant div that contains the review text
                    review_text = review.find('p', class_ = 'typography_body-l__v5JLj typography_appearance-default__t8iAq').text
                    # print(f'The review_text is: {review_text}')
                    # print("\n")


                except AttributeError as e:
                    print(f"Error parsing review_text from {review}: {e}")
                    continue  # Skip this job if data parsing fails


            if review:
                try:
                    # 6, Find the relevant div that contains the review Date of experience
                    Date_of_experience = review.find('p', class_ = 'typography_body-m__k2UI7 typography_appearance-default__t8iAq').text
                    Date_of_experience = Date_of_experience.split(':')
                    Date_of_experience1 = Date_of_experience[1]
                    # print(f'{Date_of_experience[0]}: {Date_of_experience[1]}')
                    # print("\n")


                except AttributeError as e:
                    print(f"Error parsing Date_of_experience from {review}: {e}")
                    continue  # Skip this job if data parsing fails


            if review:
                try:
                    # 7, Find the relevant div that contains the rating
                    rating = review.find('div', class_ = 'star-rating_starRating__sdbkn star-rating_medium__Oj7C9')
                    # print(f'The rating is: {rating}')
                    # print("\n")

                    if rating:
                        try:
                            # Find the <img> tag inside the div
                            img_tag = rating.find('img')
                            # print(f'The img tag is: {img_tag}')
                            # print("\n")
                            
                            if img_tag:
                                try:
                                    # Extract the value of the 'alt' attribute, which contains the rating text
                                    rating_text = img_tag.get('alt')
                                    # print(f'The rating_text is: {rating_text}')
                                    # print("\n")

                                except AttributeError as e:
                                    print(f"Error parsing rating_text from {review}: {e}")
                                    continue  # Skip this job if data parsing fails

                        
                        except AttributeError as e:
                            print(f"Error parsing img_tag from {review}: {e}")
                            continue  # Skip this job if data parsing fails

                
                except AttributeError as e:
                    print(f"Error parsing rating from {review}: {e}")
                    continue  # Skip this job if data parsing fails



            reviews_dict['reviewer_name'].append(reviewer_name1)
            reviews_dict['country'].append(country1)
            reviews_dict['review_title'].append(review_title)
            reviews_dict['review_text'].append(review_text)
            reviews_dict['Date_of_experience'].append(Date_of_experience1)
            reviews_dict['rating_text'].append(rating_text)
        reviews_dict['company_name'].append(company_name1)

        #This is to balance the count of company name in the dictionary, if not we'll not be able to create the dataframe table...
        # ...because it'll say table can't be created unless all dictionary values are the same number-count
        count_reviewer_name = len(reviews_dict['reviewer_name'])
        count_company_name = len(reviews_dict['company_name'])

        #print(f'count_reviewer_name is: {count_reviewer_name}')
        #print('\n')
        #print(f'count_company_name is: {count_company_name}')


        for _ in range(count_reviewer_name - count_company_name):
            reviews_dict['company_name'].append(company_name1)


    reviews_df = pd.DataFrame(reviews_dict)
    kwargs['ti'].xcom_push(key='extract_data', value=reviews_df)
    return reviews_df

'''
------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
'''




# # # def load_data1(**kwargs):
# # #     ti = kwargs['ti']
# # #     reviews_df = ti.xcom_pull(task_id='extract_reviews_data', key='extract_data')

# # #     if reviews_df is None:
# # #         raise ValueError("XCom pull for key 'extract_data' returned None. Check extract_data task.")

# #     # load_dotenv()
# #     # DB_NAME = os.getenv("DB_NAME")
# #     # DB_USER = os.getenv("DB_USER")
# #     # DB_PASSWORD = os.getenv("DB_PASSWORD")
# #     # DB_HOST = os.getenv("DB_HOST")
# #     # DB_PORT = os.getenv("DB_PORT")

# # #     try:
# # #         engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
# # #         con = engine.connect()

# # #     except Exception as e:
# # #         raise ValueError(f"Failed to connect to DB. Check your environment variables. Error: {e}")


# # #     reviews_df.head(0).to_sql('Reviews_data', con=con, if_exists='replace', index=False)
# # #     reviews_df.to_sql('Reviews_data', con=con, if_exists='append', index=False)
# # #     con.close()


# # #     kwargs['ti'].xcom_push(key='load_data1', value=reviews_df)





# import logging

# def load_data1():
#     logging.info("Starting load_data1")
    
#     reviews_df = extract_data()
    
#     # ti = kwargs['ti']
#     # reviews_df = ti.xcom_pull(task_id='extract_reviews_data', key='extract_data')
    
#     if reviews_df is None:
#         raise ValueError("No data found in XCom from extract_data")
#     logging.info("Received reviews_df from XCom")
    
#     # ... (load env variables) ...
#     load_dotenv()
#     DB_NAME = os.getenv("DB_NAME")
#     DB_USER = os.getenv("DB_USER")
#     DB_PASSWORD = os.getenv("DB_PASSWORD")
#     DB_HOST = os.getenv("DB_HOST")
#     DB_PORT = os.getenv("DB_PORT")

    
#     try:
#         engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
#         con = engine.connect()
#     except Exception as e:
#         logging.error("Error connecting to the database", exc_info=True)
#         raise

#     try:
#         reviews_df.head(0).to_sql('Reviews_data', con=con, if_exists='replace', index=False)
#         reviews_df.to_sql('Reviews_data', con=con, if_exists='append', index=False)
#     except Exception as e:
#         logging.error("Error writing data to SQL", exc_info=True)
#         raise
#     finally:
#         con.close()
    
#     logging.info("Data loaded into SQL successfully")
#     #ti.xcom_push(key='load_data1', value=reviews_df)




'''
------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
'''

def transform_data(**kwargs):
    ti = kwargs['ti']
    reviews_df = ti.xcom_pull(task_ids='extract_reviews_data', key='extract_data')

    #reviews_df = extract_data()


    # 1, Removind duplicate
    df = reviews_df.drop_duplicates()


    # 2, Removing irrelevant reviews by setting a minimum word count threshold
    df = df[df['review_text'].str.len() > 150]  # only reviews longer than 50 characters


    # 3, Proper case formatting by converting appropriate columns to lowercase and uppercase

    # Convert to lowercase, strip spaces, and capitalize first letters of sentences in the review_text column
    df['review_text'] = (
        df['review_text']
        .str.lower()                # Convert to lowercase
        .str.strip()                # Remove leading/trailing spaces
        .str.replace(r'\s+', ' ', regex=True)  # Replace multiple spaces with one
        .str.capitalize()           # Capitalize first character of the entire string
        .str.replace(r'\.\s*([a-z])', lambda x: f". {x.group(1).upper()}", regex=True) # Capitalize the first letter after periods and ensure proper spacing, # Find periods followed by optional spaces and a lowercase letter, # Replace with ". " + uppercase letter

    )



    # Convert to lowercase, strip spaces, and capitalize first letters of sentences in the review_title
    df['review_title'] = (
        df['review_title']
        .str.lower()                # Convert to lowercase
        .str.strip()                # Remove leading/trailing spaces
        .str.replace(r'\s+', ' ', regex=True)  # Replace multiple spaces with one
        .str.capitalize()           # Capitalize first character of the entire string
        .str.replace(r'\.\s*([a-z])', lambda x: f". {x.group(1).upper()}", regex=True) # Capitalize the first letter after periods and ensure proper spacing, # Find periods followed by optional spaces and a lowercase letter, # Replace with ". " + uppercase letter

    )



    # Convert to lowercase, strip spaces, and capitalize first letters of sentences in the reviewer_name

    # A function to handle mixed punctuation/spaces
    def smart_title_case(name):
        name = name.lower()
        # Capitalize first letter of each word
        name = name.title()
        # Fix Mc/Mac/O' prefixes (e.g., 'mcdonald' → 'McDonald')
        name = re.sub(r"\b(Ma?c)([a-z])", lambda m: m.group(1) + m.group(2).upper(), name)
        name = re.sub(r"\b(O')([a-z])", lambda m: m.group(1) + m.group(2).upper(), name)
        return name

    df['reviewer_name'] = df['reviewer_name'].apply(smart_title_case)


    df['reviewer_name'] = (
        df['reviewer_name']
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)  # Fix multiple spaces
        .str.strip()  # Remove leading/trailing spaces
        .apply(smart_title_case)  # Apply advanced capitalization
    )


    # Convert to lowercase, strip spaces, and capitalize first letters of sentences in the company_name

    df['company_name'] = (
        df['company_name']
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)  # Fix multiple spaces
        .str.strip()  # Remove leading/trailing spaces
        .str.capitalize() # Capitalize first character of the entire string
    )



    # 4, Next is to remove stopwords in the
    minimal_stopwords = {"um", "uh", "er", "ah"}


    # A helper function to remove stopwords
    def remove_stopwords(text):
        return ' '.join(word for word in text.split() if word not in minimal_stopwords)

    df['review_text'] = df['review_text'].apply(remove_stopwords)



    # 5, Next is to standardize the datetime to UTC
    # Pandas Datetime Conversion
    df['Date_of_experience'] = pd.to_datetime(df['Date_of_experience'], errors='coerce')

    # Pandas UTC Conversion
    # df['Date_of_experience'] = df['Date_of_experience'].dt.tz_localize('UTC') 



    # 6, Performing sentiment analysis
    import nltk
    nltk.download('punkt')
    nltk.download('averaged_perceptron_tagger')
    nltk.download('brown')
    nltk.download('wordnet')

    from textblob import TextBlob

    # Assume 'df' is your DataFrame and 'review_text' is the column containing reviews.
    #temp = df.copy()

    def get_sentiment(text):
        # Ensure text is a string to avoid errors if there are any non-string values.
        return TextBlob(str(text)).sentiment.polarity

    df['sentiment'] = df['review_text'].apply(get_sentiment)

    #print(df[['review_text', 'sentiment']].head())



    # Categorizing the Reviews and adding it to the dataframe
    def classify_sentiment(score):
        if score > 0:
            return 'positive'
        elif score < 0:
            return 'negative'
        else:
            return 'neutral'

    df['sentiment_category'] = df['sentiment'].apply(classify_sentiment)


    kwargs['ti'].xcom_push(key='transform_data', value=df)
    return df






'''
------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
'''



def load_data2(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_reviews_data', key='transform_data')

    # df = transform_data()

    # Data Loading
    # Loading the environment variables from .env files
    load_dotenv()
    connect_str = os.getenv('AZURE_CONNECT_STR')
    container_name = os.getenv('CONTAINER_NAME')

    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Load data to Azure Blob Storage
    files = [
        (df, 'cleandataset/cleaned_Techgenius_reviews_data.csv')
    ]

    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f'{blob_name} loaded into Azure Blob Storage')
