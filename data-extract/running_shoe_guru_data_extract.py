# Import libraries
import pandas as pd
from bs4 import BeautifulSoup
import yaml
import requests
import re
import pymongo
import uuid

# Function to get review links
# Might need to make this be able to paginate
def getReviewLinks(base_url):

    # Get HTML content
    content = requests.get(base_url).content

    # Extract soup
    base_soup = BeautifulSoup(content, 'html.parser')

    # List for links 
    review_links = [link.get('href') for link in base_soup.find_all('a')]

    # Filter to only those that are reviews
    review_links = [link for link in review_links if "review" in link and "https" in link]

    # Return links
    return(review_links)

# Function to get a page object
def getPageObj(url):

    shoe_content = requests.get(url).content

    # Get HTML
    shoe_soup = BeautifulSoup(shoe_content, 'html.parser')

    # Get specifics to shoe
    # Title
    title = shoe_soup.select('h1')[0].text.replace('\n', '').strip()
    # Expert score
    score = shoe_soup.select('div.panel')[0].text.replace('\n', '')
    # Remove "expert score" from score
    score = re.sub("expert score", "", score).strip()
    # Get body text (three parts)
    # Get verdict
    try:
        body_txt_verdict = shoe_soup.find("div", {"class": "big"}).text.replace('\n', '').strip()
    except:
        print(f"No verdict was found for {url}")
        body_txt_verdict = ""
    # Get main review
    try:
        body_txt_main = shoe_soup.find("div", {"class": "review-sections"}).text.replace('\n', '').strip()
    except:
        try:
            body_txt_main = shoe_soup.find("div", {"id": "review-body-content"}.text.replace('\n', '').strip())
        except:
            print(f"No body text was found for {url}")
            body_txt_main = ""
    # Get scores
    try:
        body_txt_scores = shoe_soup.find("div", {"class": "panel panel-inverted"}).text.replace('\n', '').strip()
    except:
        print(f"No score was found for {url}")
        body_txt_scores = ""

    # Combine three string components into one big string
    body_txt = body_txt_main + body_txt_scores + body_txt_verdict

    # Create page object
    # Generate UUID
    temp_id = str(uuid.uuid4())

    # Define object to write to DynamoDB
    page_obj = {
        'id': temp_id,
        'name': title,
        'review_score': score,
        'review_txt': body_txt
    }

    # Return the object
    return(page_obj)

def main():

    # Read YAML file for MongoDB pwd
    # Import cred from yaml
    with open("/Users/samivanecky/git/running-shoe-app/mongo_creds.yaml") as f:
        creds = yaml.safe_load(f)

    # Connect to mongo
    connection_str = "mongodb+srv://trackrabbit:" + str(creds['pwd']) + "@running-shoes.ncb9yfc.mongodb.net/?retryWrites=true&w=majority"

    # Connect to MongoDB
    client = pymongo.MongoClient(connection_str)
    # Connvect to database
    db = client["running-shoes"]
    # Connect to collection
    collection = db["runningshoesguru"]

     # Base url
    base_url = "https://www.runningshoesguru.com/reviews/page/"

    # Iterate over pages and grab links
    page_links = []

    # Will modify the range once initial base load is complete
    for i in range(1, 105):
        print(f"Getting data for page {i}")
        # Create temporary URL
        temp_url = base_url + str(i)
        # Call function
        temp_links = getReviewLinks(temp_url)
        # Append to other links
        page_links = page_links + temp_links

    # Remove any duplicate links
    page_links = list(set(page_links))

    for link in page_links:
        print(f"Loading data for {link}")
        try:
            temp_obj = getPageObj(link)
            collection.insert_one(temp_obj)
        except:
            print(f"Error getting data for: {link}")

if __name__ == "__main__":
    main()