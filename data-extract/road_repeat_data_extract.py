# Import libraries
import pandas as pd
from bs4 import BeautifulSoup
import yaml
import requests
import re
import pymongo
import uuid

# Function to get links from a specific page
def getPageLinks(page_url):
    # Get HTML
    try:
        base_html = requests.get(page_url).content
    except:
        print(f"Page link of {page_url} was not valid. Breaking out.")
        return(None)

    # Extract HTML code
    html = BeautifulSoup(base_html, 'html.parser')

    # List for links 
    review_links = [link.get('href') for link in html.find_all('a')]

    # Append prefix for links
    # Remove anything of None type
    review_links = [link for link in review_links if link is not None]
    review_links = ["https://runrepeat.com" + link if "https://runrepeat.com" not in link else link for link in review_links]
    
    # Return links
    return(review_links)

# Function to get specific info for review page
def getReviewInfo(url):
    # Get HTML
    html = requests.get(url).content

    # Convert to soup
    shoe_soup = BeautifulSoup(html, 'html.parser')

    # Extract specifics
    # Title
    title = shoe_soup.select('h1.p-name')[0].text.replace('\n', '')
    score = shoe_soup.select('div.corescore-big__score')[0].text.replace('\n', '')
    body_txt = shoe_soup.body.text
    # Remove new line char and double whitespace
    body_txt = body_txt.replace('\n', ' ')
    body_txt = re.sub(r'\s+', ' ', body_txt).strip()

    # Generate UUID
    temp_id = str(uuid.uuid4())

    # Define object to write to DynamoDB
    page_obj = {
        'id': temp_id,
        'name': title,
        'review_score': score,
        'review_txt': body_txt
    }

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
    collection = db["runrepeat"]

    # Base URL
    base_url = "https://runrepeat.com/catalog/running-shoes?page="

    # Iterate over pages and grab links
    page_links = []

    # Will modify the range once initial base load is complete
    for i in range(1, 64):
        print(f"Getting data for page {i}")
        # Create temporary URL
        temp_url = base_url + str(i)
        # Call function
        temp_links = getPageLinks(temp_url)
        # Append to other links
        page_links = page_links + temp_links

    # Remove any duplicate links
    page_links = list(set(page_links))

    for link in page_links:
        print(f"Loading data for {link}")
        try:
            temp_obj = getReviewInfo(link)
            collection.insert_one(temp_obj)
        except:
            print(f"Error getting data for: {link}")

if __name__ == "__main__":
    main()

