# Import libraries
import pandas as pd
from bs4 import BeautifulSoup
import yaml
import requests
import re
import pymongo
import uuid

# Function to get links from page
def getLinks(base_url):

    # Get HTML
    base_html = requests.get(base_url).content

    # Extract HTML code
    html = BeautifulSoup(base_html, 'html.parser')

    # List for links 
    review_links = [link.get('href') for link in html.find_all('a')]

    # Remove None values
    review_links = list(link for link in review_links if link is not None)

    # Filter out only those that are reviews
    review_links = [link for link in review_links if "review" in link]

    # Return
    return(review_links)

# Function to get single page object
def getPageObj(page_url):
    # Get HTML
    page_html = requests.get(page_url).content

    # Extract HTML via soup
    page_soup = BeautifulSoup(page_html, 'html.parser')

    # Get single elements
    # Title
    page_title = page_soup.select('h3.post-title')[0].text.replace('\n', '')
    # Date
    page_dt = page_soup.select('h2.date-header')[0].text.replace('\n', '')

    # Get remaining text within the body of the review
    body_txt = page_soup.body.text

    # Remove new line char and double whitespace
    body_txt = body_txt.replace('\n', ' ')
    body_txt = re.sub(r'\s+', ' ', body_txt).strip()

    # Generate UUID
    temp_id = str(uuid.uuid4())

    # Define object to write to DynamoDB
    page_obj = {
        'id': temp_id,
        'name': page_title,
        'review_d': page_dt,
        'review_txt': body_txt
    }

    # Return
    return(page_obj)

# Main program
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
    collection = db["roadtrailrun"]

    # Get page links
    page_links = getLinks("https://www.roadtrailrun.com/p/blog-page.html")

    # Iterate over page links and upload to MongoDB
    for link in page_links:
        print(f"Loading data for {link}")
        try:
            temp_obj = getPageObj(link)
            collection.insert_one(temp_obj)
        except:
            print(f"Error getting data for: {link}")


if __name__ == "__main__":
    main()