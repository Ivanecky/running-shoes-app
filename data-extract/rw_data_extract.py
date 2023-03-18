# Import libraries
import pandas as pd
from bs4 import BeautifulSoup
import yaml
import requests
import re
import pymongo
import uuid

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
    review_links = [link for link in review_links if "review" in link or "multi" in link]

    # Return
    return(review_links)

def getShoeReviews(page_url):
    # Get HTML
    page_html = requests.get(page_url).content

    # Extract HTML via soup
    page_soup = BeautifulSoup(page_html, 'html.parser')

    # Get single elements
    # Title
    page_title = page_soup.find('h1', {'class': 'h2 desc_top-head-title'}).text.replace('\n', '')

    # Get remaining text within the body of the review
    body_txt = page_soup.find('div', {'class': 'display-review-container'}).text.replace('\n', '')

    # Remove new line char and double whitespace
    body_txt = re.sub(r'\s+', ' ', body_txt).strip()


    # Create page object
    # Generate UUID
    temp_id = str(uuid.uuid4())

    # Define object to write to DynamoDB
    page_obj = {
        'id': temp_id,
        'name': page_title,
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
    collection = db["runningwarehouse"]

    # List of links to scrape
    base_links = [
        # Mens
        "https://www.runningwarehouse.com/Mens_Neutral_Road_Running_Shoes/catpage-MNROAD.html",
        "https://www.runningwarehouse.com/Mens_Stability_Road_Running_Shoes/catpage-MSROAD.html",
        "https://www.runningwarehouse.com/Mens_Road_Racing_Shoes__Flats/catpage-MRAC.html",
        "https://www.runningwarehouse.com/trailshoesmen.html",
        # Womens
        "https://www.runningwarehouse.com/Womens_Neutral_Road_Running_Shoes/catpage-WNROAD.html",
        "https://www.runningwarehouse.com/Womens_Stability_Road_Running_Shoes/catpage-WSROAD.html",
        "https://www.runningwarehouse.com/Womens_Road_Racing_Shoes__Flats/catpage-WRAC.html",
        "https://www.runningwarehouse.com/trailshoeswomen.html"
    ]

    # Iterate over base links to get shoe links
    shoe_links = []
    for link in base_links:
        try:
            temp_links = getLinks(link)
            shoe_links = shoe_links + temp_links
        except:
            print(f"Error getting data for {link}")

    # Remove any duplicate links
    shoe_links = list(set(shoe_links))

    # Iterate over links and push data to Mongo
    for s in shoe_links:
        try:
            print(f"Uploading data for {s}")
            temp_obj = getShoeReviews(s)
            collection.insert_one(temp_obj)
        except:
            print(f"Error getting data for {s}")

if __name__ == "__main__":
    main()