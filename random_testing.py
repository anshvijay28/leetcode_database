import pymongo, certifi, os
from dotenv import load_dotenv
import ssl

load_dotenv()
MONGODB_URL = os.getenv("MONGODB_URL")

mongo_client = pymongo.MongoClient(MONGODB_URL, tlsCAFile=certifi.where())
print(mongo_client.server_info())