import pymongo
import csv


# Connect to mongodb
myclient = pymongo.MongoClient("mongodb://localhost:27017/", username='root', password='password')

mydb = myclient["social"]  # select the database
mycol = mydb["posts"]  # select the collection


# Function to process pipe-separated likes into array
def process_likes(likes_string):
    """Convert pipe-separated likes (e.g. "123|456|789") into array."""
    if not likes_string or likes_string.strip() == "":
        return []
    
    # Split by pipe and filter out empty strings
    user_ids = [user_id.strip() for user_id in likes_string.split("|") if user_id.strip()]
    return user_ids


# Function to read posts from CSV and insert into MongoDB
def insert_posts_from_csv():
    csv_file = '/data/initial_posts.csv'
    
    print(f"Reading posts from {csv_file}...")
    
    posts = []
    
    # Read CSV file
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            # Create post document
            post = {
                '_id': row['post_id'],  # Use post_id as _id
                'user_id': row['user_id'],
                'text': row['text'],
                'timestamp': row['timestamp'],
                'user_likes': process_likes(row['likes'])  # Convert likes to array
            }
            posts.append(post)
    
    print(f"Loaded {len(posts)} posts from CSV")
    
    # Insert all posts into MongoDB
    print("Inserting posts into MongoDB...")
    result = mycol.insert_many(posts, ordered=False)
    
    print(f"Successfully inserted {len(result.inserted_ids)} posts!")
    
    # Show sample
    print("\nSample post:")
    sample = mycol.find_one()
    print(f"  _id: {sample['_id']}")
    print(f"  user_id: {sample['user_id']}")
    print(f"  text: {sample['text'][:60]}...")
    print(f"  user_likes: {sample['user_likes']} ({len(sample['user_likes'])} likes)")


# Run the function
insert_posts_from_csv()