import pymongo
import pandas as pd


# Connect to mongodb
myclient = pymongo.MongoClient("mongodb://localhost:27017/", username='root', password='password')

mydb = myclient["social"]  # select the database
mycol = mydb["posts"]  # select the collection


def insert_new_posts():
    """
    Read new_posts.csv and comments.csv, process them, and insert into MongoDB.
    - Converts 'likes' column to 'like_count' (numeric)
    - Embeds comments as an array within each post
    """
    
    print("Reading new_posts.csv...")
    # Read the new posts
    posts_df = pd.read_csv('data/new_posts.csv')
    print(f"Loaded {len(posts_df)} new posts")
    
    print("Reading comments.csv...")
    # Read the comments
    comments_df = pd.read_csv('data/comments.csv')
    print(f"Loaded {len(comments_df)} comments")
    
    # Group comments by post_id
    print("\nGrouping comments by post_id...")
    comments_grouped = comments_df.groupby('post_id', group_keys=False).apply(
        lambda x: x.to_dict('records'), include_groups=False
    ).to_dict()
    
    print(f"Comments grouped for {len(comments_grouped)} posts")
    
    # Process posts
    print("\nProcessing posts...")
    posts_to_insert = []
    
    for _, row in posts_df.iterrows():
        post_id = row['post_id']
        
        # Create post document
        post = {
            '_id': str(post_id),  # Use post_id as _id
            'user_id': str(row['user_id']),
            'text': row['text'],
            'timestamp': row['timestamp'],
            'like_count': int(row['likes'])  # Store as numeric like_count
        }
        
        # Add comments array if this post has comments
        if post_id in comments_grouped:
            # Get comments for this post and clean them up
            post_comments = []
            for comment in comments_grouped[post_id]:
                cleaned_comment = {
                    'comment_id': str(comment['comment_id']),
                    'user_id': str(comment['user_id']),
                    'text': comment['text'],
                    'timestamp': comment['timestamp'],
                    'like_count': int(comment['like_count']),
                    'love_count': int(comment['love_count']),
                    'laugh_count': int(comment['laugh_count'])
                }
                post_comments.append(cleaned_comment)
            
            post['comments'] = post_comments
        else:
            # No comments for this post
            post['comments'] = []
        
        posts_to_insert.append(post)
    
    print(f"Processed {len(posts_to_insert)} posts with embedded comments")
    
    # Show sample before inserting
    print("\nSample post (first entry):")
    print("-" * 70)
    sample = posts_to_insert[0]
    print(f"  _id: {sample['_id']}")
    print(f"  user_id: {sample['user_id']}")
    print(f"  text: {sample['text'][:60]}...")
    print(f"  timestamp: {sample['timestamp']}")
    print(f"  like_count: {sample['like_count']}")
    print(f"  comments: {len(sample['comments'])} comments")
    if sample['comments']:
        print(f"    First comment: {sample['comments'][0]['text'][:50]}...")
    
    # Insert into MongoDB
    print("\n" + "=" * 70)
    print("Inserting posts into MongoDB...")
    result = mycol.insert_many(posts_to_insert, ordered=False)
    
    print(f"✓ Successfully inserted {len(result.inserted_ids)} new posts!")
    
    # Show statistics
    print("\n" + "=" * 70)
    print("Statistics:")
    print("=" * 70)
    
    total_comments = sum(len(post['comments']) for post in posts_to_insert)
    posts_with_comments = sum(1 for post in posts_to_insert if post['comments'])
    avg_comments = total_comments / len(posts_to_insert) if posts_to_insert else 0
    
    print(f"Total posts inserted: {len(posts_to_insert)}")
    print(f"Total comments embedded: {total_comments}")
    print(f"Posts with comments: {posts_with_comments}")
    print(f"Posts without comments: {len(posts_to_insert) - posts_with_comments}")
    print(f"Average comments per post: {avg_comments:.2f}")
    
    # Verify in MongoDB
    print("\nVerifying in MongoDB...")
    total_in_db = mycol.count_documents({})
    print(f"✓ Total posts in database: {total_in_db}")


# Run the function
insert_new_posts()