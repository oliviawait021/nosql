import pymongo


# Connect to mongodb
myclient = pymongo.MongoClient("mongodb://localhost:27017/", username='root', password='password')

mydb = myclient["social"]  # select the database
posts_col = mydb["posts"]  # select the collection


def search_posts_by_keyword(search_term):
    """
    Searches the text field of posts using full-text search.
    Returns the 5 most recent posts that match the search term.
    
    Args:
        search_term: The keyword to search for in post text
        
    Returns:
        List of matching posts, ordered by timestamp (most recent first)
    """
    # Perform text search and sort by timestamp descending
    results = posts_col.find(
        {"$text": {"$search": search_term}},
        {"_id": 1, "user_id": 1, "text": 1, "timestamp": 1, "score": {"$meta": "textScore"}}
    ).sort([
        ("timestamp", -1)  # Most recent first
    ]).limit(5)
    
    return list(results)


if __name__ == "__main__":
    # Test search term
    search_term = "interdum"
    
    print("=" * 70)
    print("Text Search with Indexing")
    print("=" * 70)
    print()
    
    # Create text index on the text field
    print("Creating text index on 'text' field...")
    try:
        # Drop existing text index if it exists
        try:
            posts_col.drop_index("text_text")
            print("  Dropped existing text index")
        except:
            pass  # Index might not exist yet
        
        # Create new text index
        result = posts_col.create_index([("text", "text")])
        print(f"  ✓ Created index: {result}")
    except Exception as e:
        print(f"  Note: {e}")
    
    print()
    print("-" * 70)
    print(f"Searching for: '{search_term}'")
    print("-" * 70)
    print()
    
    # Search for posts
    try:
        results = search_posts_by_keyword(search_term)
        
        if results:
            print(f"Found {len(results)} posts (showing top 5 most recent):\n")
            
            for i, post in enumerate(results, 1):
                print(f"Post {i}:")
                print(f"  _id: {post['_id']}")
                print(f"  user_id: {post['user_id']}")
                print(f"  timestamp: {post['timestamp']}")
                print(f"  text: {post['text'][:80]}..." if len(post['text']) > 80 else f"  text: {post['text']}")
                if 'score' in post:
                    print(f"  search_score: {post['score']:.2f}")
                print()
        else:
            print(f"No posts found matching '{search_term}'")
    
    except Exception as e:
        print(f"Error searching: {e}")
    
    print("=" * 70)