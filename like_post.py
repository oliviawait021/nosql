import pymongo


# Connect to mongodb
myclient = pymongo.MongoClient("mongodb://localhost:27017/", username='root', password='password')

mydb = myclient["social"]  # select the database
posts_col = mydb["posts"]  # posts collection
users_col = mydb["users"]  # users collection


def like_post(user_id, post_id):
    """
    Implements the new post liking functionality:
    1. For posts with user_likes array: adds user_id to the array
    2. For posts with like_count: increments the count
    3. Adds the post_id to the user's posts_liked array
    4. Prevents duplicate likes (idempotent operation)
    
    Args:
        user_id: The ID of the user liking the post
        post_id: The ID of the post being liked
        
    Returns:
        dict: Result information including success status and message
    """
    # Convert to strings for consistency
    user_id_str = str(user_id)
    post_id_str = str(post_id)
    
    # First, check if the user has already liked this post
    user = users_col.find_one(
        {"_id": user_id_str},
        {"posts_liked": 1}
    )
    
    # Check if user already liked this post
    if user and "posts_liked" in user and post_id_str in user["posts_liked"]:
        return {
            "success": False,
            "message": "User has already liked this post",
            "user_id": user_id_str,
            "post_id": post_id_str
        }
    
    # Get the post to determine its structure
    post = posts_col.find_one({"_id": post_id_str})
    
    if not post:
        return {
            "success": False,
            "message": "Post not found",
            "user_id": user_id_str,
            "post_id": post_id_str
        }
    
    # Determine if this is an old post (user_likes array) or new post (like_count number)
    if "user_likes" in post:
        # Old post structure - add user to user_likes array
        if user_id_str in post["user_likes"]:
            return {
                "success": False,
                "message": "User has already liked this post",
                "user_id": user_id_str,
                "post_id": post_id_str
            }
        
        post_result = posts_col.update_one(
            {"_id": post_id_str},
            {"$addToSet": {"user_likes": user_id_str}}
        )
    else:
        # New post structure - increment like_count
        post_result = posts_col.update_one(
            {"_id": post_id_str},
            {"$inc": {"like_count": 1}}
        )
    
    # Step 2: Add post_id to user's posts_liked array
    user_result = users_col.update_one(
        {"_id": user_id_str},
        {"$addToSet": {"posts_liked": post_id_str}},
        upsert=True  # Create user document if it doesn't exist
    )
    
    return {
        "success": True,
        "message": "Post liked successfully",
        "user_id": user_id_str,
        "post_id": post_id_str,
        "post_type": "user_likes" if "user_likes" in post else "like_count",
        "post_modified": post_result.modified_count > 0,
        "user_modified": user_result.modified_count > 0,
        "user_created": user_result.upserted_id is not None
    }


def get_post_info(post_id):
    """Helper function to get post info"""
    post_id_str = str(post_id)
    post = posts_col.find_one({"_id": post_id_str})
    if not post:
        return None
    
    if "user_likes" in post:
        return {
            "type": "user_likes",
            "count": len(post["user_likes"])
        }
    elif "like_count" in post:
        return {
            "type": "like_count",
            "count": post["like_count"]
        }
    return None


def get_user_liked_posts(user_id):
    """Helper function to get posts liked by a user"""
    user_id_str = str(user_id)
    user = users_col.find_one({"_id": user_id_str}, {"posts_liked": 1})
    if user and "posts_liked" in user:
        return user["posts_liked"]
    return []


def demonstrate_functionality():
    """Demonstrate the like_post functionality with test values"""
    
    # Test cases
    test_cases = [
        {"user_id": 232534, "post_id": 32185},
        {"user_id": 232534, "post_id": 817255},
        {"user_id": 232534, "post_id": 817255},  # Duplicate - should be ignored
    ]
    
    print("Running like_post tests...\n")
    
    # Check initial state
    user_id = 232534
    initial_user = users_col.find_one({"_id": str(user_id)})
    print(f"Initial user {user_id} state:")
    if initial_user:
        print(f"  posts_liked: {initial_user.get('posts_liked', [])}")
    else:
        print(f"  User does not exist yet")
    print()
    
    for i, test in enumerate(test_cases, 1):
        user_id = test["user_id"]
        post_id = test["post_id"]
        
        # Show post info before
        post_info_before = get_post_info(post_id)
        if post_info_before:
            print(f"Post {post_id} before: {post_info_before['type']} = {post_info_before['count']}")
        
        # Perform the like action
        result = like_post(user_id, post_id)
        
        # Show result
        status = "✓" if result["success"] else "⚠️"
        print(f"{status} like_post({user_id}, {post_id}) → {result['message']}")
        
        if result["success"]:
            post_info_after = get_post_info(post_id)
            print(f"   Post {post_id} after: {post_info_after['type']} = {post_info_after['count']}")
        print()
    
    # Show final user state
    print("=" * 70)
    user_id = 232534
    final_user = users_col.find_one({"_id": str(user_id)})
    print(f"Final user {user_id} document:")
    if final_user:
        for key, value in final_user.items():
            if key == "posts_liked":
                print(f"  {key}: {value}")
    
    liked_posts = get_user_liked_posts(user_id)
    print(f"\nUser {user_id} posts_liked: {liked_posts}")
    print()


if __name__ == "__main__":
    demonstrate_functionality()