from enum import Enum
from bson import ObjectId

def serialize(obj):
    """
    Convert objects like ObjectId, Enums, or custom objects to JSON-serializable values.
    Handles circular references and complex objects.
    """
    if isinstance(obj, ObjectId):
        return str(obj)  # Convert ObjectId to string
    if isinstance(obj, Enum):
        return obj.value  # Convert Enums to their value
    if hasattr(obj, "__dict__"):  # For objects with attributes
        return {key: serialize(value) for key, value in vars(obj).items()}
    if isinstance(obj, list):  # Serialize each item in a list
        return [serialize(item) for item in obj]
    if isinstance(obj, set):  # Convert sets to lists
        return list(obj)
    if isinstance(obj, dict):  # Serialize each key-value pair in a dictionary
        return {key: serialize(value) for key, value in obj.items()}
    return str(obj)  # Fallback: Convert anything else to a string
