# General utility functions

def hash_string(s):
    import hashlib
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def get_current_timestamp():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

