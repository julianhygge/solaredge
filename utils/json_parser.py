import json
import re
import html
from utils.logger_config import get_logger

logger = get_logger(__name__)

def fix_invalid_json(text: str) -> str:
    """
    Attempts to fix common issues in malformed JSON strings.
    - Removes JavaScript-style comments and fields.
    - Corrects boolean expressions.
    - Fixes invalid backslashes.
    - Decodes HTML entities.
    """
    # Remove JS-style fields (e.g., viewDashboard:true, ...)
    text = re.sub(r'\s*view[A-Za-z]+\s*:\s*[^,\n]+,?', '', text)
    # Remove any leftover JS boolean expressions (e.g., true && false && true,)
    text = re.sub(r'\s*:\s*true\s*&&.*?,', ': false,', text)
    # Fix invalid backslashes (ensure it doesn't break valid escapes)
    text = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', text)
    # Decode HTML entities
    text = html.unescape(text)
    return text

def tolerant_json_decode(text: str):
    """
    Tries to decode a JSON string, attempting to fix it if initial parsing fails.
    Uses demjson3 as a last resort.
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError as e1:
        logger.debug("Standard JSON decode failed: %s. Attempting to clean...", e1)
        try:
            cleaned_text = fix_invalid_json(text)
            logger.debug("Cleaned text for JSON parsing: %s", cleaned_text[:500]) # Log snippet of cleaned text
            return json.loads(cleaned_text)
        except json.JSONDecodeError as e2:
            logger.error("Cleaned JSON decode failed: %s. Trying demjson3...", e2)
            try:
                import demjson3
                return demjson3.decode(text) # Try demjson3 with original text
            except Exception as e3:
                logger.error("demjson3 decode failed: %s", e3)
                logger.error("Response text snippet (first 800 chars): %s", text[:800])
                return None
