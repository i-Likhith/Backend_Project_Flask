import requests

def check_url_status(url):
    '''
    Checks the HTTP status of a given URL.
    Returns a dictionary with link, status_code, is_working, and error (if any).
    '''
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        if 200 <= response.status_code < 300:
            return {"link": url, "status_code": response.status_code, "is_working": True}
        else:
            return {"link": url, "status_code": response.status_code, "is_working": False}
    except requests.exceptions.RequestException as e:
        return {"link": url, "status_code": None, "is_working": False, "error": str(e)}
    
