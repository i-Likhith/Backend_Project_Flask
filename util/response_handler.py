from flask import jsonify

def success_response(message="Operation successful.", data=None, status_code=200):
    """Generates a standardized success JSON response."""
    response = {"status": "success", "message": message}
    if data is not None:
        response["data"] = data
    return jsonify(response), status_code

def error_response(message="An error occurred.", errors=None, status_code=500):
    """Generates a standardized error JSON response."""
    response = {"status": "error", "message": message}
    if errors is not None:
        response["errors"] = errors
    return jsonify(response), status_code

def info_response(message="Information.", data=None, status_code=200):
    """Generates a standardized informational JSON response."""
    response = {"status": "info", "message": message}
    if data is not None:
        response["data"] = data
    return jsonify(response), status_code

def not_found_response(message="Resource not found.", status_code=404):
    """Generates a standardized 404 Not Found JSON response."""
    return jsonify({"status": "error", "message": message}), status_code

def bad_request_response(message="Bad request. Please check your input.", errors=None, status_code=400):
    """Generates a standardized 400 Bad Request JSON response."""
    response = {"status": "error", "message": message}
    if errors is not None:
        response["errors"] = errors
    return jsonify(response), status_code

