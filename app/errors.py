class APIError(Exception):
    def __init__(self, status, code, message=None, headers=None):
        self.status, self.code = status, code
        self.message = message or code.replace('_', ' ')
        self.headers = headers or {}
