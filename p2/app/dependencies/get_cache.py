from fastapi import Request


def get_cache(request: Request):
    return request.app.state.redis
