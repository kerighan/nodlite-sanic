from sanic import response

# =============================================================================
# WRAPPER
# =============================================================================


def json_task(func):
    async def wrapper(*args, **kwargs):
        task = await func(*args, **kwargs)
        await task.wait()
        return response.json(task.data, task.status)
    return wrapper
