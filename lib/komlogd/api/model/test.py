# Some functions/classes needed for testing purposes
import asyncio
from unittest.mock import Mock, patch

def sync(loop=None, tr_support=False):
    ''' decorator for transforming coroutines into functions. '''
    def wrap(coro):
        def wrapped(*args, **kwargs):
            if not loop:
                print('NO LOOP')
            int_loop = loop or asyncio.new_event_loop()
            int_loop.run_until_complete(coro(*args, **kwargs))
            int_loop.close() if not loop else None
        return wrapped
    return wrap

class AsyncMock(Mock):

    def __call__(self, *args, **kwargs):
        sup = super(AsyncMock, self)
        async def coro():
            return sup.__call__(*args, **kwargs)
        return coro()

    def __await__(self):
        return self().__await__()

