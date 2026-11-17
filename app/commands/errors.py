from app.protocol import RedisResponse


class CommandError(Exception):
    def __init__(self, response: RedisResponse, *, abort_transaction: bool = True):
        super().__init__(str(response.value))
        self.response = response
        self.abort_transaction = abort_transaction
