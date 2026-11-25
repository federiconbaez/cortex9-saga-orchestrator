class StepError(Exception):
    """Base for errors raised by step handlers to classify a failure."""


class TransientStepError(StepError):
    """Retry may help. The effect may or may not have happened."""


class PermanentStepError(StepError):
    """Definitive rejection. The participant guarantees it had no effect."""


class UnknownSaga(LookupError):
    pass


class MissingHandler(LookupError):
    pass
