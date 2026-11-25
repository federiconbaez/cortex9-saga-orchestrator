"""Pure saga domain: definitions, events, the Saga aggregate and recovery strategies.

Nothing here performs I/O, reads clocks or draws random numbers: time and
randomness are passed in as plain values by the application layer.
"""
