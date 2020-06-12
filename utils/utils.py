def add_method(cls, func):
    setattr(cls, func.__name__, func)
