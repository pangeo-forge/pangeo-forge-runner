def some_callable(some_argument):
    pass


some_callable(
    some_argument=_CALLABLE_ARGS_INJECTIONS.get("some_callable", {}).get(  # noqa
        "some_argument"
    )
)
