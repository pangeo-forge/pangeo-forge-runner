def some_callable(some_argument):
    pass


recipes = {
    "a": some_callable(
        some_argument=_CALLABLE_ARGS_INJECTIONS.get("some_callable", {}).get(  # noqa
            "some_argument"
        )
    ),
    "b": some_callable(
        some_argument=_CALLABLE_ARGS_INJECTIONS.get("some_callable", {}).get(  # noqa
            "some_argument"
        )
    ),
}
