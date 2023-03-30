import pytest
from traitlets import HasTraits, TraitError

from pangeo_forge_runner.commands._traits import CallableArgsInjection, JSONDict


def test_json_dict():
    # Test that custom trait JSONDict.from_string method works as expected.
    # Compare to traitlets test of same method:
    # https://github.com/ipython/traitlets/blob/21fdc40d230661c0ac0c1386360786bc41f35a88/traitlets/tests/test_traitlets.py#L2881-L2897

    as_dict = {"a": {"b": "c"}}
    as_string = '{"a": {"b": "c"}}'
    json_dict = JSONDict()
    assert json_dict.from_string(as_string) == as_dict


@pytest.mark.parametrize(
    "inject, raises",
    (
        [{"func": {"arg": 0}}, False],
        [123, True],  # not a dict
        [{"func": 1}, True],  # not a nested dict
        [{"0func": {"arg": 0}}, True],  # top-level key not a valid variable name
        [{"func": {"0arg": 0}}, True],  # second-level key not a valid variable name
        [{"while": {"arg": 0}}, True],  # top-level key overwrites keyword
        [{"func": {"while": 0}}, True],  # second-level key overwrites keyword
    ),
)
def test_callable_args_injection(inject, raises):
    # Compare to traitlets test:
    # https://github.com/ipython/traitlets/blob/21fdc40d230661c0ac0c1386360786bc41f35a88/traitlets/tests/test_traitlets.py#L114-L127

    class HT(HasTraits):
        c_a_i = CallableArgsInjection

    if raises:
        with pytest.raises(TraitError, match=CallableArgsInjection.info_text):
            HT(c_a_i=inject)
    else:
        HT(c_a_i=inject)
