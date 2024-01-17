from data_viz.utils import filter_string


def test_filter_string_1():
    input_string = "1001 AB"
    expected_output = "1001"

    result = filter_string(input_string)

    assert result == expected_output


def test_filter_string_2():
    input_string = "1001AB"
    expected_output = "1001"

    result = filter_string(input_string)

    assert result == expected_output


def test_filter_string_3():
    input_string = "1001"
    expected_output = "1001"

    result = filter_string(input_string)

    assert result == expected_output


def test_filter_string_4():
    input_string = 1001
    expected_output = "1001"

    result = filter_string(input_string)

    assert result == expected_output


def test_filter_string_5():
    input_string = None
    expected_output = None

    result = filter_string(input_string)

    assert result == expected_output
