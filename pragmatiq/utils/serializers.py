from typing import Any, Dict, List, Tuple

from pragmatiq.types import F


def serialize_args(args: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Serialize arguments, replacing callable objects with their names.

    Converts a tuple of arguments into a serializable form by replacing functions with
    dictionaries containing their names. This is useful for storing or transmitting
    arguments that include callable objects, which cannot be directly serialized.

    Args:
        args: A tuple of arguments to serialize, which may include callable objects.

    Returns:
        Tuple[Any, ...]: A tuple of serialized arguments, where callables are replaced
            with dictionaries of the form {"type": "function", "name": str}.
    """
    serialized_args: List[Any] = []
    for arg in args:
        if callable(arg):
            serialized_args.append({"type": "function", "name": arg.__name__})
        else:
            serialized_args.append(arg)
    return tuple(serialized_args)


def deserialize_args(
    args: Tuple[Any, ...],
    func_mapping: Dict[str, F],
) -> Tuple[Any, ...]:
    """Deserialize arguments, recovering functions from their names using a mapping.

    Converts a tuple of serialized arguments back into their original form by replacing
    function descriptors (dictionaries) with the corresponding callable objects from
    a provided function mapping. This reverses the process performed by serialize_args.

    Args:
        args: A tuple of serialized arguments, potentially containing function descriptors.
        func_mapping: A dictionary mapping function names to their callable implementations.

    Returns:
        Tuple[Any, ...]: A tuple of deserialized arguments with functions restored.

    Raises:
        ValueError: If a function name in the serialized args is missing or not found in
            the func_mapping.
    """
    deserialized_args: List[Any] = []
    for arg in args:
        if isinstance(arg, dict) and arg.get("type") == "function":
            func_name: str | None = arg.get("name")
            if not func_name:
                raise ValueError(f"Function name {func_name} not found in mapping.")
            func: F | None = func_mapping.get(func_name)
            if not func:
                raise ValueError(f"Function {func_name} not found in mapping.")
            deserialized_args.append(func)
        else:
            deserialized_args.append(arg)
    return tuple(deserialized_args)
