from importlib import resources
import os


def binary_path() -> str:
    name = "substrate-runtime.exe" if os.name == "nt" else "substrate-runtime"
    return str(resources.files(__package__).joinpath("bin", name))


__all__ = ["binary_path"]
