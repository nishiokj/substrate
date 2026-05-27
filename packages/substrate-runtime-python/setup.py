from setuptools import Distribution, setup
from wheel.bdist_wheel import bdist_wheel


class RuntimeDistribution(Distribution):
    def has_ext_modules(self) -> bool:
        return True


class RuntimeWheel(bdist_wheel):
    def finalize_options(self) -> None:
        super().finalize_options()
        self.root_is_pure = False
        self.python_tag = "py3"

    def get_tag(self) -> tuple[str, str, str]:
        _python, _abi, platform = super().get_tag()
        return ("py3", "none", platform)


setup(distclass=RuntimeDistribution, cmdclass={"bdist_wheel": RuntimeWheel})
