import setuptools

REQUIRED_PACKAGES = [
    'jsonschema',
    'nytaxirides'
]

setuptools.setup(
    name='nytaxirides',
    version='0.0.1',
    description='NY Taxi records analysis with DataFlow',
    author='Gabor Hrasko',
    author_email='gabor@hrasko.com',
    url='https://github.com/ghrasko/nytaxirides.git',
    install_requires=[REQUIRED_PACKAGES],
    packages=setuptools.find_packages(),
    python_requires='>=3.7',
)

