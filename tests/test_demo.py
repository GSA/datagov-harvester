from demo import __version__
from demo.example import hello

def test_version():
    assert __version__ == "0.1.0"

def test_hello():
    assert hello('name') == "Hello name!"