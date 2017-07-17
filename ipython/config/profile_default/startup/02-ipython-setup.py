def run_magics():
    from IPython import get_ipython
    ipython = get_ipython()
    ipython.magic("matplotlib inline")


run_magics()
