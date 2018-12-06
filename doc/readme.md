# S2Graph Documentation

## Dependencies
  [Python](https://www.python.org/)
  
  [Sphinx](http://www.sphinx-doc.org/en/master/)

  [Read the Docs Sphinx Theme](https://sphinx-rtd-theme.readthedocs.io/en/latest/index.html)

I used [`pip`](https://pip.pypa.io/en/stable/installing/) to install Python module.
I used [`virtualenv`](https://virtualenv.pypa.io/en/latest/) to isolate the Python environment.

> Depending on your environment, the tools(pip, virtualenv) may not be required

## Quickstart

All work is done under the `s2graph/doc` folder.

```
cd doc
```

### Creating a virtualenv environment for documnet build

If `pip` is not installed, you need to install it first by referring to the link: https://pip.pypa.io/en/stable/installing/

```
pip install virtualenv

virtualenv -p python s2graph_doc
source s2graph_doc/bin/activate
```

### install sphinx and theme
```
pip install Sphinx
pip install sphinx_rtd_theme 
```

### Building
```
make html
```

### Viewing
```
# python 2
pushd build/html && python -m SimpleHTTPServer 3000 

# python 3
pushd build/html && python -m http.server 3000 
```

### Screenshot

<img src="https://user-images.githubusercontent.com/1182522/48395569-04995d00-e75b-11e8-87b8-2f28662ef3ca.png">
