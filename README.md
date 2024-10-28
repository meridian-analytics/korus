# Korus

Welcome to `Korus`, a Python package for managing acoustic annotations.


## Documentation

Full documentation including code examples and Jupyter Notebook tutorials
can be found at [meridian-analytics.github.io/korus](https://meridian-analytics.github.io/korus).


## Installation 

```
python setup.py sdist
pip install dist/korus*
```


## Support

oliver.kirsebom@gmail.com


## Authors

Oliver Kirsebom


# Acknowledgements

Korus was developed to meet the data management needs of the 
[HALLO (Humans and Algorithms Listening and Looking for Orcas)](https://orca.research.sfu.ca)
project. The design of Korus has been informed by numerous conversations 
with members of the HALLO team. Thank you all for your inputs!

The HALLO project has been generously supported by the Canadian Department of Fisheries 
and Oceans through Grants and Contribution Agreements and the Canada Nature Fund for Aquatic 
Species at Risk program. Furthermore, graduate students in the HALLO project have received 
support through partnerships with NSERC, MITACS, SIMRES, JASCO, SMRU Consulting and the Vancouver 
Port's ECHO Program. 


## License

[GNU GPLv3 license](https://www.gnu.org/licenses/) 


## Project status

Korus is still in its infancy and under active development. We aim to have a first, stable 
release out by the end of 2024, but until then users should be prepared for substantial and 
non-backward compatible changes to the code base. If you have any feedback for us, we would 
love to hear it. Please create an issue with your question or suggestion.


## Notes for developers

The database schema diagram can be generated automatically with,
```
eralchemy -i sqlite:///tutorial/t2/db_t3.sqlite -o figures/erd_from_sqlite.png
```
You can install `eralchemy` with
```
sudo apt-get install python3-dev graphviz libgraphviz-dev pkg-config
pip install 'SQLAlchemy<1.4'
pip install eralchemy
```

Publish a new release to PyPy with,
```
python setup.py sdist bdist_wheel
twine upload dist/korus-X.Y.Z.tar.gz
```


## Miscellaneous

 * [SQLite data types](https://www.sqlite.org/datatype3.html)


