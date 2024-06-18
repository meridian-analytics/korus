Welcome to Korus, a Python package for managing acoustic metadata and annotations.


Briefly about Korus
=====================

Korus was designed with machine-learning applications in mind. Specifically, 
to help build training and test sets for advancing sound detection and 
classification models in underwater acoustics.

Korus stores acoustic annotations and metadata in an SQLite database. 
The data are organized in several cross-referenced tables, the main 
tables being

 - deployment: metadata pertaining hydrophone deployments
 - file: metadata pertaining to audio files
 - storage: data storage locations for audio files
 - taxonomy: taxonomies used for annotating sound sources and sound types
 - job: metadata pertaining to annotation jobs
 - annotation: acoustic annotations
 - label: standardized annotation labels employed by the taxonomies
 - tag: non-standardized tags used for annotating sound events

The Korus Python API includes functions to facilitate common operations 
on the database such as adding data and performing certain searches. 
However, it is also possible to interact directly with the database 
using standard SQLite syntax.

The Python API also includes functions for creating and managing annotation 
taxonomies programmatically. 


Usage
=====

There are two ways to use Korus:

 1. Write your own Python scripts using the Korus Python API
 2. Use the command-line tool `ktam-submit` included in Korus


Getting Started
===============

To familiarize yourself with the Korus Python API we encourage you to 
explore the Jupyter notebooks :ref:`tutorials_page`.


Code Examples
=============

Import modules and connect to the database,

.. code-block:: python
    
    >>> import sqlite3
    >>> import korus.db as kdb
    >>> conn = sqlite3.connect("db.sqlite")

View the taxonomy used by the acoustic analyst for labelling sound sources,

.. code-block:: python
    
    >>> tax = kdb.get_taxonomy(conn)
    >>> tax.show(append_name=True)
    Unknown
    ├── Anthro [Anthropogenic]
    │   └── Boat
    │       ├── Engine
    │       └── Prop [Propeller]
    └── Bio [Biological]
        └── Whale
            ├── HW [Humpback whale]
            └── NARW [North Atlantic right whale]

and the taxonomy used for labelling NARW vocalisations,

.. code-block:: python
    
    >>> tax.sound_types("NARW").show(append_name=True)
    Unknown
    ├── GS [Gun shot]
    └── TC [Tonal call]
        └── Upcall

Search the database for NARW upcalls,

.. code-block:: python
    
    >>> indices = kdb.filter_annotation(conn, select=("NARW","Upcall"))
    >>> print(indices)
    [1, 4, 12]

Retrieve the annotations in `Ketos <https://docs.meridian.cs.dal.ca/ketos/introduction.html>`_-friendly format,

    >>> annot_tbl, label_dict = kdb.get_annotations(conn, indices, ketos=True)
    >>> print(label_dict)
    {0: 'NARW;Upcall'}
    >>> print(annot_tbl)
                               filename  dir_path     start  duration  freq_min  freq_max  label comments 
    0   audio_20130623T080000.116Z.flac  20130623  1127.340       3.0         0       500      0     None 
    1   audio_20130623T080000.116Z.flac  20130623  1152.026       3.0         0       500      0     None
    2   audio_20130623T080000.116Z.flac  20130623  1195.278       3.0         0       500      0     None

Finally, close the connection to the database,

.. code-block:: python
    
    >>> conn.close()


Cautionary Note
===============

Korus is still in its infancy and under active development. We aim to have a first, stable 
release out by the end of 2024, but until then users should be prepared for substantial and 
non-backward compatible changes to the code base. If you have any feedback for us, we would 
love to hear it. Please create an issue on the 
`Korus GitHub repository <https://github.com/oliskir/korus>`_.


Annotation Taxonomies
=====================

Korus gives users the freedom to create their own annotation taxonomies, 
with two structural constraints:

 - The taxonomy must exhibit a hierarchical, tree-like structure in which more specific 
   categories derive from more general ones. (For example, a killer whale is a particular 
   type of toothed whale, which in turn is a type of whale, which is a type of marine 
   mammal, etc.)

 - The taxonomy must address both sound *sources* and sound *types*, in the following sense: 
   When annotating acoustic data, it is customary to use not one, but two 'tags' to label 
   sounds: one tag to specify what made the sound, i.e, its *source* (e.g. a killer whale) 
   and another tag to specify what the sound sounds like, i.e., the *type* of sound 
   (e.g. a tonal call). Korus does not enforce a 'universal' taxonomy of sound 
   types shared by all sound sources. Instead, every sound sources is allowed to have its 
   own taxonomy of sound types.

Enforcing the use of annotation taxonomies is useful for two reasons: First, they provide 
a standard vocabulary for labelling sounds, which ensures that labels are consistent 
across annotation efforts (e.g. killer whales are consistently tagged as 'KW' rather 
than a mixture of 'killer whale', 'orca', 'KW', etc.). Second, their hierarchical structure 
provides a recipe for combining sets of annotations that employ different levels of 
specificity (e.g. 'killer whale' and 'toothed whale').

Korus allows for taxonomies to evolve over time. That is, users can make changes to the 
taxonomy (e.g. add a new sound source, or merge two sound types into a single category) and 
save the modified taxonomy to the database alongside the existing ones.


Korus vs. Tethys
================

Coming soon ...


Relation to Ketos
=================

`Ketos <https://docs.meridian.cs.dal.ca/ketos/introduction.html>`_ 
is a Python package for developing and training deep learning models 
to solve detection and classification tasks in underwater acoustics.

While Ketos includes basic utilities for managing acoustic 
annotations and metadata, we have found these utilities to be 
insufficient for managing larger projects that combine datasets 
from multiple sources and employ richer annotation taxonomies - hence 
the need for Korus.

While Korus does integrate nicely with Ketos - for example, Korus 
has functions for ingesting and exporting annotation tables in the 
standard Ketos format - it can be used entirely independently of Ketos.


License
=======

Korus is licensed under the `GNU GPLv3 license <https://www.gnu.org/licenses/>`_ 
and hence freely available for anyone to use and modify.


Acknowledgements
================

Korus was developed to meet the data management needs of the 
`HALLO (Humans and Algorithms Listening and Looking for Orcas) <https://orca.research.sfu.ca/>`_ 
project, and the design of Korus has been informed by numerous conversations 
with members of the HALLO team.

The HALLO project has been generously supported by the Canadian Department of Fisheries 
and Oceans through Grants and Contribution Agreements and the Canada Nature Fund for Aquatic 
Species at Risk program. Furthermore, graduate students in the HALLO project have received 
support through partnerships with NSERC, MITACS, SIMRES, JASCO, SMRU Consulting and the Vancouver 
Port's ECHO Program. 


Developers
==========

Oliver S. Kirsebom
