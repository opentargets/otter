Core
====

The ``otter.core`` module contains the main class for Otter, the `Runner`.

`Runner` is the class that is should be called in the root of the Otter
application.

.. note:: Otter handles the logging by itself (to write the manifest), and uses
   the `loguru library <https://loguru.readthedocs.io/en/stable/>`_ library for
   it. Ideally, Otter apps will stick to loguru. If the logging functionality is
   not enough, interfaces could be provided to extend the current setup.


core module
-----------

.. automodule:: otter.core
   :members:
   :undoc-members:
   :show-inheritance:
