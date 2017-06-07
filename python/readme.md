### OBJECTIVE:

  - Library to manage execution of the EAPG grouper software so it can be imported as a standalone product component

### DEVELOPER NOTES:

  - It is encouraged to have all modules, subpackages, etc. import cleanly without side effects
  - Current stitching solution `PYTHONPATH` environmental variable so library components are accessible with `import eapg...`. It is important that root package starts from `python` subfolder
  - Testing code will reside in the adjacent `tests` folder and should be written to work with the `pytest` framework.
