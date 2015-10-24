import os.path, sys
libdir = os.path.join(os.path.dirname(__file__), '../')
libdir = os.path.normpath(libdir)

if libdir not in sys.path:
    sys.path.insert(0, libdir)
