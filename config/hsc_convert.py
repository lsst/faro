# Default HSC conversion config assumes there are brightObjectMask
# datasets landing in an HSC/masks collection, and then sets the
# option below to include HSC/masks in the HSC/defaults umbrella
# collection.  The repo we're converting here has no brightObjectMask
# datasets, so that collection won't exist.
config.extraUmbrellaChildren = []
