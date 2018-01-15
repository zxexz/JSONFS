JSONFS - A simple FUSE filesystem using fusepy.

This was just intended as a learning experience for me, but perhaps somebody will find it amusing or "useful".

Creates or loads a root blob ( a json file ) that describes and is the filesystem.

When \_\_FSDATA\_\_ is read, it dumps the current filesystem ( this output can be used to load the filesystem later ).

Binary data is base64 encoded into the json, text is kept as is.
