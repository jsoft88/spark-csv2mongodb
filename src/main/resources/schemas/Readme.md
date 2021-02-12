#Parser schema vs Reader schema
Add here the full fledged schema for the data. It could be the case that
it is exactly as the `reader` schema, but for example in our case, this
is not happening. To understand how this might be different, consider the
following field `genres`:

* In `schemas/reader`:
```json
{
  "name": "genres",
  "type": "string",
  "nullable": false
}
```
* While in `schemas/parser`:
```json
{
  "name": "genres",
  "type": {
    "type": "array",
    "items": {
      "name": "genre",
      "type": "record",
      "fields": [
        {
          "name": "id",
          "type": "int",
          "nullable": false
        },
        {
          "name": "name",
          "type": "string",
          "nullable": false
        }
      ]
    }
  }
}
```
So one question could be: why does it look so different for the same
field? Well, it might be the case that the json object in the source,
is actually provided as a string, for example: `"["id":1,"name":"a"]"`.
So for parsing this from the input, we need to tell the reader that we
are expecting a string, and then unpack the stringified json, into a 
real json structure. That is when we would use the full fledged schema
under `schemas/parser`.

I hope this made it easier to understand the difference between the 
two current directories.