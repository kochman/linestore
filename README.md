# linestore

## What

linestore is an event database. In comparison to traditional databases that support Create, Read, Update, and Delete actions, linestore supports only Create and Read. Traditional Updates are destructive and replace existing data, and linestore is designed to store records of events.

## Example

If a user changes the title of a document from "Awesome Doc" to "Cool Doc," a traditional RDBMS will irretrievably delete "Awesome Doc," and there is no record that the title of the document was ever anything other than "Cool Doc."

With linestore, the change is appended to the datastore, allowing the application to retrieve a history of the document's previous titles.

## Why

Storing events allows the application to jump back to the state of any ID at any point in time. This is ideal for applications that produce many changes or require the ability to view the state of an entity at any previous point in time.
