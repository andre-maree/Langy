# Langy

Langy is a Durable Function app that stores language items in Azure Table Storage, defines usage groups, and compiles the usage definitions into blobs that is ready to be used by a UI application.

- Save languages
- Save language items for each language
- Define usage groups and assign language items to these groups
- Compile the usage groups into blobs containing the placeholder keys and corresponding language item values
- The blobs are then availble to download via the Langy API