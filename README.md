# Langy

Langy is a Durable Function app that stores language items in Azure Table Storage, defines usage groups, and compiles the usage definitions into blobs that is ready to be used by a UI application.

- Save languages
- Save language items for each language
- Define usage groups and assign language items to these groups
- Compile the usage groups into blobs containing the placeholder keys and corresponding language item values
- The blobs are then availble to download via the Langy API

Test locally using my shared PostMan workspace: https://www.postman.com/orbital-module-candidate-40190727/workspace/public/collection/27341060-51d7e023-1b01-4520-a8c3-b67e15ba73db?action=share&creator=27341060