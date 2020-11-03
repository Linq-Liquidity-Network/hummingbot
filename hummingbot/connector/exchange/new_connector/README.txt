1. Create a new folder in hummingbot/connector/exchange for your connector:
    cd ./hummingbot/connector/exchange
    mkdir your_connector_name
  Note the use of lowercase and underscore for spaces. this is necessary

2. From hummingbot/connector/exchange, copy the contents of new_connector:

    cp ./new_connector/* your_connector_name

3. Rename the files in the folder so that the preface new_connector is replaced by your 
   connector with the same format as described in step 1.

4. Perform the following series of replacements using search and replace:
   i) 